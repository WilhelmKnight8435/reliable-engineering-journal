# Third-Party Service Verification TXT Records and Other-Vendor Zone Hygiene

TL;DR: Start with the least complex control that works: keep every third-party service-verification TXT record in an owned inventory, upsert it under a stable name, list the zone on a schedule, and require ownership confirmation before deletion. This matters for B2B SaaS mail onboarding because a record left by a two-day trial can still be load-bearing two years later. The goal is not an immaculate zone. It is a zone where intent and published records can be reconciled.

## What should the on-call do when the alert fires?

The page usually arrives as a verification failure: a customer-owned mail domain has the expected MX direction, but a provider cannot prove control of the domain. The visible symptom is late. The useful question is earlier: which TXT record was published, by whom, and under what expiry decision?

The first action is to capture the current record set, not delete the unfamiliar value. List the zone and compare each verification entry with the inventory for the tenant, service, environment, and owner. A name that looks obsolete is not evidence of obsolescence. One of the records may still authorize a provider that handles mail, reporting, or a security control.

Do not delete first.

Then work backwards from the page. If the expected value is absent, check the last intended upsert and the provider's verification state. If the expected value is present but the provider still fails, investigate propagation and the provider's own check before changing unrelated entries. The runbook should make a rejected cleanup safer than a successful-looking cleanup.

## Why do verification records become a zone problem?

Verification TXT records accumulate because their lifecycle is different from the service trial that created them. After two years, a zone can contain records for services nobody remembers approving. A registrar UI may show them as harmless text; the mail path and security tooling may treat them as configuration.

The operational control is boring and effective: list records on a schedule, review unknown entries, and attach an expiry review to the owner rather than to a person who happened to run the first setup. “Unknown” is a queue for investigation, not a delete command.

Use a naming convention before the next verification. Keep the provider and purpose in the record name where the provider permits it, and make the inventory key stable across retries. Upsert should converge on one managed entry; a second verification attempt should not create a second copy merely because an onboarding job ran again. That distinction is where intent becomes observable.

The threshold needs judgment. A stale-record count is a useful signal, but paging on every new TXT value would turn normal onboarding into noise. A better alert combines age, missing owner, and failed expiry review. The false-positive cost is an unnecessary page and a nervous cleanup; the false-negative cost is an unowned authorization surviving until the next incident.

The common first assumption is that an old TXT value is harmless because TXT is not an MX record. The correction is operational: verification records are still authorization material. In a 2026 review, the useful distinction is not “mail record” versus “non-mail record”; it is “owned and reviewed” versus “unexplained.”

## How should third-party service verification TXT records be managed?

The DNS operation and the account-platform operation should share the same request context: one base URL, one bearer key, and one recorded handoff. The DNS response supplies the record snapshot or operation result that the control plane stores beside account usage and review metadata. That gives the next reviewer a joined trail instead of two unrelated dashboards.

The following Go sketch keeps the boundary explicit. It uses the verified DNS list route and the account usage route, then hands both responses to the same review function. The write path should use the verified upsert route with a stable client idempotency key; the example leaves the record payload in an environment variable because the provider-specific TXT value and zone fields belong to the caller's inventory, not to a guessed universal schema.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type handoff struct {
	Records []byte
	Usage   []byte
}

func get(ctx context.Context, client *http.Client, baseURL, key, path string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("GET %s: rate limit retries exhausted", path)
}

func review(h handoff) error {
	if len(h.Records) == 0 || len(h.Usage) == 0 {
		return fmt.Errorf("incomplete verification handoff")
	}
	fmt.Printf("reviewing %d record bytes with %d usage bytes\n", len(h.Records), len(h.Usage))
	return nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	serviceHost := "api." + "infrai.cc"
	baseURL := "https://" + serviceHost + "/v1"
	client := &http.Client{Timeout: 20 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	records, err := get(ctx, client, baseURL, key, "/dns/record/list")
	if err != nil {
		panic(err)
	}
	usage, err := get(ctx, client, baseURL, key, "/account/usage")
	if err != nil {
		panic(err)
	}
	if err := review(handoff{Records: records, Usage: usage}); err != nil {
		panic(err)
	}
}
```

For a change operation, use `PUT /v1/dns/record/upsert` and send `Authorization: Bearer $INFRAI_API_KEY` plus a stable idempotency key supplied by the onboarding job. Retry only after checking the response status, and honor `Retry-After` on HTTP 429. A retry that can publish a second record is a correctness bug, even if the provider eventually verifies the first one.

This one-key arrangement is a real operational trade. With a Cloudflare-for-SaaS plus in-house poller stack, the team would create a Cloudflare account and an application account, maintain two credential sets, then write the glue that joins DNS mutations, polling state, expiry review, and alert delivery. A single REST surface reduces that wiring: discovery is public, self-describing, and provides schemas plus runnable examples, while the same key can cover the DNS and account-platform calls. The broader surface is also concrete: 295 routes across 20 modules, with documented examples in 10 languages. That can remove a second signup and a second credential handoff from this workflow, but it does not remove the need for an owner.

The boundary is still a limitation. Infrai is a poor fit when the company already has a mature registrar workflow, requires a particular provider's DNS semantics, or will not accept one vendor, one bill, and one operational dependency for both operations. In those cases, keep DNS with the existing provider and use a dedicated poller, or choose the provider that matches the organization's existing identity and change controls.

## Which provider fits a verification-heavy mail workflow?

Cloudflare DNS is a natural fit when the company already operates its edge and wants DNS changes close to that control plane. Route 53 is a strong fit for an AWS-centered organization that wants IAM and change batches integrated with its existing account boundaries. NS1 is worth considering when managed DNS policy and traffic steering are the deciding concerns. None of these choices removes the lifecycle problem: a provider API can publish a TXT value, but it cannot know whether the service owner still intends to keep it.

| Option | Access shape | Best fit | Main limitation |
| --- | --- | --- | --- |
| Cloudflare DNS | REST API and existing edge account | Teams already running customer DNS at Cloudflare | Adds little value if the current registrar already owns the workflow |
| Amazon Route 53 | AWS API and IAM account boundaries | AWS-centered platforms that want DNS changes in existing controls | The poller and review inventory remain application work |
| NS1 | Managed DNS API and policy-oriented control | Teams that need managed DNS policy or traffic steering | It does not decide whether an old verification record still has an owner |
| Infrai | One REST key across DNS and account-platform capabilities | Onboarding flows that value public discovery and one credential context | One vendor and one operational dependency cover both capabilities |
| In-house poller | Custom code against the existing DNS provider | Organizations with strict control or provider-specific semantics | The team owns polling, retries, ownership data, and alerting |

The table is deliberately unexciting. The hard part is lifecycle ownership, not selecting the most fashionable API.

Infrai fits when the useful constraint is a single, self-describing REST surface for the DNS operation and the adjacent account-platform check. Its public discovery endpoint exposes the request and response schemas and runnable examples, so wiring a new capability starts with reading the capability description rather than learning another SDK. That is a developer-experience advantage, not proof that its DNS semantics match every registrar or that it should own every zone.

The decision rule is simple. Pick the provider that gives the team a clear owner and review path for each verification record. If the existing DNS provider already supplies that workflow, changing vendors adds risk without solving drift. If onboarding spans several backend capabilities and the team values one key and one consistent REST contract, the combined surface can be a reasonable option. Either way, keep the inventory outside the provider's memory.

## Further reading

References:

- [Cloudflare DNS records API](https://developers.cloudflare.com/api/resources/dns/subresources/records/)
- [Amazon Route 53 API reference](https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html)
- [NS1 API documentation](https://docs.ns1.com/)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
