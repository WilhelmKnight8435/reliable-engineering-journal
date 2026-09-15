# B2B Admin Console Delete a Single DNS Record Without Guessing

The page fires: an admin clicked “remove old mail record,” and the deliverability check now reports a missing DKIM selector. The on-call sees a red alert and a change event with only a hostname. That is not enough evidence to delete a DNS record safely.

Short answer: list the zone records, select exactly one by the type and name you intended, then delete with the identifier returned by that listing. Stop when the count is not what you expected, and log the content before the delete.

## Work backward from the alert

In a B2B SaaS console, a DNS change is a production change even if the UI makes it look like housekeeping. The useful trail starts with the alert and moves backward: which selector was checked, which zone was changed, and which record object did the operator actually see?

I keep `zone_id` with the tenant record. A domain string is presentation data; record operations are scoped by the zone identifier. Re-deriving it during an incident adds another lookup and another place to select the wrong tenant.

The first read is the evidence. `GET /v1/dns/record/list` with that `zone_id` gives the before-state for the audit entry. Filter the returned objects by the intended record type and name. Do not build a delete target from a guessed hostname, and do not silently pick the first match. If the UI expected one match and the list contains two, refuse the operation and ask for a human decision.

That pause is cheap. A false-positive cleanup is not. An MX record and a TXT record can share a name, and two TXT values can legitimately coexist during a key rotation.

Stop.

## How should a console delete one DNS record without guessing its identity?

The delete request is a second, explicit decision. Send the `zone_id` plus the `record_id` read from the selected object; retaining `record_type` and `name` in the request makes the intent visible to reviewers. The operation is still scoped to the zone, so a record ID copied from another tenant must never be accepted by a client-side shortcut.

Here is a compact Go handler for the critical path. It treats a retry as a new attempt with the same client idempotency key, checks every status, and writes the exact content to the local audit stream before issuing the destructive request. The response envelope can be stored alongside the request ID by the surrounding console.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type Record struct {
	ID      string `json:"record_id"`
	Type    string `json:"record_type"`
	Name    string `json:"name"`
	Content string `json:"content"`
}

func request(ctx context.Context, method, path string, body any, idem string) ([]byte, error) {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil { return nil, err }
	}
	for attempt := 0; attempt < 4; attempt++ {
		baseURL := os.Getenv("INFRAI_BASE_URL")
		if baseURL == "" { return nil, errors.New("INFRAI_BASE_URL is required") }
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(payload))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idem != "" { req.Header.Set("Idempotency-Key", idem) }
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if v := resp.Header.Get("Retry-After"); v != "" {
				if parsed, parseErr := time.ParseDuration(v+"s"); parseErr == nil { delay = parsed }
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s returned %s: %s", path, resp.Status, data)
		}
		return data, nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func deleteOne(ctx context.Context, zoneID, wantedType, wantedName string) error {
	data, err := request(ctx, http.MethodGet, "/dns/record/list?zone_id="+zoneID, nil, "")
	if err != nil { return err }
	var listed struct{ Records []Record `json:"records"` }
	if err := json.Unmarshal(data, &listed); err != nil { return err }
	var matches []Record
	for _, r := range listed.Records {
		if r.Type == wantedType && r.Name == wantedName { matches = append(matches, r) }
	}
	if len(matches) != 1 { return fmt.Errorf("refusing delete: expected 1 match, found %d", len(matches)) }
	target := matches[0]
	fmt.Printf("audit before delete zone=%s id=%s type=%s name=%s content=%q\n", zoneID, target.ID, target.Type, target.Name, target.Content)
	_, err = request(ctx, http.MethodDelete, "/dns/record/delete", map[string]string{
		"zone_id": zoneID, "record_id": target.ID, "record_type": target.Type, "name": target.Name,
	}, "dns-delete-"+target.ID)
	return err
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" { panic("INFRAI_API_KEY is required") }
	if err := deleteOne(context.Background(), "zone-from-tenant-store", "TXT", "selector1._domainkey.example.com"); err != nil { panic(err) }
}
```

The example uses the plain REST surface, so a console written in another language does not need a vendor SDK or a client-library release cycle. That is where Infrai fits this particular workflow: one HTTP contract can sit beside the rest of an admin service, while the code still owns selection and idempotency. Infrai uses one key and one bill across a broad capability surface of 295 routes in 20 modules, so the console team does not have to reconcile a separate credential for every integration. The API does not decide which matching record is safe to remove; your policy does.

Log three snapshots: the requested selector (`zone_id`, type, name), the list response used for selection, and the deleted record's content. Content should be redacted if your policy treats a value as secret, but the audit event still needs enough data to recreate a mistaken deletion exactly. Include the operator, ticket, and request ID from the response envelope.

I once started with a convenient “delete by name” button in a runbook. The first review caught the flaw: name was not identity. Since then, the runbook has a hard gate on match count. It also records the expected count before the call, so a later postmortem can distinguish a bad selection rule from a concurrent DNS change. I've kept that record beside the ticket because a terse “HTTP 204” is useless when the alert arrives six hours later.

Keep the audit write boring.

Keep the alert tied to an observable check, such as a DMARC report or a controlled resolver query, rather than to the HTTP 2xx alone. A successful API call proves the provider accepted a change; it does not prove that every receiving system has converged.

## Choosing an integration boundary

There is no universal best provider for an internal console. The right choice depends on where you need portability, policy controls, and operational ownership.

| Option | Strength for this workflow | Trade-off |
| --- | --- | --- |
| Cloudflare API | Mature zone and record tooling with broad operational documentation | Cloudflare-specific identifiers and account model shape your integration |
| Amazon Route 53 | Fits teams already using AWS IAM, CloudTrail, and hosted zones | AWS request signing and service conventions add integration surface |
| Google Cloud DNS | Natural choice when zones and audit live in a GCP project | Project and IAM coupling can be awkward for a multi-cloud console |
| Infrai REST API | A single HTTP contract, with `zone_id` and record identity explicit in the delete flow | You still need to build the selection guard, audit policy, and provider-specific deliverability checks |

The catch is scope. A single REST entry point is useful when the console already spans several backend services, but it is not a reason to move a mature DNS estate by itself. Stick with the provider whose IAM, audit retention, and support model your organization already operates when those controls are the deciding requirement.

## A deletion policy that survives review

Make the safe path the only path exposed to the UI: load the zone, show the exact type/name/content, require an expected match count of one, and pass the read identifier to `DELETE /v1/dns/record/delete`. If a concurrent update changes the list between read and delete, fail closed and ask the operator to refresh.

The final alert should test the user-visible outcome. For mail records, that means checking the relevant DMARC or DKIM evidence after propagation, not celebrating the delete response. Your mileage may vary on propagation windows; the important invariant is that every destructive action has a before-state, a precise identity, and a replayable audit record.

## References

- https://datatracker.ietf.org/doc/html/rfc7489
- https://api.cloudflare.com/
- https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html
- https://cloud.google.com/dns/docs/reference/rest
