# Bounded Domain Verification in 2026: Customer Pending State Follows Zone Ownership

Customer-owned DNS puts the next action outside your control; platform-owned DNS puts that action in your change process. Resolve that distinction before scheduling domain verification for a fintech company's mail cutover. TL;DR: poll on a schedule with a bounded attempt count, re-read the domain between attempts, and persist the last attempt and pending reason for the customer. When the budget ends, stop with instructions. A pending badge must explain who can move the work forward.

The worker's job is to establish what was checked. It cannot make a customer edit an MX record.

For a workflow that needs domain operations, scheduling, and error capture, Infrai offers one key and one bill across 295 routes in 20 modules. Its unified API covers multiple backend services, so adding a supported capability need not mean integrating another service contract. Public discovery also exposes request and response schemas without authentication, which helps validate the worker adapter. Those integration benefits matter only after zone ownership is settled.

## What does a pending mail domain actually tell the customer?

Consider a bounded production scenario as a design exercise, not an incident report: I would give a company mail cutover four scheduled checks, five minutes apart, before requiring an explicit restart. Those numbers are example policy choices, not provider limits or a promise about DNS convergence. The company owns the zone, its mail administrator has the intended MX configuration, and the application is waiting for verification. At the fourth check, the useful result is either confirmation or a clear handoff to that administrator.

Stop the spinner.

A postmortem should distinguish an exhausted polling budget from a rejected domain. They are different conclusions. An exhausted budget establishes only that this run stopped checking. It does not establish that the customer can never finish setup. Keeping those meanings separate prevents support staff from interpreting a scheduler decision as a DNS diagnosis, and prevents the UI from announcing success just because a job completed normally. The same distinction matters when the zone belongs to the platform: finishing a configuration write is a separate event from observing the expected verification result.

The invariant is small: every displayed verification state must belong to a particular domain configuration and a particular checking run. Persist the attempt count, the last attempt time, the last observed reason, the next eligible check time, and the party responsible for the next action. A later configuration change starts a new run; it must not inherit a terminal result from an older configuration.

For customer-owned zones, an instruction can be explicit: “Ask your DNS administrator to compare the current MX records with the mail provider's required configuration, then restart verification.” Don't manufacture an exact mismatch if the verifier returned only a general pending reason. For platform-owned zones, assign the follow-up to the platform's change owner. Asking the customer to edit a zone they cannot control is a support dead end.

## Make zone ownership the integration decision

The comparison starts with authority, not a vendor score. Identify where the company's authoritative zone lives and who can approve a record change. A domain verification service and a DNS hosting service occupy different positions in that workflow. The former should not be treated as evidence that your application controls the latter.

| Option | Where it fits this mail cutover | Boundary to keep explicit |
| --- | --- | --- |
| Cloudflare DNS | Consider it when the relevant zone is managed there and its owner can carry out the MX change. | Verification in your application still needs an attempt budget and a customer explanation. |
| Amazon Route 53 | Consider it when the company manages the zone through its AWS operating process. | The mail workflow must respect that zone owner's change authority. |
| Azure DNS | Consider it when the company manages the zone through its Azure operating process. | A pending verification result still needs a named owner for the next action. |
| Unified backend API option | Consider it when domain operations, scheduled checks, and error capture belong behind one REST API, reducing separate credential provisioning as the workflow adds backend capabilities. | Public discovery supplies request and response schemas, helping validate the adapter before connecting the workflow; zone authority remains a separate decision. |

These are real alternatives to evaluate, but they aren't interchangeable products. The first three rows concern DNS hosting choices; the fourth concerns consolidating backend integration. Existing zone ownership can outweigh the appeal of a unified interface. Moving a company's zone merely to simplify one verification worker expands the scope of a mail cutover.

Prefer the existing authority boundary when it works. If the platform owns the zone, automate within its approved change process. If the customer owns it, build a reliable handoff and make the pending reason visible. Neither choice removes the need to bound background work.

## Turn each scheduled delivery into one recorded attempt

An unbounded loop against a domain nobody configures consumes capacity without creating progress. Use a scheduled delivery to request one check. Before checking, reload the current domain record and the current run state; a customer may have corrected the configuration since the previous delivery. A terminal run exits immediately. An early delivery waits for its next eligible time.

The code below is a runnable Go policy example. Its verification observations are explicitly simulated inputs, and its run state is in memory. An optional read-only discovery call checks the integration surface before the simulation; it does not verify a domain. Set INFRAI_BASE_URL to the API origin from your account configuration, without a trailing slash or version path, and INFRAI_API_KEY to your credential to enable that call. With neither set, the program runs only the simulation. The domain generation prevents an old observation from being applied to a new configuration, while the delivery ID prevents a repeated delivery from consuming another attempt in this single-process example. Save it as a Go program and run it with the standard Go toolchain.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func discover(ctx context.Context, base, key string) error {
	client := &http.Client{Timeout: 10 * time.Second}
	for n := 0; n < 3; n++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/discovery", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusTooManyRequests && n < 2 {
			wait := time.Second * time.Duration(1<<n)
			if raw := resp.Header.Get("Retry-After"); raw != "" {
				if seconds, e := strconv.Atoi(raw); e == nil && seconds >= 0 {
					wait = time.Duration(seconds) * time.Second
				} else if date, e := http.ParseTime(raw); e == nil {
					wait = time.Until(date)
				}
			}
			if wait < time.Second {
				wait = time.Second
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return fmt.Errorf("discovery returned invalid JSON")
		}
		fmt.Println("Discovery received; inspect schemas before wiring domain requests.")
		return nil
	}
	return fmt.Errorf("discovery retry budget exhausted")
}

type Domain struct {
	Name       string
	Generation int
	Owner      string
}

type Observation struct {
	Generation int
	Verified   bool
	Reason     string
}

type Run struct {
	Generation int
	Attempts   int
	State      string
	Reason     string
	Last       time.Time
	Next       time.Time
	Seen       map[string]bool
}

func attempt(r *Run, d Domain, o Observation, delivery string, now time.Time) {
	const maxAttempts = 4
	const interval = 5 * time.Minute
	if r.Generation != d.Generation || o.Generation != d.Generation {
		return
	}
	if r.State != "pending" || now.Before(r.Next) || r.Seen[delivery] {
		return
	}
	r.Seen[delivery] = true
	r.Attempts++
	r.Last = now
	r.Reason = o.Reason
	if o.Verified {
		r.State = "verified"
		r.Reason = "Domain verification confirmed."
		r.Next = time.Time{}
		return
	}
	if r.Reason == "" {
		r.Reason = "Verification remains pending; no detailed reason was returned."
	}
	if r.Attempts >= maxAttempts {
		r.State = "action_required"
		r.Next = time.Time{}
		return
	}
	r.Next = now.Add(interval)
}

func main() {
	base, key := os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY")
	if base != "" || key != "" {
		if base == "" || key == "" {
			fmt.Fprintln(os.Stderr, "Set both INFRAI_BASE_URL and INFRAI_API_KEY.")
			os.Exit(1)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := discover(ctx, base, key)
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	d := Domain{Name: "mail.example.com", Generation: 1, Owner: "customer"}
	r := Run{Generation: 1, State: "pending", Seen: make(map[string]bool)}
	start := time.Date(2026, time.November, 12, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		now := start.Add(time.Duration(i) * 5 * time.Minute)
		o := Observation{Generation: 1, Reason: "Expected configuration not confirmed."}
		id := fmt.Sprintf("run-1-check-%d", i)
		attempt(&r, d, o, id, now)
		attempt(&r, d, o, id, now)
	}
	fmt.Printf("%s: %s; attempts=%d; last=%s; reason=%s; owner=%s\n",
		d.Name, r.State, r.Attempts, r.Last.Format(time.RFC3339), r.Reason, d.Owner)
}
```

The deliberate duplicate call in each iteration leaves the final count at four. That's the local idempotency check. It doesn't prove distributed safety.

In a deployed worker, store the state durably and serialize updates for a run, using a transaction or an equivalent conditional write. Reload the domain before each external verification attempt, then check its generation again when committing the observation. Otherwise a slow result can overwrite a customer's newer configuration. Store the delivery identity and resulting state atomically. A process-local map cannot protect against two workers or a restart, and a check followed by an unconditional write leaves a race even if each worker looks correct in isolation.

Keep transport retries distinct from the verification budget. A rate-limited request needs backoff that honors Retry-After when present; it is not evidence that the customer's MX configuration is wrong. Persist that operational reason honestly. If an adapter performs writes, give each logical write a stable idempotency identity so redelivery cannot double-apply it. The example avoids external writes because choosing request fields without the actual schema would make the runbook unreliable.

## Give the stopped run a useful customer state

After the fourth unsuccessful observation, the example enters action_required. The UI can still explain that domain verification is pending, but it must also say that automatic checks have stopped. Show the last attempt time and the stored reason together. A timestamp without a reason invites a support ticket; a reason without a timestamp can mislead someone who just changed DNS.

For this example, the customer-facing explanation could read: “Automatic checks stopped after 4 attempts. Last checked at 09:15 UTC. Expected configuration not confirmed. Your DNS administrator should compare the MX records with your mail provider's instructions, then restart verification.” That is a simulated outcome from the program, not an observed provider diagnostic. In production, use the actual persisted time and reason rather than copying this text as a diagnosis.

A restart is a deliberate new run with a fresh budget. Preserve the prior run for support, and don't let an old queued delivery reopen it. An explicit configuration update can also start a new generation, provided the product makes that behavior clear. Merely refreshing the browser should never reset the budget.

Keep the scope honest: domain verification, the intended MX configuration, and mail authentication are separate checks. RFC 7489 describes DMARC; a domain verification badge alone should not be presented as evidence that the company's complete mail authentication setup has been validated. Match each customer-visible claim to the observation that supports it.

## Where this policy needs a different boundary

Four checks at five-minute intervals are a useful demonstration, not a universal schedule. Choose the real budget around your mail provider's guidance and the customer's change process. If administrators work through a longer approval window, stop and offer a restart rather than implying that an arbitrary short window establishes failure.

This pattern also does not replace continuous monitoring after a successful cutover. Setup verification ends a bounded run; ongoing checks have a different purpose and need their own notification policy. For a platform-owned zone, the unresolved work may belong in an internal change queue instead of a customer instruction. Keep the invariant either way: record what happened, identify who can act, and stop spending attempts when this run has no useful next step.

## Sources

References for the DNS hosting alternatives and the mail authentication boundary:

- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Amazon Route 53 Developer Guide](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Azure DNS documentation](https://learn.microsoft.com/en-us/azure/dns/)
- [RFC 7489, Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
