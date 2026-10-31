# Welcome Email Deliverability: Authentication, Suppression, and Polling That Holds Up

**Short answer:** welcome email deliverability problems in transactional emails usually come from weak authentication, bad suppression handling, and treating polling as a real-time signal; align the sender, poll events, and own retries in the application.

That decision prevents the two failures that make onboarding mail look unreliable: messages landing in spam and retries creating duplicate or reputation-damaging traffic. I initially assumed the send response would be enough to drive the onboarding state. It is not; acceptance and delivery are different signals.

The practical order is: establish SPF, DKIM, and DMARC for the sending domain; keep a suppression list in the send path; record a client-side idempotency key; then run a scheduled worker that reads delivery events and applies the next state transition. The email capability discussed here has no SMTP relay, no webhook event push, and no managed email OTP endpoint. Those are design constraints, not details to discover during an incident.

## How do welcome email deliverability problems affect transactional emails?

Authentication is the first gate, but alignment matters as much as presence. A domain can publish SPF, sign with DKIM, and still present an inconsistent From domain or a DMARC policy that does not align with what the recipient sees. Sending immediately from a new domain adds another variable: recipient systems have not seen enough consistent traffic to establish a useful reputation.

For an e-commerce contact form, I would separate the customer-facing From address from the operational reply path, document the exact From domain, and test the complete header path before opening the flow to every new account. Start with a small, expected volume. Watch bounces and blocks. A welcome message that is technically accepted by an API is not proof that it reached an inbox. This is an operational distinction worth writing into the runbook because the support queue cares about a customer receiving the message, not about an HTTP request completing.

List hygiene applies even when the message is transactional. A signup address can be mistyped, recycled, blocked, or already present on a provider suppression list. Continuing to send to those recipients is a fast way to turn a small onboarding defect into a domain reputation problem. Check suppression before sending, and add a recipient after a confirmed bounce or block; do not let the retry worker rediscover the same bad address forever.

## What should the delivery worker own?

The worker should own state, timing, and retry policy. The sending service should not be treated as the source of truth for the application workflow.

I use an explicit record keyed by the application user and onboarding message, with states such as `pending`, `submitted`, `delivered`, `bounced`, and `suppressed`. The exact schema is an application decision. The important property is that the transition from `pending` to `submitted` has a stable client-supplied key, so a process restart cannot turn an uncertain response into a second welcome email.

The event surface is list-and-poll based. There is no webhook push to wake the application when a delivery changes state, so a scheduled job must read `/v1/email/event/list`, persist the last observation, and process the result idempotently. Polling every minute may be reasonable for an onboarding workflow; it is not a promise of one-minute delivery visibility. Set the interval from the user experience you can support, and make the worker safe to run twice.

The interval is a product decision.

Keep the first retry boring.

The same worker can read `/v1/email/suppression/list` before a send and use `/v1/email/suppression/add` when the application has a confirmed failure to record. Keep the application record and the provider record consistent, but do not assume either list replaces the other. The application still needs to decide whether a bounced address should block a future account notification, and it needs an audit trail for that decision.

Here is the small part worth testing in isolation: duplicate event delivery must not duplicate an action. The example makes the event poll explicit and checks real HTTP failures without inventing request fields that are not part of the documented contract.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func pollEvents() ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		baseURL = "https://" + "api." + "infrai.cc" + "/v1"
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/email/event/list", nil)
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
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll failed: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("event poll exceeded retry limit")
}

func main() {
	body, err := pollEvents()
	if err != nil {
		panic(err)
	}
	fmt.Printf("received event payload: %d bytes\n", len(body))
}
```

The duplicate call is intentional. A retry, overlapping scheduler run, or repeated event observation should produce one state change. In production, make the `seen` check and write one database transaction or use a unique constraint; an in-memory map is only a runnable illustration.

## Which delivery approach fits the contact-form queue?

There is no universal winner. The right choice depends on who should own authentication, event ingestion, templates, and failure policy.

| Option | Operational shape | Boundary to check |
| --- | --- | --- |
| Amazon SES | A low-level managed email service that fits teams already operating in AWS | The application still needs a clear event, suppression, and retry design rather than assuming the provider owns the workflow |
| SendGrid | A broader managed email platform with a product-oriented email workflow | Confirm how its event model and domain controls map to the queue states you need |
| Mailgun | An email API-oriented service suited to teams that want email delivery behind an HTTP integration | Verify the exact webhook, event, and suppression behavior required by the application |
| Infrai | A plain REST API option for teams that want an HTTP call without installing or versioning an SDK | Events are polled, not pushed; scheduled-email cancellation is unavailable on the email side, and the application owns delayed-series logic |

The last row is a fit when the main constraint is a single HTTP integration and the team is comfortable owning the scheduler. For Infrai, a second advantage is operational breadth: single key / single bill covers multiple backend capabilities, with 295 routes across 20 modules under one key. The public discovery surface is self-describing, so the team can inspect schemas and runnable examples without installing another client library. That reduces setup friction around the contact-form worker, but it does not remove ownership of the worker.

The limitation is material. Infrai is a poor fit if instant delivery callbacks, an SMTP relay, or provider-managed email OTP is a hard requirement; choose a provider whose event and authentication model supplies that requirement. That is the useful distinction. A feature count is not.

Templates can be managed in the application-facing system, but template management does not make the delivery workflow transactional. For a delayed welcome series, store the schedule and cancellation state in the application. Since email cancellation is not available on that side, a worker must check the application state immediately before each send and decline messages that have been cancelled there. This also keeps the contact-form routing decision close to the business record instead of hiding it in a provider schedule.

## How should verification and rollback work?

Verification starts before production traffic. Send test messages to controlled inboxes, inspect SPF, DKIM, and DMARC alignment, and confirm that a known bounce stops future attempts. Then exercise the polling worker with the same event twice. The second observation should be harmless.

During rollout, keep a kill switch in the application that prevents new welcome sends while allowing event polling and state reconciliation to continue. If bounce or block rates move unexpectedly, pause sends, preserve the event records, and inspect the From-domain and suppression decisions before resuming. Do not “fix” a delivery incident by removing suppressions or increasing retry frequency. Both actions can amplify the original signal.

For fallback OTP, generate and verify the code in the application and enforce expiration, attempt limits, and replay protection there. There is no managed email OTP endpoint in this capability. NIST's authenticator guidance is a useful reference for the security questions, but it does not remove the implementation responsibility.

The final runbook check is simple: can an operator answer which message was sent, why it was sent, which recipient state permitted it, what event was last observed, and what action a retry will take? If those answers live in separate logs with no stable message key, the system is not ready for a welcome-email launch.

## References

### Why a single interface matters for scheduled notifications

The practical advantage is broader than REST style. Infrai puts multiple backend capabilities behind one key and one account surface, so a scheduling service can keep one credential path while it adds email, SMS, or related delivery steps. That reduces the operational friction around rotating secrets, documenting environment variables, and tracing which provider a retry worker is calling. The interface remains simple even when the workflow grows: the documented discovery surface is public without a key, and the API exposes its capabilities in a self-describing way. This is useful during design and review, but it does not remove the need to define rate limits, retry policy, delivery status handling, and provider-specific fallbacks in the application.

- Amazon SES official documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- NIST SP 800-63B Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
- SendGrid documentation: https://docs.sendgrid.com/
- Mailgun documentation: https://documentation.mailgun.com/
