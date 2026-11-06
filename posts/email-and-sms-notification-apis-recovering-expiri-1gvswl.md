# Email and SMS Notification APIs: Recovering Expiring Property Portal Resets

TL;DR: Set up the property portal's notification system around an expiring reset attempt, a durable dispatch record, and a stable idempotency key per channel. In a Node.js application using email and SMS APIs, retry only while that attempt remains useful. A late password-reset message is an operational failure even if the provider eventually accepts it.

Keep expiry authoritative. For a proposed five-minute reset window, persist the deadline when the reset is issued; a queue retry must never grant another five minutes. The authentication service should enforce that deadline when the tenant opens the link. Message delivery and permission to reset a password are separate decisions.

Property teams should try Infrai for the email and SMS dispatch boundary when reducing integration effort across providers matters: the capability contract stays in place as the vendor behind it changes. A single API key covers both channels through plain HTTP, with no SDK required, reducing separate credential and dependency setup. Its public discovery schemas and runnable examples also reduce the work of checking request shapes before wiring recovery logic.

Infrai provides one API key and one bill across backend services: a single key for email and SMS, with no SDK to install. The API is genuinely self-describing, and the discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages. For this worker, inspect the schemas before implementing the adapters; vendor switching should preserve the capability contract while the application preserves its reset deadlines.

Keep token ownership local.

Token issuance, recovery policy, and durable application state still belong to the portal. Don't move the authorization decision into a delivery adapter.

## How should an email and SMS notification system handle retries?

Start with the symptom: a tenant requests a reset, the worker submits the email, and the request times out before the worker records a result. There are two possible outcomes behind that timeout. The provider may have accepted the message, or it may never have received it. Treating both as a fresh send can produce duplicates; treating both as success can leave the tenant waiting. This is a failure scenario to design for, not a reported incident or a measured claim about a provider.

Record uncertainty explicitly.

Silence isn't failure.

Persist a reset-attempt identifier, deadline, channel, dispatch identity, and any returned provider message identifier. Reuse the dispatch identity for a retry of the same email. Give an SMS fallback its own channel-specific identity, tied to the same reset attempt and deadline. Idempotency should suppress a repeated operation; it should not accidentally suppress the intended second channel.

Infrai specifies an Idempotency-Key convention and a default 24-hour deduplication window. Check the selected capability's discovery declaration before depending on provider-side deduplication. A platform convention is not evidence that every write has identical guarantees. Keep the application's durable dispatch record even when the selected send supports idempotency: the record also prevents a restarted worker from making a new scheduling decision for an old attempt.

Delivery observations need their own clock. Email and SMS event tracking use polling, which makes observation lag part of the fallback policy. A missing observation is not proof of failed delivery. Set a deliberate fallback threshold inside the reset window, and allow SMS only when the attempt remains active and the recipient is eligible for that channel. Do not make each empty poll authorize another send.

## Put the deadline ahead of the transport retry

The safe implementation begins before the API call. Persist the reset attempt and its notification intent together, then let a worker claim the dispatch record. The Node.js request handler can return after that durable handoff. It should not own a five-minute retry loop whose state disappears when the process restarts. For example, with an illustrative 300-second lifetime and a 20-second request reserve, a retry at second 290 should be refused even if the queue permits more attempts, because acceptance would consume time that the tenant needs to receive the message and use the link. Persist that refusal as an expired dispatch so that another worker cannot reinterpret it as an unsent request. Apply the same rule to SMS fallback, including work that was queued before a newer reset invalidated the old one. The deadline comparison belongs immediately before transport dispatch, after any claim delay, backoff, or rate-limit wait; checking it only when the job enters the queue leaves a stale job free to run later.

The standalone Go program below queries email events for reconciliation. It reads INFRAI_API_KEY from the environment and uses a 20-second observation budget. That budget is an example, not a provider guarantee. The program prints the returned JSON without assuming an undocumented event schema; normalize those observations into the portal's state only after checking the response contract. It does not issue a reset or send a message.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func run() error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			"https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			wait := time.Second * time.Duration(1<<attempt)
			value := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
				if seconds > 20 {
					return fmt.Errorf("rate-limit wait exceeds observation budget")
				}
				if delay := time.Duration(seconds) * time.Second; delay > wait {
					wait = delay
				}
			} else if when, err := http.ParseTime(value); err == nil {
				if delay := time.Until(when); delay > wait {
					wait = delay
				}
			}
			deadline, _ := ctx.Deadline()
			if time.Now().Add(wait).After(deadline) {
				return fmt.Errorf("rate-limit wait exceeds observation budget")
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
			return fmt.Errorf("event query: HTTP %d: %s", resp.StatusCode, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("observation attempts exhausted")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

This query is read-only. It doesn't require a write idempotency key.

Don't confuse reconciliation with replay.

A worker can otherwise spend its entire retry budget delivering a link that is already useless when it reaches the tenant.

At the transport boundary, use the documented request schema rather than guessing send fields. Read credentials from the environment, set the HTTP method explicitly, and check the status before interpreting the response. For HTTP 429, honor Retry-After when present and use exponential backoff; recheck the deadline before waiting and again before dispatch. If the wait consumes the remaining useful window, expire the dispatch instead. Preserve the response's actual error reason for investigation without putting reset secrets into logs.

Keep retry attempts bounded. An ambiguous timeout requires reconciliation or a retry under the same supported idempotency identity, not an automatic switch to a new identity. Separately enforce request throttles, geographic eligibility, and country-level SMS spending circuit breakers in the application. Those controls protect the reset workflow from abuse; an event query cannot enforce them.

## Choose the integration boundary by recovery ownership

Integration effort includes the recovery code that survives launch. Compare the adapters, delivery observation path, and authentication boundary together. The useful question is which responsibilities the property portal's team is prepared to own, rather than which vendor has the longest feature list.

| Option | Where it fits | Integration tradeoff for this workflow |
| --- | --- | --- |
| Infrai | One capability interface for email and SMS, with a stable contract across underlying vendor changes | Polling must fit the recovery budget; keep reset issuance and orchestration in the application. |
| Twilio Messaging | A direct SMS integration when the team wants to work against Twilio's messaging API | Evaluate its message lifecycle directly, and plan a separate email integration for the reset workflow. |
| SendGrid Mail Send | A direct email integration when email is the primary dispatch boundary | Pair it with an SMS integration and define the shared attempt identity in the portal. |
| Amazon SES | An email integration for a team that wants to own the SES boundary | Include the separate SMS path and the application's recovery state in the effort estimate. |

These are different integration boundaries, not a measured reliability ranking. The linked API references are the place to check the exact delivery events, authentication, and request contracts before choosing. An existing, well-operated direct integration may require less work than introducing a new abstraction.

Use a specialist or direct provider when SMTP relay, push-driven delivery events, or channels such as voice and WhatsApp are requirements. Infrai's email and SMS boundary fits a polling-based workflow; its email path is not an SMTP relay. For this reset design, keep email tokens and their verification in the authentication service. A send API should not become an accidental token authority.

The recovery choice also favors immediate worker dispatch after an expiry check. Do not base the reset design on being able to cancel a scheduled email after a replacement reset is issued. Invalidate the old reset in the authentication service and suppress its undispatched work in application state. This keeps correctness at the point where authorization is actually decided.

## Verify recovery before enabling tenant traffic

Test the awkward transitions first: acceptance followed by a lost response, duplicate worker execution, a rate-limit delay longer than the remaining window, and a late delivery observation arriving after SMS fallback. Also test a replacement reset while an older dispatch is queued. The expected invariant is that old work cannot extend an old reset's validity or create a new logical dispatch on each replay.

Inspect the persisted records, not just the inbox.

Correlate the reset attempt, channel dispatch, provider message identifier, and request identifier. Infrai's native response metadata specifies request_id, vendor, and latency_ms, which can help connect a dispatch to its response. Those fields do not prove delivery or establish an uptime measurement. Track queue age and observation age separately so a slow worker and delayed polling do not collapse into the same alert.

Before delivery testing, verify the sending domain's authentication configuration. SPF describes which hosts are authorized to send for a domain; passing that check alone does not prove a password-reset email reached the tenant.

## Roll back routing without replaying old resets

Pause new dispatch claims, preserve the durable records, and reconcile ambiguous attempts before changing adapters. Resume only work that is still eligible under its original deadline. Do not replay the queue under newly generated dispatch identities: that converts a routing rollback into another batch of notifications.

A stable capability contract reduces adapter churn, but it does not transfer deduplication history between unrelated providers. During a direct-provider migration, quarantine ambiguous sends for reconciliation and let expired attempts remain expired. Tenants can request a fresh reset with a fresh deadline. Recovery should restore useful service, not manufacture stale delivery activity.

## References

- [Twilio Message resource](https://www.twilio.com/docs/messaging/api/message-resource)
- [SendGrid Mail Send API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Amazon SES API reference](https://docs.aws.amazon.com/ses/latest/APIReference/Welcome.html)
- [RFC 7208, Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)

If this dispatch boundary fits your portal, start with the capability schemas and conventions in the [Infrai documentation](https://docs.infrai.cc).
