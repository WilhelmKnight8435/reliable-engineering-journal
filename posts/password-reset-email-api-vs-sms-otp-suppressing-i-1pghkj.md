# Password Reset Email API vs SMS OTP: Suppressing Invalid Support Recipients

Use email reset links for ordinary SaaS account recovery, and suppress an invalid address before support can trigger another send. Short answer: SMS OTP belongs in a separately controlled backup or higher-risk path, not as an automatic reaction to every failed email. For a US and EU support team, the deciding integration cost is the work between a bounce signal and the next recovery request. Sending is the easy part.

An email reset link avoids telecom registration, country-dependent SMS charges, and the phone-abuse controls that a business must build for SMS. That usually makes it simpler and cheaper for normal SaaS login recovery, without making price the only consideration. A managed SMS OTP API can simplify code delivery and verification, but it cannot decide whether an invalid mailbox should be retried, or whether a phone number should be trusted for this account. Keep those decisions in the recovery service.

## Should password reset use an email API or SMS OTP after a bounce?

A permanent invalid-recipient result should change the address's eligibility for future recovery mail. A transient delivery failure should not silently become permanent suppression. Record the provider event ID, address, classification, and decision time, then deduplicate events before updating the local state. A support agent may submit a second reset while the first bounce is still waiting to be processed; the queue worker must read current suppression state immediately before it claims a send, even if the request passed a check when it entered the queue.

The claim and the challenge need a stable identifier. An at-least-once worker retry must reuse that identifier rather than issue another reset link after losing its acknowledgment. Treat suppression as an account-recovery policy decision, not just a provider-side mailing-list setting. Return the same neutral response to the requester whether the address is suppressed or unknown, so the recovery endpoint does not disclose account membership.

Check again at send time.

For an email-code design, there is an additional integration step: Infrai has no managed email OTP endpoint. A reset link can use the email send capability, while code generation, expiration, and verification would belong to your backend. Its email and SMS events are pull-based, so polling lag limits how quickly an automatic cross-channel transition could follow a bounce. Do not promise immediate fallback based on those events.

## Which integration is smaller once invalid recipients matter?

Compare the whole signal-to-suppression path. These options solve different portions of it; none replaces the recovery service's authorization and idempotency rules.

| Option | Integration surface | Where it fits | Operational boundary |
| --- | --- | --- | --- |
| Amazon SES | AWS email delivery, bounce notifications, and account-level suppression | Teams already routing AWS notification events | Map notifications into the support recovery state and own challenge deduplication |
| SendGrid | Email API, event webhook, and suppression lists | Teams that need pushed email events for timely classification | Webhook ingestion and recovery eligibility remain application decisions |
| Postmark | Email delivery and bounce webhook | Teams prioritizing an email-focused event integration | Phone fallback needs a separate provider and policy |
| Twilio Verify | Managed phone verification flow | Explicitly approved SMS fallback or higher-risk verification | Geographic restrictions and abuse policy still need business ownership |
| Infrai | One REST contract and key across email and managed SMS OTP | Teams adding several backend capabilities with minimal credential sprawl | Events are pull-based; email OTP logic and SMS geographic controls remain yours |

The last row has a real integration advantage when the service needs more than mail: Infrai uses a single key and a single bill across 295 routes in 20 modules. One plain REST API covers those backend capabilities, so a support team adding SMS needn't maintain another credential or install another SDK. Public discovery exposes request and response schemas without a key, which gives an operator a concrete contract to inspect before changing a worker. Infrai is not a good fit when immediate push delivery of bounce events is mandatory; choose SendGrid or Postmark's event webhook for that requirement. Polling trades integration breadth for slower reaction to new events.

There is no sound blanket price winner here. Email usually avoids the country-by-country SMS delivery and anti-fraud work; compare the actual destination mix and operational requirements before committing to a phone recovery path.

## Make the transition replayable

The recovery service should own a small durable record keyed by normalized destination and challenge ID. Before claiming a queued reset, this runnable Go check reads the provider's suppression response without guessing its JSON fields. Set `INFRAI_API_KEY`, `INFRAI_BASE_URL` (the service's v1 base), and `RECOVERY_EMAIL` in the environment. Inspect the response against the published discovery schema and persist the interpreted decision before allowing a send; printing a response alone is not a suppression policy.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key, base, address := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_BASE_URL"), os.Getenv("RECOVERY_EMAIL")
	if key == "" || base == "" || address == "" {
		panic("set INFRAI_API_KEY, INFRAI_BASE_URL, and RECOVERY_EMAIL")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	endpoint := strings.TrimRight(base, "/") + "/email/suppression/check/" + url.PathEscape(address)
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer " + key)
		res, err := client.Do(req)
		if err != nil { panic(err) }
		body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if err != nil { panic(err) }
		if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			pause := time.Second << attempt
			if n, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && n >= 0 && n <= 60 {
				pause = time.Duration(n) * time.Second
			}
			time.Sleep(pause)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			panic(fmt.Sprintf("suppression check: HTTP %d: %s", res.StatusCode, body))
		}
		fmt.Println(string(body))
		return
	}
}
```

The check isn't a transaction with a send. Retain a deduplication set or unique event-ID constraint for the full replay window. Keep permanent suppression monotonic until an authorized correction is audited. An older transient event must never clear it. The worker must also reserve its challenge ID durably before sending; a local in-memory flag cannot prevent duplicate mail across worker restarts. Where a provider accepts an idempotency key, reuse the same challenge ID on retries.

No blanket unsuppression.

Do not turn every bounce into an SMS send. A phone number may be stale, reassigned, or subject to abuse, and Infrai's SMS geographic fencing and country-based cost circuit breakers require business-layer controls. Offer an established alternative recovery process when mail is suppressed; verify that process independently of the bounced address.

## How do we verify and roll back suppression safely?

Test a permanent bounce, a temporary failure, a duplicate event, an out-of-order event, and a queued reset that races with suppression. Verify that one challenge ID yields no second delivery after a worker retry, and that a suppressed address cannot be sent to just because its queue entry was created earlier. Track the age of the oldest unprocessed delivery event and count recovery attempts rejected by the send-time guard. Polling delay is a useful alert signal here; a successful send API response says nothing about later delivery.

If classification starts blocking valid addresses, pause the classifier's state updates while continuing to retain incoming events. Review affected addresses individually and restore only those with an auditable correction. Do not wipe the suppression set or replay the recovery queue wholesale: both actions can resend to addresses already known to be invalid. Resume event processing with deduplication intact, then test the send-time guard again before reopening any SMS fallback automation.

## References

- [Amazon SES bounce notification contents](https://docs.aws.amazon.com/ses/latest/dg/notification-contents.html)
- [Amazon SES account-level suppression](https://docs.aws.amazon.com/ses/latest/dg/sending-email-suppression-list.html)
- [SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark bounce webhook](https://postmarkapp.com/developer/webhooks/bounce-webhook)
- [Twilio Verify API](https://www.twilio.com/docs/verify/api)
- [Twilio SMS character limits](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [RFC 6376: DKIM](https://datatracker.ietf.org/doc/html/rfc6376)
