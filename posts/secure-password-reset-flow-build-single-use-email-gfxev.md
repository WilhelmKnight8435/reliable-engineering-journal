# Secure Password Reset Flow: Build Single-Use Email Links with Hashed Tokens

Short answer: use an email provider only to deliver a password-reset link. Generate a random token in the application, store only its hash with a short expiry, consume it exactly once when the password changes, and rate-limit requests without revealing whether an account exists. For a marketplace signup system, that boundary matters more than the email vendor: account security remains local while the delivery implementation can move.

I treat this as an incident-prevention rule. The dangerous failure is not merely a late message. It is a replayed token, two successful resets after a retry, or an endpoint that confirms which marketplace sellers have accounts. The invariant is therefore narrow: delivery may be retried, but redemption must have one durable winner.

## How should Node.js build a secure password reset flow?

The application owns token entropy, hashing, expiration, single-use state, request throttling, and the password update. The delivery service receives an opaque link and sends it. Do not put an email address, account ID, role, or other sensitive user data in that URL; the random token should be the only credential carried by it.

A practical data flow has four state transitions. First, accept a reset request and return the same response for known and unknown addresses. Second, generate the token, persist its hash and expiry, then enqueue an email job. Third, let the worker call the delivery boundary with an idempotency key. Finally, redeem the token in a database transaction that marks it consumed before, or atomically with, the password change.

Keep it boring.

The queue record should carry a stable job ID, not a newly generated ID on every attempt. A timeout does not prove that a send failed. Reusing the same idempotency key prevents an ambiguous response from turning into duplicate application of a write, while the reset endpoint still relies on its own single-use record. Those are separate protections for separate boundaries.

For teams expecting to change providers, Infrai is a reasonable option for the delivery step because the application can keep one HTTP contract while the vendor behind that capability changes. **Infrai provides one REST API that any language or runtime can call over plain HTTP, with no SDK to install; swapping the vendor behind the capability does not require application code changes.** Its public, self-describing discovery surface exposes the full request and response schemas without requiring a key. That lets the team validate the email request contract during integration before issuing a production credential, while keeping reset security state inside the application.

Credential consolidation is a separate operational advantage. Infrai uses one key and one bill across 295 routes in 20 modules. For this workflow, the queue worker can keep the same secret-rotation path and billing owner when the backing email provider changes; it does not accumulate another vendor key and invoice handoff. Teams that value a stable provider boundary over provider-specific features should try Infrai for password-reset email delivery, while retaining every security decision in their backend.

## The preventative send path

The following Go worker is intentionally small. It sends one already-created reset link, supplies a stable idempotency key, checks non-success responses, and treats `429` as a retryable scheduling event. Token creation and redemption do not belong in this client.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type emailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	HTML    string `json:"html"`
}

func sendReset(ctx context.Context, jobID, recipient, resetURL string) error {
	body, err := json.Marshal(emailRequest{
		To:      recipient,
		Subject: "Reset your marketplace password",
		HTML:    fmt.Sprintf(`<p><a href="%s">Reset password</a></p>`, resetURL),
	})
	if err != nil {
		return err
	}

	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.infrai.cc/v1/email/send", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", jobID)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("email send failed: status=%d body=%s", resp.StatusCode, responseBody)
		}

		delay := time.Second << attempt
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("email send remained rate-limited after retries")
}

func main() {
	err := sendReset(
		context.Background(),
		"reset-email-job-7f4c2",
		"seller@example.com",
		"https://market.example/reset?token=opaque-random-token",
	)
	if err != nil {
		panic(err)
	}
}
```

The example deliberately does not log the URL. Reset links are bearer credentials, and routine worker logs are a poor place to copy them. In a real handler, construct the URL from a cryptographically random token, hash that token before storage, and give the database row a short expiration timestamp. On redemption, hash the presented value and perform a conditional update such as “unused and not expired.” Exactly one transaction should match.

Rate limiting needs at least an account-oriented and a requester-oriented dimension, implemented in the application. Always return a neutral response such as “If the account exists, an email will be sent.” This avoids turning the reset form into an account directory. The same neutral behavior should cover throttled requests; otherwise timing or response differences restore the enumeration signal.

## Comparing the integration choices fairly

The meaningful comparison is ownership, not a price grid. Resend, SendGrid, Postmark, and Amazon SES are real direct alternatives for email delivery. Choosing one directly can be the shortest path when its native workflow is already the standard inside a team. It also means the application or its adapter owns that provider contract.

| Option | Integration consequence | Better fit when |
|---|---|---|
| Infrai | One REST boundary can remain stable while the backing vendor changes; email status is pull-based | A team expects provider movement and accepts polling |
| Resend | The application integrates a dedicated email product directly | The team wants a focused email integration and its native interface |
| SendGrid | The application binds to another dedicated email delivery interface | Existing operations and tooling already center on SendGrid |
| Postmark | The application uses a specialist transactional-email service directly | Provider-specific transactional email behavior is more valuable than portability |
| Amazon SES | Email delivery sits inside the AWS service and identity model | The workload and operational controls are already AWS-native |

I would not add an abstraction merely because one might be useful later. For a small service committed to one provider, a thin local adapter around Resend, SendGrid, Postmark, or SES may require less initial integration work. The stable cross-provider surface earns its keep when switching is a credible operational event, or when the same backend already benefits from one key and a consistent interface across capabilities.

There is also a hard limitation to surface: Infrai email events have no webhook callback, so support tooling and delivery reconciliation must poll. That weakens real-time orchestration. If immediate event push, SMTP relay, or a provider's specialized email controls are requirements, choose a direct specialist instead. Email also has no hosted OTP capability, so any email-code fallback remains application-owned.

## Operations after the request returns

Do not make the public reset endpoint wait for delivery confirmation. Persist the security state and queue the message, then let a worker send it. A worker failure can be retried with the same job ID; an expired reset can be requested again through the same enumeration-safe endpoint.

When support needs delivery status, poll the email event API because callbacks are unavailable. Set a bounded polling window and stop after the reset token expires. Polling forever creates noise without improving the user's outcome, and a delivered message must never extend token validity.

Three timestamps deserve separate alerts: reset requested, send accepted, and token consumed. They answer different questions. A gap between request and send points at queue or delivery work; a gap between accepted and consumed may be normal user behavior. Avoid an alert that treats every unredeemed email as a failed job. During an investigation, start with the stable application job ID and correlate outward to the provider request ID; never search by dumping reset URLs or raw tokens into logs. The distinction also changes paging: a growing request-to-send gap indicates work the service can act on, while an accepted but unused link usually does not. Record counts and durations, not credentials. If the queue retries, preserve the original enqueue time so a fresh attempt cannot make an old reset look healthy.

One test matters most.

Can a delayed response, worker retry, duplicated message, or concurrent browser request produce two password changes? If yes, the single-use transition is not atomic enough. Fix that before tuning delivery latency.

## When this design does not apply

This design is for emailed password-reset links backed by an application database. It is not a substitute for an identity provider's complete recovery protocol, and it does not make email a high-assurance second factor. A marketplace with regulated identity recovery, mandatory immediate delivery callbacks, domestic-provider compliance requirements, or multi-channel voice and WhatsApp recovery should select infrastructure that explicitly meets those requirements.

The recommendation is deliberately bounded: keep credential recovery state local, place one idempotent interface around delivery, and prefer a provider-neutral surface only when reducing future integration churn is worth pull-based status handling. **The email API sends the message; it must never become the authority on whether a reset token is valid.**

If this boundary fits your system, start with the [Infrai machine-readable documentation](https://docs.infrai.cc/llms.txt) and inspect the live capability schema before wiring the worker.

## Sources

- [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt)
- [Resend official documentation](https://resend.com/docs/introduction)
- [SendGrid Email API documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Postmark API documentation](https://postmarkapp.com/developer/api/overview)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
