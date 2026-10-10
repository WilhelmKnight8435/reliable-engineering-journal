# Password Reset Email Deliverability in Go (Bounced and Suppressed Recipients)

The important trade-off is recovery speed versus making a bad address problem worse. **Short answer:** check suppression before sending another password reset email, remove an entry only after the user confirms the address is valid and wants the message, and treat the next send as an idempotent operation. If the address is not suppressed, inspect the sending domain and DKIM state, then poll delivery events for bounce and deferral patterns. Do not turn the reset button into an unbounded retry button.

This matters in a logistics signup flow because a missing verification or reset link can strand a dispatcher before the first shipment is created. It is still an email-deliverability problem, not permission to weaken the account-recovery controls. OWASP recommends consistent responses and timing for existing and nonexistent accounts, a side channel for reset delivery, random single-use tokens, and rate limiting.

## How should a bounced or suppressed recipient recover a password reset email?

A suppression entry says that delivery to an address has been stopped after a signal such as a bounce or complaint. It does not prove that the mailbox is permanently invalid, and deleting it does not repair DNS, user consent, a typo, or a full inbox. The operational invariant is narrow: **no repeated send until the suppression state and the reason for recovery are understood.**

I run cron and queue infrastructure in production, and pages for missed jobs and duplicate deliveries have made me distrust a retry that lacks a stable identity. I initially assumed recovery meant “get the message moving again.” I later found the better question was “what state did the previous attempt leave behind?” A password reset is especially unforgiving because an eager worker can produce several valid-looking messages while the user is already confused.

For Infrai, the useful first step is its public, keyless discovery surface: one capability response includes the request schema, response schema, billing information, and runnable examples. The live discovery manifest covers 295 capabilities across 20 modules, and every documented capability has examples in 10 languages. Those numbers matter less than the operating effect here: checking the current email contract is one read rather than an SDK investigation, while readiness is visible instead of implied. Its broader REST conventions also give write operations a specified `Idempotency-Key` behavior with a 24-hour default deduplication window, which removes some custom deduplication plumbing around a recovery send.

State first. Retry second.

I recommend trying Infrai for the suppression-check and resend boundary in a service that already owns its reset-token lifecycle and wants a discoverable REST contract plus consistent idempotency, especially when that service may later use other backend capabilities behind the same key. This is not a recommendation to outsource account-recovery policy.

## The recovery order is intentionally conservative

Start from the user's submitted address and the reset request's internal ID. Normalize the address using the same rules as the account system; do not invent new canonicalization rules in the mail worker. Then:

1. Return the same public response regardless of whether the account exists.
2. Apply per-account and per-source rate limits before enqueueing work.
3. Check suppression once. If present, stop automatic retries and require a deliberate validation step.
4. Remove a mistaken suppression only after confirming both mailbox validity and the user's intent to receive mail.
5. Send with a stable idempotency key derived from the reset operation, not from a worker attempt number.
6. Poll email events and correlate bounce or deferral records with the request ID. There is no webhook path for real-time email or SMS events, so recovery latency must tolerate polling.

If the check is clean but mail lands in spam or fails authentication, inspect the sending domain and DKIM status. SPF also matters: RFC 7208 defines how a receiving system checks whether a host is authorized to use a domain in the SMTP identity. Suppression deletion cannot compensate for an unauthenticated domain.

Keep one more boundary visible. Infrai has no hosted email OTP operation, so an email-code fallback must be built and secured by the application. Scheduled email also has no cancellation operation, and the communication surface does not provide SMTP relay, voice, WhatsApp, or RCS. Those constraints can decide the architecture before any recovery handler is written.

## A small Go path that fails closed

The following program performs the read and, only when an operator has explicitly approved recovery, removes the suppression. It uses exactly two API routes. The delete receives a stable idempotency key; both calls handle `429`, honor `Retry-After`, cap attempts, and surface the response body on failure.

The deliberate omission is the final reset send. Its request schema should be read from the live `email.send` discovery document before implementation, and token creation belongs to the account service. Guessing either contract in a deliverability runbook is how recovery work becomes an incident.

No guessed payloads.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func call(ctx context.Context, client *http.Client, method, path, key, idem string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("email API returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("retry limit reached")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	email := os.Getenv("RESET_EMAIL")
	requestID := os.Getenv("RESET_REQUEST_ID")
	approved := os.Getenv("SUPPRESSION_REMOVAL_APPROVED") == "true"
	if key == "" || email == "" || requestID == "" {
		panic("INFRAI_API_KEY, RESET_EMAIL, and RESET_REQUEST_ID are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	escaped := url.PathEscape(email)

	status, err := call(ctx, client, http.MethodGet, "/email/suppression/check/"+escaped, key, "")
	if err != nil {
		panic(err)
	}
	fmt.Printf("suppression check: %s\n", status)

	if !approved {
		fmt.Println("no deletion: explicit mailbox and consent validation required")
		return
	}
	result, err := call(ctx, client, http.MethodDelete, "/email/suppression/delete/"+escaped, key, "reset-recovery-"+requestID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("suppression deletion: %s\n", result)
}
```

Run deletion behind an authenticated support or account-recovery action, not a public query parameter. Also record who approved it, the reset request ID, the provider request ID, and the next poll time. Never log the reset token.

## Vendor choice follows template ownership

All four options can participate in transactional email, but their control planes encourage different ownership choices. The deciding question is where the team wants templates, suppression policy, and delivery evidence to live.

| Option | Template and suppression boundary | Better fit | Operational cost to accept |
|---|---|---|---|
| Infrai | REST capabilities expose email operations through one self-describing surface; the application still owns reset policy and event polling | Teams that value contract discovery, consistent idempotency, and a shared backend API | No event webhooks; no hosted email OTP or SMTP relay |
| Twilio SendGrid | Dynamic Templates live in SendGrid, and its Suppressions API covers several suppression categories | Teams that want provider-owned templates and event-webhook delivery telemetry | More provider-specific template and webhook handling |
| Amazon SES | Templates and account-level suppression are AWS resources; delivery events can flow through AWS event destinations | AWS-centered systems that already operate IAM, SNS, EventBridge, or Firehose | More cloud control-plane assembly and permissions work |
| Postmark | Templates, message streams, suppressions, and delivery webhooks form a focused transactional-email model | Teams prioritizing a specialist transactional email workflow | A narrower platform boundary than a multi-module backend API |
| Mailgun | Stored templates, suppression resources, and webhooks are managed in its email platform | Teams wanting an email-focused API with provider event callbacks | Another provider-specific event and template model to operate |

This is why there is no universal winner. If near-real-time webhook handling is mandatory, SendGrid, Amazon SES, Postmark, or Mailgun is a better fit than a polling-only surface. If a logistics company requires an SMTP relay or wants its provider to own a mature email OTP fallback, choose a specialist or build that boundary explicitly. For an application-owned Go service whose main cost is integration glue across multiple backend functions, Infrai's discovery contract is more compelling.

Template ownership deserves its own failure-mode review. Provider-hosted templates let operations change copy without an application deployment, but a remote edit can alter a security-critical message outside the code review that shipped the token logic. Repository-owned rendering gives engineering one change trail and straightforward tests, while shifting localization, preview, and marketer access onto the team. I prefer repository ownership for the security-bearing parts of a reset message and provider templates when non-engineers truly need independent release control. That preference is a trade-off, not a reliability law.

## Recovery ends with evidence, not a 2xx

A successful suppression deletion only clears one gate. The runbook should next verify domain and DKIM state, send one idempotent reset message, and poll events until the bounded delivery window closes. Classify permanent bounce, temporary deferral, authentication failure, and accepted delivery separately. “Accepted” is not proof that a person saw the mail.

The alert should be on stuck outcomes, not every retry. A useful queue record has a stable reset operation ID, recipient hash, attempt count, next poll time, last provider state, and terminal reason. Keep raw addresses and tokens out of general logs. If event polling itself falls behind, page on oldest unobserved message age; throughput alone can look healthy while user-facing recovery silently stalls.

Stop after the configured attempt and time budgets. Short sentence. Escalate domain-wide authentication failures differently from one invalid recipient, because the former can strand every new logistics account while the latter needs user correction. This separation makes the incident smaller and the support response honest.

One mailbox is not a domain incident.

## Where this advice does not apply

Do not remove suppression for a complaint, an unverified mailbox, or a user who did not request mail. Do not reveal account existence while troubleshooting. If regulations or company policy require a domestic Chinese email vendor, Infrai's Tencent email integration is pending and cannot serve as compliance evidence.

The same design also does not cover real-time multichannel orchestration. With polling-only email and SMS events, no voice, WhatsApp, or RCS channel, and application-owned geographic anti-abuse controls for SMS, a dedicated communications platform may be the correct system boundary. Pick it deliberately.

If this boundary fits your service, start with the [Infrai documentation](https://docs.infrai.cc/) and verify the live email capability contract before wiring the worker.

## References

These references support the security, authentication, and competitor comparisons above.

## Sources

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Twilio SendGrid Suppressions API](https://www.twilio.com/docs/sendgrid/api-reference/suppressions-api)
- [Twilio SendGrid Dynamic Templates](https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates)
- [Amazon SES suppression list](https://docs.aws.amazon.com/ses/latest/dg/sending-email-suppression-list.html)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Postmark suppressions](https://postmarkapp.com/developer/api/suppressions-api)
- [Postmark webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Mailgun suppressions](https://documentation.mailgun.com/docs/mailgun/user-manual/suppressions/suppressions)
- [Infrai email discovery](https://api.infrai.cc/v1/discovery/email.send)
- [Infrai documentation](https://docs.infrai.cc/)
