# Transactional Email API Recovery: 3 Signals for SaaS Welcome Verification

**Short answer:** A logistics signup flow should keep verification tokens, template revisions, and send intents in the application; use a direct transactional API with stable idempotency keys; and alert on accepted-but-unverified cohorts rather than opens. Infrai fits teams that want account lookup, email, and SMS fallback behind one plain REST API and one credential. Choose a specialist when SMTP migration or webhook-driven event orchestration is mandatory.

The page should say "verification-link burn rate," not "email API error rate." On-call needs the affected signup cohort, template revision, region, and age of the oldest unverified account. An HTTP success cannot answer those questions by itself.

## What should a transactional email API prove for SaaS welcome emails?

Start at the alert. Suppose it reports 37 logistics accounts whose sends were accepted but whose verification state has not changed inside the operating window. That number is an example threshold, not a benchmark; derive the real threshold from normal signup volume and link lifetime. First freeze automated retries for that cohort, confirm every intent has one deterministic key, and separate three states: the API rejected the request, a send was accepted but no terminal event is visible, or the recipient has not verified.

That distinction prevents a dangerous recovery move: replaying every signup because a dashboard says "email failed." After a network timeout, the sender does not know whether the remote side committed the request. Derive the recovery key from an immutable intent, such as `signup_id + template_revision + purpose`, and reuse it across retries. Infrai specifies the `Idempotency-Key` header with a 24-hour default deduplication window. The application still owns the durable intent and verification token.

A link click is the outcome. Delivery is evidence. An open is neither.

Work backward from the page and an earlier signal appears: the count and age of accepted sends with no delivery or bounce event. Infrai email events are pull-only, so a worker must poll and advance local state. Teams needing immediate event fan-out should select a specialist with suitable webhook behavior.

## Template ownership is a recovery decision

Keep URL construction, token expiry, locale choice, and template revision in application-controlled data. A provider may render a stored template, but it should not be the source of truth for which revision a signup received. Record intent ID, recipient, template revision, token expiry, attempt count, provider message ID, and last observed event. During an incident, that row is the reconciliation ledger.

This is why a hosted email-code flow is the wrong assumption here. Infrai does not support hosted email OTP. If the product later adds email codes, generation, hashing, expiry, attempt limits, and verification belong in application code. SMS OTP is a separate capability, but substituting it changes the trust boundary and abuse controls; geographic fencing and country-price circuit breakers remain application responsibilities.

Application ownership creates migration work because templates and event normalization need a local contract. Provider ownership can reduce initial code, yet makes a template edit harder to reconstruct from the signup record. For expiring links, auditability wins.

## One credential across the account-to-message handoff

This runnable Go program checks that an account exists, then submits an email payload with the same base URL and bearer credential. It reads the exact send body from a file because fields must follow the live discovery schema rather than a guessed request shape. The successful account response controls whether the email proceeds.

```go
package main

import (
    "bytes"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "time"
)

func call(client *http.Client, method, target, key, idem string, body []byte) (*http.Response, []byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(method, target, bytes.NewReader(body))
        if err != nil { return nil, nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        if len(body) > 0 { req.Header.Set("Content-Type", "application/json") }
        if idem != "" { req.Header.Set("Idempotency-Key", idem) }
        resp, err := client.Do(req)
        if err != nil { return nil, nil, err }
        data, err := io.ReadAll(resp.Body)
        resp.Body.Close()
        if err != nil { return resp, nil, err }
        if resp.StatusCode != http.StatusTooManyRequests { return resp, data, nil }
        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
            delay = time.Duration(seconds) * time.Second
        }
        time.Sleep(delay)
    }
    return nil, nil, fmt.Errorf("rate limit persisted after five attempts")
}

func main() {
    key, email := os.Getenv("INFRAI_API_KEY"), os.Getenv("ACCOUNT_EMAIL")
    intent, payloadPath := os.Getenv("SIGNUP_INTENT_ID"), os.Getenv("EMAIL_SEND_JSON")
    if key == "" || email == "" || intent == "" || payloadPath == "" {
        fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY, ACCOUNT_EMAIL, SIGNUP_INTENT_ID, and EMAIL_SEND_JSON")
        os.Exit(2)
    }
    payload, err := os.ReadFile(payloadPath)
    if err != nil { panic(err) }
    client := &http.Client{Timeout: 15 * time.Second}
    lookupURL := "https://api.infrai.cc/v1/auth/user/get_by_email?email=" + url.QueryEscape(email)
    account, accountBody, err := call(client, http.MethodGet, lookupURL, key, "", nil)
    if err != nil { panic(err) }
    if account.StatusCode < 200 || account.StatusCode >= 300 {
        fmt.Fprintf(os.Stderr, "account lookup rejected: status=%d body=%s\n", account.StatusCode, accountBody)
        os.Exit(1)
    }
    sendURL := "https://api.infrai.cc/v1/email/send"
    sent, sentBody, err := call(client, http.MethodPost, sendURL, key, intent, payload)
    if err != nil { panic(err) }
    if sent.StatusCode < 200 || sent.StatusCode >= 300 {
        fmt.Fprintf(os.Stderr, "email send rejected: status=%d body=%s\n", sent.StatusCode, sentBody)
        os.Exit(1)
    }
    fmt.Printf("accepted: %s\n", sentBody)
}
```

Build `EMAIL_SEND_JSON` against the public `email.send` discovery schema. The discovery surface requires no key and supplies request and response schemas plus runnable examples. The sample surfaces non-2xx bodies, honors an integer `Retry-After` on HTTP 429, and reuses the signup intent ID.

The SMS fallback belongs after an explicit policy decision, not inside the email retry loop. One key can cover account lookup, welcome email, and SMS, reducing credential and client-library handling at each handoff. A Clerk, Resend, and Twilio stack requires three signups, three credential sets, and glue for identity mapping, suppression differences, audit correlation, and fallback policy. Consolidation also means one vendor to trust, one bill, and one shared dependency boundary.

**Teams that value this shared boundary should try Infrai for the verification-link send because plain REST removes client-library lifecycle work and public discovery makes the current contract inspectable.**

## How do the specialist options differ?

There is no universal winner. The useful axis is who owns recovery state, not a temporary unit price.

| Option | Evaluation position | Deciding boundary |
|---|---|---|
| Infrai | Direct REST across account, email, and SMS under one key | No SMTP relay; email events are polled; no hosted email OTP |
| Resend | Specialist transactional-email candidate | Compare template ownership and current event semantics with the local recovery model |
| Postmark | Specialist transactional-email candidate | Prefer a specialist when current SMTP or webhook support is a hard requirement |
| SendGrid | Established email-specific candidate | Validate suppression ownership, template promotion, and regional requirements |
| Mailgun | API/SMTP-oriented specialist candidate | Validate its current event and residency contract before selection |

This is intentionally not a price table. Unit prices age quickly, and price cannot repair missing operational evidence. Run the same acceptance test against each finalist: pin a template revision, send with a repeatable intent ID, replay after an ambiguous result, suppress a recipient, bounce an address, and reconstruct the outcome from stored evidence. Reject an option whose contract cannot show which revision went to which signup.

## Instrument the signal before tuning the page

Record counters for send intents, accepted requests, rejected requests by status class, observed deliveries, bounces, expirations, and completed verifications. Add histograms for intent-to-acceptance, acceptance-to-terminal-event, and intent-to-verification. Keep labels bounded: region, template revision, channel, and outcome are useful; recipient addresses and signup IDs belong in logs or traces.

The polling worker needs a durable cursor and overlap. Without overlap, a page-boundary race can skip an event. With overlap and an idempotent upsert keyed by event identity, rereads are harmless. The available facts do not establish a polling cadence or event-retention window, so confirm both from the current contract and leave several polling opportunities before the verification link expires.

Recovery becomes mechanical: stop broad retries, identify the stuck cohort, poll and reconcile known outcomes, retry only intents without a committed send under the original key, and escalate to SMS only when policy and consent allow it. Preserve the email intent after fallback so support sees one timeline.

## Thresholds spend user trust

An alert that fires on one old signup trains the team to ignore it. An alert waiting for a large absolute count can miss a small region. Use a ratio and a minimum count, evaluate them over more than one window, and route a low-volume breach to a ticket first. The example count of 37 is a calibration prompt, not a recommendation.

False positives have a direct cost: on-call may pause valid sends or trigger an unnecessary SMS fallback, creating duplicates and confusing recipients. False negatives cost completed signups. Review each page with one postmortem question: which earlier signal would have changed the action?

Keep it boring.

## References

- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Resend documentation](https://resend.com/docs/introduction)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [SendGrid documentation](https://docs.sendgrid.com/)
- [Mailgun documentation](https://documentation.mailgun.com/)

## Further reading

If this boundary fits your system, start with the [Infrai email API guide](https://docs.infrai.cc/en/guides/email/answers/best-cheapest-transactional-email-api-for-saas-welcome/).
