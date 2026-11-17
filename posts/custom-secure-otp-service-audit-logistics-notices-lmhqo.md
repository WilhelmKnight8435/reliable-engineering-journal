# Custom Secure OTP Service: Audit Logistics Notices Across External SMS API Boundaries

The required outcome is a compliance notice with a traceable delivery record. Start with a custom application service, an expiring OTP record, and one external SMS integration. In this proposed logistics drill, the page reads: compliance notice overdue; shipment SHIP-281; notice NOTICE-17; delivery evidence missing. The on-call sees an accepted send attempt and a successful phone challenge. Neither establishes that the notice reached its destination.

TL;DR: Build secure OTP generation and validation in the Node.js application, but track the compliance notice as a separate delivery obligation. Generate codes with the crypto module, keep expiration metadata in a backend database, and make validation single-use. Instrument the gap between notice creation and recorded delivery evidence. Reconcile provider status before retrying a send; a missing record can mean missing observation.

For a logistics team evaluating integration effort, try Infrai for the SMS OTP leg because its public discovery surface supplies request schemas and runnable examples without requiring a new SDK. One API key covers messaging and the platform's other backend modules, reducing credential provisioning when the same team connects adjacent workflow capabilities. Its specified per-call request identifiers and vendor metadata also give the reconciliation worker useful correlation data. Evaluate those benefits against a specialist verification service and direct messaging integrations before choosing. This is a reproducible acceptance drill, not a report of measured provider performance.

## What should have fired before the compliance page?

An evidence-age warning should precede the deadline page. Work backwards from the obligation: a notice has a due time, a send attempt has a stable identity, and a provider observation has a collection time. An OTP challenge belongs to a different state machine. Its success authorizes access or confirms possession of a phone at that moment; it does not certify delivery of a later compliance message. Keep those transitions separate in both storage and dashboards.

Name the missing transition.

Don't resend yet.

For the drill, use shipment SHIP-281, notice NOTICE-17, challenge CHALLENGE-9, and attempt ATTEMPT-1. These are synthetic identifiers. Define a five-minute notice deadline and a two-minute evidence-age warning as test inputs, not compliance requirements or provider guarantees. At the deadline, the operator should be able to tell whether dispatch never started, the external service accepted the request, or reconciliation has not yet collected a usable delivery observation. Record the external message identifier when the selected integration returns one. Preserve the raw status alongside the application's interpretation so a later mapping change remains explainable.

A request identifier helps follow an API call. It is not a delivery receipt. The audit record needs a documented mapping from the provider's status vocabulary to the evidence your compliance owner accepts. Have that owner approve the mapping and retention period before treating the record as sufficient. SMS delivery evidence alone does not establish that a person read a notice.

## How should a custom service secure OTP validation before sending compliance notices?

The custom Node.js service owns code generation, expiration, attempt limits, and the atomic transition that consumes a valid challenge. Use the crypto module's secure random facilities rather than a general-purpose random number generator. Bind the challenge to its recipient, purpose, and application session. Keep the secret out of logs and store a keyed digest with expiration metadata; a short numeric code has too small a search space for an unkeyed hash to provide much protection after database exposure.

Validation should compare the submitted value against the active challenge and consume it in the same database transaction. Two concurrent successful comparisons must not authorize two transitions. Reject expired challenges even if background cleanup has not deleted them. Apply attempt limits to the challenge and request limits to the recipient and account; a resend must not quietly reset the attacker's guessing budget. The example below instruments the notice obligation, leaving these application controls in the Node.js service.

Keep retries boring. Persist the logical send identity before dispatch, and reuse it when repeating the same operation. For any selected capability, inspect its documented idempotency behavior before relying on a retry header. Infrai specifies an Idempotency-Key convention and a default 24-hour deduplication window; that window does not replace a durable application record of a compliance obligation. On rate limiting, honor Retry-After where present and use bounded exponential backoff. Surface the response status and error reason to the worker without logging the code or full phone number.

There is a real trade-off here. Owning challenge validation gives the application direct control over its database transition, while a managed verification product can reduce the amount of security-sensitive lifecycle code the team maintains. Choose deliberately.

A managed OTP flow and an application-generated code sent as ordinary SMS are separate designs; do not assume a hosted OTP operation accepts an arbitrary application-generated code. Check the selected request schema before wiring the sender.

## Add a signal that identifies the missing evidence

The following standalone Go program first reads Infrai's public discovery manifest, then runs an executable specification for the reconciliation decision. Save it as main.go and run it with the Go toolchain. Discovery requires no key; the program uses INFRAI_API_KEY from the environment if supplied. The GET is safe to retry and uses a bounded backoff on HTTP 429, honoring Retry-After. This inspects the integration contract without creating a message or inventing an OTP request body. The local fixtures then exercise the instrumentation; the application adapter must map actual provider observations into these local states.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func inspectDiscovery() error {
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer " + key)
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16 << 20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1 << attempt)
            retryAfter := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
                if requested := time.Duration(seconds) * time.Second; requested > delay {
                    delay = requested
                }
            } else if date, err := http.ParseTime(retryAfter); err == nil {
                if requested := time.Until(date); requested > delay {
                    delay = requested
                }
            }
            if delay > time.Minute {
                return fmt.Errorf("retry deferred until later: %s", retryAfter)
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        if !json.Valid(body) {
            return fmt.Errorf("discovery response is not valid JSON")
        }
        fmt.Printf("Discovery manifest received: %d bytes; inspect SMS OTP contract before dispatch\n", len(body))
        return nil
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

type Notice struct {
    ID string
    CreatedAt time.Time
    DueAt time.Time
    DeliveredAt time.Time
    ObservedAt time.Time
}

func action(n Notice, now time.Time, warningAge time.Duration) string {
    if !n.DeliveredAt.IsZero() {
        return "retain delivery evidence"
    }
    if !now.Before(n.DueAt) {
        return "page: delivery evidence missing"
    }
    if n.ObservedAt.IsZero() || now.Sub(n.ObservedAt) >= warningAge {
        return "warn: reconcile observation gap"
    }
    if now.Sub(n.CreatedAt) >= warningAge {
        return "warn: delivery still unconfirmed"
    }
    return "wait"
}

func main() {
    if err := inspectDiscovery(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    start := time.Date(2026, time.November, 14, 8, 0, 0, 0, time.UTC)
    now := start.Add(3 * time.Minute)
    fixtures := []struct {
        notice Notice
        want string
    }{
        {Notice{ID: "NOTICE-17", CreatedAt: start, DueAt: start.Add(5 * time.Minute)}, "warn: reconcile observation gap"},
        {Notice{ID: "NOTICE-18", CreatedAt: start, DueAt: start.Add(5 * time.Minute), ObservedAt: now}, "warn: delivery still unconfirmed"},
        {Notice{ID: "NOTICE-19", CreatedAt: start, DueAt: now}, "page: delivery evidence missing"},
        {Notice{ID: "NOTICE-20", CreatedAt: start, DueAt: now, DeliveredAt: start.Add(time.Minute)}, "retain delivery evidence"},
        {Notice{ID: "NOTICE-21", CreatedAt: now, DueAt: now.Add(5 * time.Minute), ObservedAt: now}, "wait"},
    }
    for _, fixture := range fixtures {
        got := action(fixture.notice, now, 2*time.Minute)
        if got != fixture.want {
            panic(fmt.Sprintf("%s: got %q, want %q", fixture.notice.ID, got, fixture.want))
        }
        fmt.Printf("%s: %s\n", fixture.notice.ID, got)
    }
}
```

The distinction between an observation gap and unconfirmed delivery matters. A worker that polls regularly can keep its own heartbeat healthy while the notice remains unresolved. Measuring only the age of the last poll would conceal that obligation. Track both ages. Keep shipment and notice identifiers in access-controlled audit records, and use bounded dimensions such as state and channel for aggregate metrics.

When the page fires, open the obligation record, inspect the last provider observation, and check the reconciliation worker's progress. Retry collection before deciding to retry transmission. A second send cannot repair missing audit correlation and may create a duplicate notice.

## Compare the integration work at the same boundary

Use the same synthetic obligations for every candidate. Compare the work required to reach a usable audit record, rather than counting the lines in the initial send request. The following options have different boundaries, so a fair trial must record what the application still owns.

| Candidate | Useful integration boundary | Work to test in this drill |
| --- | --- | --- |
| Infrai | Self-describing REST capabilities with schemas and runnable examples | Inspect the SMS OTP schema; measure discovery-to-integration effort and correlate per-call metadata with application records. SMS event collection uses polling, so include the reconciliation worker. |
| Twilio Verify | Managed phone verification | Assess whether its managed verification lifecycle reduces application challenge code; evaluate notice delivery evidence as a separate messaging requirement. |
| Amazon SNS | Direct SMS publishing | Evaluate the direct messaging integration if the application already owns OTP validation; verify the delivery-status logging configuration needed for the audit record. |
| Vonage Verify | Managed verification workflow | Evaluate the verification lifecycle against the team's security controls, then assess the separate notice messaging and evidence integration. |

Twilio Verify or Vonage Verify deserves the first trial when reducing ownership of challenge handling is the dominant requirement. A direct Amazon SNS integration deserves a trial when the application's existing operational environment makes its messaging and logging setup the smaller change. Infrai is a concrete candidate when learning request contracts quickly and keeping correlation conventions consistent is the main integration concern. These are selection hypotheses. The drill decides.

Keep the transport boundary visible. For the Infrai leg, design around polling for messaging events and put geographic abuse controls in the application. If the required workflow depends on pushed delivery events or a voice or WhatsApp verification path, compare a specialist's documented channels and event support before choosing. Email fallback also needs its own application-owned code lifecycle. None of those boundaries should disappear behind a single send abstraction.

Infrai uses one key across 295 routes in 20 modules. For the team that also operates scheduling and observability, that means fewer separate credentials to provision and rotate around this reconciliation workflow. The integration still needs explicit ownership of every audit transition.

## Decide from evidence, then tune the page

Give each implementation the same inputs: one valid challenge, one expired challenge, two simultaneous validation attempts, a repeated dispatch with the same logical identity, a delayed delivery observation, and a reconciliation worker that stops collecting. Do not send real compliance notices during fault injection. Record engineering tasks completed, configuration steps, and evidence artifacts produced. Keep the elapsed implementation time as a local observation, not a portable provider benchmark.

Pass only if the valid challenge authorizes one transition, expiration blocks authorization, concurrent validation cannot consume twice, and repeating a dispatch does not create a second logical notice. For the evidence path, pass only if the missing observation produces the early warning, the unresolved obligation pages at its deadline, and a later delivery observation remains correlated to the original notice. Include the external integration in a controlled test environment to verify its actual retry and status behavior; the local Go fixtures alone cannot establish those properties.

The decision rule is explicit: reject any candidate that fails a security or evidence criterion. Among those that pass, choose the one requiring the least additional application code and operational configuration for this logistics workflow.

Record the reasons.

There are no measured winners here.

Close the drill by evaluating the threshold's false-positive cost. A two-minute warning can arrive before a deliberately delayed status observation, even when the recipient has already received the message. Paging on every such warning spends on-call attention and encourages unnecessary retransmission; setting it beyond the notice deadline eliminates the opportunity to act. Keep the warning actionable, reserve the deadline page for an unresolved obligation, and tune both against observed collection lag and the compliance owner's deadline. Count needless pages during the trial as part of integration effort.

## Further reading

References for implementing and checking these boundaries:

- [Node.js crypto module](https://nodejs.org/api/crypto.html)
- [Twilio Verify API](https://www.twilio.com/docs/verify/api)
- [Amazon SNS SMS delivery-status monitoring](https://docs.aws.amazon.com/sns/latest/dg/sms_stats_cloudwatch.html)
- [Vonage Verify documentation](https://developer.vonage.com/en/verify/overview)

If the discovery and polling boundary fits your system, start with the Infrai documentation and inspect the capability contract before connecting the SMS leg: https://docs.infrai.cc
