# Transactional Email Failure Containment Across SaaS Onboarding and Edtech Support Intake

**Short answer:** Choose the email API that lets your application persist the event, retry the notification, and deduplicate the support action independently; integration effort is secondary to a failure boundary you can operate.

The decisive feature of an email API for onboarding is not its send method. It is how cleanly the system contains a delivery failure without losing the business event or sending it twice. For an edtech contact form, that means preserving the learner's request, routing it to the correct support queue, and treating the confirmation email as a consequence that can be retried independently. The same boundary keeps a SaaS welcome email from becoming an outage in account creation.

## What should fail when the email provider is unavailable?

A contact form should accept and durably record a valid request before it attempts delivery. The request can then produce two separate effects: a queue-routing event and a notification event. If the notification API times out, the support ticket still exists. If queue routing fails, the confirmation email should not claim that a case is ready for an agent.

That distinction sounds obvious until the handler owns all three operations. A five-second provider timeout becomes a slow form submission; a client retry creates a second ticket; a response lost after the provider accepted the message causes the worker to resend it. I have learned to draw the failure boundary before choosing an API. The boundary is more useful than a feature matrix.

That is the whole contract.

For onboarding, use the same rule. Account creation and the welcome message are different state transitions. Store the intended message with a stable event identifier, then let a worker deliver it. A provider outage should increase queue age, not roll back a successfully created account.

## How do you stop retries from creating duplicate support work?

Retries are necessary because timeouts are ambiguous. They are dangerous because an accepted request and an accepted email are not the same observation. An idempotency key gives the application a stable identity for the logical send, but it does not make every external endpoint idempotent by magic. Record the key, payload hash, attempt count, and provider result locally.

A small outbox worker can make that state explicit. This example keeps the provider behind a generic interface; the important part is the transition ordering and the stable key, not a vendor-specific SDK.

```go
type Message struct {
	ID          string
	To          string
	Template    string
	PayloadHash string
	Attempts    int
	SentAt      *time.Time
}

type EmailSender interface {
	Send(ctx context.Context, key string, message Message) error
}

func deliver(ctx context.Context, store Store, sender EmailSender, id string) error {
	m, err := store.ClaimPending(ctx, id)
	if err != nil {
		return err
	}

	key := "contact-form/" + m.ID + "/" + m.PayloadHash
	err = sender.Send(ctx, key, m)
	if err != nil {
		_ = store.RecordAttempt(ctx, m.ID, err.Error())
		return err
	}

	now := time.Now().UTC()
	return store.MarkSent(ctx, m.ID, now)
}
```

`ClaimPending` must be a lease or an atomic state change, and the lease needs an expiry for a worker that dies mid-send. `MarkSent` must be safe to repeat. Those details matter more than whether the API call is wrapped in a particularly pleasant client library. A duplicate is still possible when the remote system accepts a request and the worker dies before recording success, so the receiving path needs a deduplication policy as well. For support intake, deduplicate on the form submission ID and show the original case to the user when a retry arrives.

When I am paged for a missed job, I want 3 answers before I replay anything: does the outbox row exist, did the worker lease expire, and can the destination identify the original submission? If those answers are not visible in one trace, the integration is too opaque for production. A second replay can compound the incident, especially when a learner has submitted a sensitive account question and two agents begin working the same case.

## Which delivery checks belong in the integration?

Separate message correctness from mailbox reachability. In tests, assert that a contact submission produces one queue event and one outbox row. Assert that a retry reuses the same logical ID. Then exercise the ambiguous result: the fake sender records the message and returns a timeout. The next worker run should not silently create another support case.

In production, the useful signals are queue age, pending count, attempt distribution, provider latency, rejected-message count, and the time from accepted form to routed case. Alert on the business symptom first. A rising queue age for support intake deserves attention even when the provider reports healthy HTTP responses, because routing and delivery are separate paths.

Authentication and domain setup also belong in the runbook. SPF describes a policy for which hosts are authorized to send mail for a domain; it is one part of sender validation, not a guarantee that a message reaches an inbox. Keep DNS changes, DKIM key rotation, return-path handling, bounce processing, and suppression behavior documented next to the deployment. Test with a controlled domain before changing the production sender.

## When is a managed email API the wrong boundary?

A hosted API is a reasonable boundary when the team wants to operate application-level retries and delegate SMTP delivery operations. Its limitation is that it cannot prove a person read the message, and an HTTP success cannot prove that the support queue accepted the case. It is a poor boundary when provider-specific status codes leak through every business handler. Keep an internal send contract and map external results at one adapter.

The integration-effort decision should include the surrounding work: domain authentication, templates, event storage, retry policy, bounce ingestion, observability, and exit procedures. A service with a short initial integration can still demand substantial operational work later. Conversely, a generic adapter may take an extra day and prevent a provider change from reaching the contact-form handler, the onboarding workflow, and the tests.

Do not optimize for the cheapest individual send. Optimize for a failure mode the on-call engineer can explain at 03:00: which event exists, which attempt is active, what the user saw, and how a replay avoids duplicate work.

The trade-off is deliberate: a little more state in the application buys a smaller incident.

## Sources

- https://datatracker.ietf.org/doc/html/rfc7208
- https://www.rfc-editor.org/rfc/rfc5321
- https://www.rfc-editor.org/rfc/rfc7489
