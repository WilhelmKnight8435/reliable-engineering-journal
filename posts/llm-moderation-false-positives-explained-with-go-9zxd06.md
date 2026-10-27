# LLM Moderation False Positives Explained With Go Policy Thresholds for Support Review

Short answer: an LLM moderation false positive is usually a routing failure before it is a model failure. A support ticket can quote an abusive message, discuss self-harm in a bug report, or contain a customer’s offensive example without being harmful content itself. The reliable design keeps the classifier score, the policy threshold, the evidence span, and the final action separate. Allow low-risk text, send ambiguous text to review, and block only when the policy and context justify it.

For a developer-tools team triaging customer-support tickets, I would add one operational constraint: every decision must carry a tenant and a cost record. A cheap, high-recall first pass may be appropriate for a large tenant with a review team; a small tenant may need fewer escalations and a stricter intake contract. The correct threshold is therefore a policy decision with an operational budget, not a magic number copied from a dashboard.

## The incident lesson is a routing lesson

I have been paged by missed jobs and duplicate deliveries. That history changes how I read moderation alerts: a queue that cannot explain why a ticket was held is an incident waiting for a second shift. The bounded scenario here is a support-triage worker receiving user-generated ticket text, classifying it, and attaching the result to a tenant’s case. It is not an autonomous enforcement system.

The invariant is simple: do not let one uncertain score become an irreversible action. Store the original text reference, policy version, classifier output, threshold band, and delivery id. Make the review enqueue idempotent. If the worker retries after a timeout, the ticket should still have one moderation decision and one billable classification event.

The tempting alternative is a single cutoff. Below 0.50, allow; above it, block. That looks clean until a ticket says, “The customer wrote ‘kill the process,’” or includes a quoted slur while asking for help removing it. A score measures a model’s interpretation of the input. It does not determine the business meaning of the input, the speaker, or the requested action.

A three-way policy is easier to operate. Use a low band for automatic allowance, a middle band for human or specialist review, and a high band for blocking only when the policy permits it. Keep the bands tenant-configurable, but version them and record the effective configuration with every result.

Here is the smallest useful routing boundary. The numbers are an example policy, not a universal calibration; teams must set them from labeled tickets and inspect the resulting error costs.

```go
type Decision string

const (
	Allow Decision = "allow"
	Review Decision = "review"
	Block Decision = "block"
)

type Policy struct {
	ReviewFrom float64
	BlockFrom  float64
}

func Route(score float64, p Policy, contextConfirmed bool) Decision {
	if score < p.ReviewFrom {
		return Allow
	}
	if score < p.BlockFrom || !contextConfirmed {
		return Review
	}
	return Block
}
```

The `contextConfirmed` check is deliberate. A model can flag a phrase while the ticket’s purpose is reporting abuse, debugging a filter, or quoting a log line. If the surrounding evidence is unavailable, uncertainty belongs in review. This advice does not apply to a narrowly defined, legally required or safety-critical blocklist where the rule itself, rather than a probabilistic score, is the enforcement authority. Even there, preserve an appeal and audit path.

## Why do LLM moderation false positives happen at policy boundaries?

False positives happen because moderation combines several different questions: what words appear, what the author appears to intend, who may be affected, and what the application is allowed to do. A classifier can be correct about a phrase and still be wrong about the action. Language, reclaimed terms, quotation, translation, sarcasm, and missing conversation history all widen that gap.

The US and EU context also changes the control surface. A platform’s terms, notice-and-action process, privacy obligations, and records policy cannot be reduced to a single model threshold. The Digital Services Act describes processes around illegal content and user redress; the GDPR requires purpose limitation and data minimization principles. Those sources do not provide a universal classifier cutoff. They are reasons to define purpose, access, retention, and appeal behavior before tuning one.

For support tickets, the useful evidence is often compact: the flagged span, the preceding customer statement, the requested operation, and whether the submitter is reporting someone else’s content. Do not send an entire account history to every model call just because more context feels safer. More context can add personal data, increase token cost, and introduce unrelated phrases that raise the score.

I keep the explanation attached to the decision rather than asking a reviewer to reconstruct it from logs. A reviewer should see “quoted phrase in abuse report” and the policy version, not just `score=0.81`. That makes an appeal actionable and lets an SRE distinguish a bad label from a stuck queue.

## What should the review queue measure besides volume?

Queue volume is a poor success metric by itself. Measure false-positive rate on reviewed tickets, false-negative discoveries, median and tail review age, repeat submissions, and the fraction of decisions missing tenant attribution. Track those by tenant and policy version. Aggregate numbers hide the exact customer segment that is paying for unnecessary review.

Per-tenant cost visibility needs a ledger-shaped event, even if the first implementation is a table. Record tenant id, ticket id, classifier stage, model or rule identifier, input token estimate, result, reviewer outcome, and an idempotency key. Keep sensitive text out of the cost ledger when a stable content reference is enough.

A simple event can look like this:

```go
type ModerationEvent struct {
	TenantID       string
	TicketID       string
	PolicyVersion  string
	Stage          string
	Decision       Decision
	InputUnits     int
	IdempotencyKey string
}

func EventKey(tenantID, ticketID, policyVersion string) string {
	return tenantID + "/" + ticketID + "/" + policyVersion
}
```

This identifier is not a billing system by itself. It is a stable join point for usage, queue work, and the final ticket state. A retry must upsert the same event or be rejected as a duplicate. If policy changes while a ticket waits, do not silently rewrite the old result; run a new version and retain both decisions.

Keep the boundary boring.

For a queue with three retry attempts, I would treat the attempt count as a policy example, record it with the event, and send a persistent validation error to a dead-letter path; the exact limit depends on dependency behavior and the tenant’s service objective, but the operator needs one explicit rule instead of an invisible loop.

The queue should also have a dead-letter path, a visible retry count, and an operator action for a permanently malformed ticket. Automatic retries are good for transient dependencies. They are bad for a payload that will fail validation every time. That distinction is where a neat moderation diagram usually meets production.

## How should teams tune bands without creating a new failure mode?

Start with a labeled sample that reflects actual support traffic, including quoted abuse reports and multilingual tickets. Split results by tenant, language, content type, and policy category. Then choose thresholds by the cost of each mistake: an unnecessary review consumes reviewer time and delays a customer; a missed severe violation may create a safety or legal escalation. Those costs are not interchangeable, so one global accuracy number is insufficient.

Run a shadow period before changing enforcement. Produce the proposed decision beside the current action, sample the disagreements, and have reviewers label the reason for each disagreement. A threshold change should have an owner, an effective timestamp, a rollback value, and a reason. Keep the deployment boring.

There is a practical stopping rule: if a tenant cannot inspect its own allowance, review, block, and usage counts, do not add another model stage. Better instrumentation often resolves the operational question faster than a more sophisticated classifier. The extra stage may improve recall while making per-tenant attribution and reviewer latency harder to explain.

A runbook entry for an unexpected spike should answer four questions: did the policy version change, did the input mix change, did the queue retry, and did tenant attribution fail? Check those in that order. It prevents a team from tuning a threshold to compensate for duplicate delivery or a broken join.

The conclusion is deliberately modest: use model scores as evidence, policy bands as a control, and review as a first-class state. Put tenant identity and usage accounting on the same path as the decision. That combination makes false positives visible, reversible, and discussable without pretending that language classification can settle context on its own.

## Sources

References:

- https://www.nist.gov/itl/ai-risk-management-framework
- https://eur-lex.europa.eu/eli/reg/2016/679/oj
- https://eur-lex.europa.eu/eli/reg/2022/2065/oj
- https://docs.cohere.com/docs/rerank-overview
- https://github.com/BerriAI/litellm
