# Gaming Reservation Expiry: Cheap SaaS Webhook Processing with Queue Rate Limits

For a gaming SaaS, the cheap path for webhook task processing is a durable due time plus a bounded queue whose rate limit protects the receiver. Expire a gaming reservation with that schedule, then let workers deliver the expiry webhook. The decision is latency versus cost: polling more often reduces expiry lag but burns more compute and downstream calls, while a slower sweep is cheaper and can leave held inventory unavailable longer.

Short answer: store the reservation's absolute expiry time, schedule a small number of sweep tasks, and make each expiry operation idempotent before sending a webhook. Use a queue with finite retries and jittered backoff to absorb bursts; use a workflow system only when the reservation process needs durable branches or joins.

This is a runbook decision, not a feature checklist. A missed expiry keeps a seat, item, or match slot locked. A duplicate expiry can produce two customer notifications or race with a payment confirmation. Both pages deserve a design before launch.

## What signal tells an SRE that reservation scheduling is falling behind?

Start with the oldest overdue reservation, not average scheduler latency. For every sweep, measure the age of the oldest due item, the count of due items, the number of expiry transitions, webhook acknowledgments, retries, and terminal failures. A healthy queue can be deep during a short game launch. It is unhealthy when the oldest due item keeps getting older.

The reservation table should hold an immutable reservation ID, the absolute `expires_at` time, the current state, and a version or transition token. The expiry worker claims only rows that are still held and due. It then performs the state transition under the database's normal concurrency control. A late payment confirmation must not silently turn an already expired reservation back into a held one; define that state machine explicitly and test the race.

Keep the webhook outside the transaction that changes inventory. The transaction records the expiry and an outbox entry with a stable delivery ID. A separate publisher reads the outbox and puts the delivery on the queue. This prevents a committed expiry from being lost because an HTTP call happened to fail, and it keeps the database transaction from waiting on a remote subscriber.

Small. Observable. Recoverable.

Do not infer correctness from queue depth alone. Compare due-item age with webhook age. If due items are current but webhook age grows, the scheduler is fine and delivery is the bottleneck. If both grow, inspect sweep cadence, database contention, worker capacity, and the rate limit imposed by the receiving service. That split makes an incident easier to route.

## How should a Node.js SaaS schedule webhook retries under queue rate limits?

The language of the application does not change the contract. Node.js can publish a task, but the task still needs a stable delivery ID, a destination key, an attempt count, and the reservation event reference. The consumer should acknowledge only after the next durable action is complete. A successful webhook is acknowledged after the receiver confirms it. A retryable response is given a future eligibility time, and the current delivery is acknowledged only after that retry handoff is durable.

Respect a receiver's `Retry-After` value when it supplies one. Otherwise, use exponential backoff with jitter and a finite attempt limit. A 4xx response that represents a permanent schema or authorization rejection should go to a terminal path with an audit record. It should not spin forever. A 429 is a pacing signal, not permission to tight-loop.

The worker must make the reservation effect idempotent even when transport delivery is at-least-once. Record the delivery ID at the receiving boundary, or have the receiver treat a repeated ID as the already-recorded outcome. The awkward interval is after the remote side effect and before acknowledgment; a second transport attempt is normal there. The durable ID is what keeps the business effect singular.

Here is the policy decision in Go. It is deliberately an adapter-sized example: the queue client, database transaction, and HTTP client remain behind interfaces owned by the service. The policy is testable without a live queue.

```go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

const maxAttempts = 6

type Delivery struct {
	ID         string `json:"delivery_id"`
	Attempt    int    `json:"attempt"`
	Status     int    `json:"status"`
	RetryAfter string `json:"retry_after"`
}

type Decision struct {
	Action     string `json:"action"`
	DeliveryID string `json:"delivery_id"`
	Attempt    int    `json:"attempt,omitempty"`
	DelaySec   int    `json:"delay_seconds,omitempty"`
}

func main() {
	var d Delivery
	if err := json.NewDecoder(os.Stdin).Decode(&d); err != nil || d.ID == "" {
		fmt.Fprintln(os.Stderr, "delivery_id is required")
		os.Exit(1)
	}

	decision := decide(d)
	if err := json.NewEncoder(os.Stdout).Encode(decision); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func decide(d Delivery) Decision {
	if d.Status != 429 || d.Attempt >= maxAttempts {
		return Decision{Action: "ack", DeliveryID: d.ID}
	}

	delay := 1 << min(d.Attempt+1, 10)
	if seconds, err := strconv.Atoi(d.RetryAfter); err == nil && seconds >= 0 {
		delay = seconds
	}

	return Decision{
		Action:     "requeue",
		DeliveryID: d.ID,
		Attempt:    d.Attempt + 1,
		DelaySec:   delay,
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

The adapter must not treat this snippet as a complete delivery transaction. On `requeue`, publish the next task with the same delivery ID and incremented attempt before acknowledging the current task. On `ack`, persist the terminal result or confirm that the receiver has done so. A process crash between those operations is why the idempotency record and outbox matter.

I've seen enough duplicate-delivery pages to put the acknowledgment order in the runbook twice: ack last. The failure sequence is easy to miss in a code review. A worker receives an expiry task, the receiver commits the notification or state change, and the worker process dies before it acknowledges the message. The queue quite reasonably delivers the task again. If the receiver's durable record is keyed by the same delivery ID, the second call returns the recorded outcome; if a retry generated a new ID, the receiver cannot distinguish recovery from a new business event. That is why the ID belongs to the outbox record and survives every attempt, while the attempt number belongs to delivery policy. The exact retry ceiling depends on the reservation's business value and the downstream contract; I'm not sure six attempts is right for your game, and your mileage may vary. Decide it from the maximum acceptable expiry-webhook age, then test that decision against a burst.

## How do latency, cost, and retry backoff change the architecture?

A fixed hold window gives you a useful budget. If a reservation expires at 12:00:00, define whether the webhook must be emitted by 12:00:05, 12:01:00, or only before the next inventory refresh. That service objective determines sweep frequency, worker concurrency, database indexes, queue delay, and the permitted retry schedule. Without it, “cheap” means only that the failure is being paid for by unavailable inventory.

There are three common designs:

| Design | Latency and cost profile | Failure boundary |
| --- | --- | --- |
| Frequent database sweep | Low expected expiry lag; more reads and scheduler activity | Database contention or a burst of due rows |
| Delayed queue task per reservation | Predictable wake-up; per-task storage and delivery overhead | Duplicate delivery and stale task state |
| Coarse sweep plus bounded queue | Moderate lag; fewer timer entries and controlled downstream pressure | Sweep delay plus queue backlog |

For most reservation expiry paths, the coarse sweep plus bounded queue is the useful default. The sweep finds due work in batches, the database enforces the state transition, and the queue smooths the webhook burst. A per-reservation delayed task is a better fit when the expiry objective is tight and the task volume makes the extra scheduling records acceptable. Frequent polling is not suitable when the primary database is already near its read or lock budget.

Rate limits belong to the destination, not to a global worker sleep. Partition concurrency by destination or quota key, and track the oldest retry for each partition. One slow partner should not consume the entire worker budget. If a provider uses a rolling window that cannot be represented by a simple delay, put the admission decision in a dedicated rate-limit coordinator; scattered sleeps are difficult to inspect during an incident.

The simple queue is not suitable for a process that needs durable fan-out, replay across independent consumer groups, compensation, or joins. Stick with an event log when replay is the actual requirement. Choose workflow orchestration when a reservation spans several durable steps and the state machine is the product behavior. Keep a queue when the job is one bounded side effect with explicit retry semantics.

## How do you verify, deploy, and roll back reservation expiry safely?

Test the state machine before testing the timer. Cover a successful expiry, a late confirmation, a repeated sweep, a duplicate delivery ID, a receiver 429 with `Retry-After`, a permanent rejection, and a worker crash after the receiver's side effect. The expected result is one inventory transition and at most one business effect, even if the transport sees the same task again.

For a staging burst, use a dataset that resembles the real boundary: reservations expiring in the same second, destinations sharing a quota, and retries becoming eligible together. Observe oldest due age, oldest retry age, transition conflicts, webhook status classes, attempt distribution, and terminal-task count. Logs should include the reservation ID, delivery ID, attempt, state transition, and destination quota key. Do not put customer payloads or secrets in those fields.

Deploy the consumer before the producer starts emitting a new envelope version. During rollback, stop or drain consumers while preserving queued tasks, restore the previous publisher or consumer version, and resume only after both versions can safely read the retained envelope. Do not purge the queue to make a dashboard look healthy. If the schema is wrong, quarantine the affected tasks and keep their delivery IDs available for replay after the fix.

The rollback target is the last known-good state transition, not merely the previous binary. If expiry has already been committed, do not reverse it automatically because a webhook failed. Repair delivery from the outbox, or invoke an explicitly reviewed business transition. That distinction prevents a transport incident from corrupting inventory state.

## References

- [Cloudflare Workers Cron Triggers documentation](https://developers.cloudflare.com/workers/configuration/cron-triggers/)
- [Inngest documentation](https://www.inngest.com/docs)
