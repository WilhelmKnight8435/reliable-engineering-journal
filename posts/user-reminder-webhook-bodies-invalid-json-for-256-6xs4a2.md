# User Reminder Webhook Bodies: Invalid JSON for 256 KB Queue Payloads

Short answer: reject invalid JSON and oversized webhook bodies before enqueueing user reminders, validate a versioned payload contract, and put large reminder content behind a durable reference rather than inside a queue message.

For a queue with a 256 KB message limit, the useful fix is not a larger parser. It is a boundary: bound the incoming read, decode one JSON value, validate the fields, build the final queue envelope, and measure its UTF-8 bytes before publishing. Invalid input is a terminal rejection; a later delivery timeout can be retried with the same idempotency identity.

## What should a user reminder webhook, queue payload, 256 KB limit, invalid JSON, and Node.js schema validation do?

Treat these as separate states. Invalid JSON means the byte stream cannot be decoded as one JSON document. A schema failure means it decoded, but required fields or their types do not meet the contract. A size failure means a valid document cannot travel through the next boundary. Retrying any of those three states just consumes capacity and obscures a producer defect.

The queue limit applies to bytes, not JavaScript characters. `string.length` is not a UTF-8 byte count, and a queue envelope can be larger than the incoming webhook body once it gains a schedule timestamp, trace identifier, idempotency key, and routing data. Set an internal ceiling below 256 KB, then test the exact serialized object sent to the queue. The margin is a local policy: it depends on which bytes and attributes the chosen broker accounts for.

The failure often appears at the wrong boundary. An ingress handler may accept a body below its configured HTTP maximum, construct a friendly-looking object, and report success; the queue publisher later serializes a larger object, adds message attributes, and rejects it. If the publisher hides that result behind a generic asynchronous error, the next on-call engineer starts with a webhook that looked valid and a reminder that never arrived. Make every boundary report its own decision: ingress bytes, decoded schema version, serialized envelope bytes, publication result, and delivery receipt. Those records should use the same correlation ID but must not include the raw reminder text. They make it possible to distinguish a body that was too large on arrival from one that grew too large in transit, and they turn a vague "malformed payload" alert into a repairable producer contract. Byte tests need multibyte fixtures as well; a body containing emoji or non-Latin text can meet a character-based test while exceeding its transport budget after UTF-8 encoding. Don't use a character count as an approximation.

Keep the failure record compact. A reason code such as `json_syntax`, `schema_invalid`, or `payload_too_large`, a correlation ID, a producer identifier, and the measured byte count are enough to operate the path. Do not log a user-authored reminder body or authorization header just to diagnose a malformed request. A content digest is usually more useful for correlation and much safer to retain.

This distinction also improves paging. One rejection is feedback for the caller. A sharp change in rejection rate after a deployment is a production signal. They should not share the same alert rule.

| Condition | Boundary action | Retry |
| --- | --- | --- |
| JSON cannot decode to one document | Reject as `json_syntax` | No |
| Decoded value violates the versioned contract | Reject as `schema_invalid` | No |
| Final serialized envelope exceeds the internal ceiling | Store content separately and enqueue its reference | No |
| A valid delivery attempt has a transient downstream condition | Preserve the idempotency key and back off | Yes |

## Put a narrow contract in front of the queue

The scheduling message should carry identity and timing, not the full user document. A workable envelope contains a schema version, reminder ID, user ID, idempotency key, due time, and a reference to immutable content storage. The worker resolves the reference after it claims the delivery identity. This preserves a small, inspectable queue message even when a reminder contains long text or attachments.

The sequence matters — especially during a producer rollout. First limit the body read. Then parse exactly one JSON value. Then reject unknown or missing fields according to the compatibility policy. Only after those checks should the producer create and serialize the outbound envelope. If independent producers and consumers must evolve separately, accept a documented range of versions before a producer writes a new version. If drift is riskier than delayed features, reject unknown fields and deploy readers before writers.

Here is a focused Go boundary that a Node.js service can mirror with its raw-body limit, `JSON.parse`, and schema validator. It demonstrates the contract rather than a broker SDK. The 240 KB value is an application ceiling that leaves room below the stated 256 KB queue limit; it is not a universal broker setting.

```go
package reminder

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const maxEnvelopeBytes int64 = 240 * 1024

type Envelope struct {
	Version        int       `json:"version"`
	ReminderID     string    `json:"reminder_id"`
	UserID         string    `json:"user_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	DueAt          time.Time `json:"due_at"`
	ContentRef     string    `json:"content_ref"`
}

func DecodeEnvelope(body io.Reader) (Envelope, error) {
	raw, err := io.ReadAll(io.LimitReader(body, maxEnvelopeBytes+1))
	if err != nil {
		return Envelope{}, fmt.Errorf("read envelope: %w", err)
	}
	if int64(len(raw)) > maxEnvelopeBytes {
		return Envelope{}, errors.New("payload_too_large")
	}

	var msg Envelope
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&msg); err != nil {
		return Envelope{}, fmt.Errorf("json_syntax: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return Envelope{}, errors.New("json_syntax: trailing data")
	}
	if msg.Version != 1 || msg.ReminderID == "" || msg.UserID == "" ||
		msg.IdempotencyKey == "" || msg.ContentRef == "" || msg.DueAt.IsZero() {
		return Envelope{}, errors.New("schema_invalid")
	}
	return msg, nil
}
```

The same producer must measure the queue form, not merely this input form. In Node.js, `Buffer.byteLength(JSON.stringify(envelope), "utf8")` measures the serialized envelope before publication. Configure the HTTP framework's raw-body limit before its JSON middleware so an excessive body does not first become an allocated application object. A validator such as JSON Schema can make the versioned contract executable, but a schema does not replace the byte measurement.

## Make delivery idempotent before adding retries

Queue delivery is commonly at-least-once. A consumer can complete the external webhook call and lose its acknowledgement, then receive the same reminder again. The durable invariant is one logical reminder delivery per idempotency key, enforced in storage close to the side effect. A transport retry should carry the same key; a materially changed reminder should receive a new one.

Retries belong only after a valid envelope is accepted. Use capped exponential backoff with jitter for a transient delivery condition, preserve the key and attempt count, and stop at a documented terminal policy. JSON syntax, schema, and size failures should go to a rejection or quarantine workflow with their reason code. They do not become correct because time passes.

The catch is that content references add a storage read and retention work. Keeping a payload inline can be reasonable when the domain guarantees a much smaller upper bound and the broker's accounting is understood. A reference-based design is not suitable when the consumer cannot safely retrieve the referenced object or when the content must be atomically visible with the message but the system offers no transaction or outbox pattern. In that case, narrow the payload further or choose a transport with the required transactional semantics; do not raise a size limit blindly.

## Verify the repair and prepare the rollback

Build fixtures from the boundary, not from hand-written happy paths: empty input, truncated JSON, two concatenated documents, an unknown field, a missing version, multibyte text near the ceiling, an envelope exactly at the application ceiling, and one byte over it. Test the function that actually serializes the queue message. Also submit the same valid delivery twice and prove that only one side effect is committed.

Deploy the reader before the writer. A consumer that understands both the old and new contract can drain the existing queue while a small producer cohort begins emitting the new envelope. Watch rejection rates by reason, encoded byte percentiles, age of the oldest ready message, delivery attempts, idempotency conflicts, and end-to-end reminder lateness. Queue depth alone cannot prove a reminder was delivered.

Rollback should stop new-version publishing, not erase accepted messages. Keep the previous reader compatible until the oldest queued message and its complete retry window have expired. If the new version must be withdrawn, route new input back to the prior serializer while leaving the idempotency record in place. This is why version and identity are part of the payload: an incident can be reversed without creating duplicate reminders.

Do the small test first.

## Further reading

- https://vercel.com/docs/cron-jobs
- https://en.wikipedia.org/wiki/Exponential_backoff
