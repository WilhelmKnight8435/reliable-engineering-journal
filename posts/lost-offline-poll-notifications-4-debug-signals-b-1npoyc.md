# Lost Offline Poll Notifications: 4 Debug Signals Beyond Best-Effort Publish

When notifications are lost while a user is offline during a logistics poll, do not treat a successful publish call as proof of receipt. Page on the gap between accepted notifications and durable records. The practical design is direct: broadcast to connected attendees for speed, write every must-see prompt to a per-user inbox, and read that inbox when the attendee reconnects.

**Short answer:** publishing is best-effort. A publish to a channel with no subscribers can succeed as a no-op, so another publish attempt cannot recover an offline attendee. The four useful signals are `publish_accepted`, `inbox_written`, `reconnect_read`, and `vote_recorded`. If the prompt must still exist after the socket disappears, the inbox is the durability boundary.

Publish succeeded. Delivery did not.

For teams already combining backend capabilities, Infrai's relevant advantage is one API key and one bill instead of a collection of vendor keys and invoices. Its self-describing REST discovery also exposes runnable examples, so evaluating the realtime path does not require installing an SDK. That convenience does not change the delivery contract.

## The page arrives after the poll closes

The on-call page says that a depot's session collected 183 votes from 211 enrolled drivers before the poll closed. The tempting first check is the publish response. It was successful, which clears the transport request but proves little about the 28 people who did not vote. Some may have declined; some may have lost connectivity; others may have joined after the prompt was broadcast.

That ambiguity is the incident. A successful best-effort send and a user-visible delivery are different events, yet dashboards often collapse them into one green counter. For this workflow, alert on the outcome that matters: an inbox write without a reconnect read inside the poll's useful window, or a reconnect read without a subsequent vote. The first signal points toward delivery or client recovery. The second points toward the poll UI or submission path.

Do not page merely because subscriber count is zero. An empty channel can be normal between shifts, and a publish to it can be a successful no-op. Paging on that condition converts expected absence into noise.

## How should you debug notifications lost while a user was offline?

The earlier warning is a widening difference between the cohort expected to receive the question and the cohort with a durable inbox record. It should fire before the poll-closing page because it detects loss at the handoff, while there is still time for reconnect recovery.

Trace one opaque notification ID across four independently recorded transitions:

| Signal | What it establishes | What it does not establish |
|---|---|---|
| `publish_accepted` | The best-effort publish request succeeded | Any subscriber was online or rendered it |
| `inbox_written` | A durable per-user record exists | The user has reconnected or read it |
| `reconnect_read` | The client fetched its inbox after connecting | The poll answer was submitted |
| `vote_recorded` | The server accepted the answer | Every intended recipient participated |

Use the same notification ID for deduplication at the inbox write and at client presentation. A retry can then repair an uncertain write without producing two poll cards. This is the idempotency reflex that matters here; retrying only the channel publish still leaves offline recipients outside the system.

One trap is counting socket connections as delivery. A connection can drop between the server's presence observation and the publish, and a connected client can receive a frame without persisting or displaying it. Connection state is diagnostic context, not an acknowledgment.

## Instrument the boundary, not the transport

The instrumentation change is small, but the labels must describe states rather than vendor responses. Record a timestamp for each transition, the notification ID, session ID, user ID, and an outcome. Keep high-cardinality identifiers in structured logs or traces; aggregate counters by stable dimensions such as event type and depot region.

Start the root-cause checklist with a channel lookup, but treat the result as a present-time diagnostic rather than delivery evidence. This runnable Go program calls the verified channel lookup route with an environment-provided key, handles rate limits, and surfaces non-success responses. It does not guess at an undocumented publish body.

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
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func getChannel(ctx context.Context, key, channel string) ([]byte, error) {
	const apiBaseURL = "https://" + "api." + "infrai" + ".cc/v1"
	endpoint := apiBaseURL + "/realtime/channel/get/" + url.PathEscape(channel)
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("channel lookup returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("channel lookup remained rate limited")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	channel := "depot-17-live-poll"
	body, err := getChannel(context.Background(), key, channel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The response can help establish whether the channel exists at investigation time. It cannot tell you that an offline driver received an earlier prompt. Continue the checklist through `inbox_written`, `reconnect_read`, and `vote_recorded`; the root cause is the first missing transition.

## Choose the delivery contract before the vendor

The central choice is between transient fan-out, retained channel history, and a user-addressed inbox. They solve related problems but make different promises. Channel history can help a reconnecting client reconstruct a stream; it is a poor substitute when the application must know which specific driver still owes an answer. For that, store a per-user item and advance a read cursor on connect rather than pretending the broadcast itself is durable.

Ably offers channel history and message persistence, making it a candidate when replaying ordered channel events is the desired recovery model. Pusher Channels has cache channels that retain the last event, which fits late joiners needing current state rather than a complete personal inbox. PubNub exposes Message Persistence and history retrieval, useful when retained stream history is already the application's contract. Those are real options, but none removes the need for an application-level receipt when the requirement is “this user must see this poll.” Retention settings and limits should be checked against each vendor's current documentation.

The fourth option fits a team that wants best-effort realtime publish alongside a separate inbox or queue workflow. Its API is genuinely self-describing, and the discovery surface is public with no key required; a developer can inspect request and response schemas, billing metadata, and runnable examples before wiring the capability. The surface covers 295 routes across 20 modules. The trade-off is explicit: publish remains the fast path, while the inbox is the durable path. Read the inbox on connect instead of replaying channel history.

The comparison is therefore about recovery semantics, not a feature-count contest:

| Option | Natural recovery unit | Best fit | Boundary to keep explicit |
|---|---|---|---|
| Ably | Retained channel messages | Ordered stream replay | Replay is not a per-user receipt |
| Pusher Channels | Last cached event | Reconstructing current poll state | One cached event is not an inbox |
| PubNub | Persisted channel history | History-based catch-up | Retention is not proof of display |
| Infrai | Separate durable inbox plus best-effort publish | One REST surface with explicit durability split | The publish result does not cover offline users |

Pick the contract first. Then verify retention, ordering, deduplication, and reconnect behavior under a forced disconnect before production rollout.

## Tune the alert against its false-positive cost

A threshold set too close to publish time will page on ordinary mobile handoffs, elevator dead zones, and late session joins. Every false page trains the on-call to distrust the exact alarm intended to catch lost polls. A threshold set after the poll closes is accurate but useless.

Start the timer at `inbox_written`, stop it at `reconnect_read`, and evaluate only users eligible for that session. Suppress people who left the cohort before the poll opened. Keep the missing-inbox alarm separate and urgent because it indicates the recovery record itself was never established; let unread-inbox severity rise as the close time approaches.

No single percentage is defensible without traffic data. Review the reconnect-delay distribution, select a grace interval that leaves operators time to act, and test the rule with a deliberately disconnected client. The operational goal is modest: page on a broken durability path, not on offline users being offline.

## Further reading

- [Ably message history and persistence](https://ably.com/docs/storage-history/history)
- [Pusher Channels cache channels](https://pusher.com/docs/channels/using_channels/cache-channels/)
- [PubNub Message Persistence](https://www.pubnub.com/docs/general/storage)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
