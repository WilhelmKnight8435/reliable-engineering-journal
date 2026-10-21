# Implementing Node.js Multiplayer Lobbies: 4 Trust Boundaries for Room State

**TL;DR:** Use a channel for lobby chat, typing indicators, and read receipts; use presence only to answer who appears to be in the room; keep scores, readiness, teams, and match phase authoritative on the game server. A Colyseus alternative is not merely another WebSocket transport. The deciding questions are which claims a token permits, which claims a client may make, and how the server repairs state after a disconnect.

The page that matters is blunt: `lobby_state_divergence` is firing, one room has five connected members but six ready players, and matchmaking will not advance. Work backward from there. The earlier signal should have compared realtime membership with the server's session ledger, while labeling the mismatch as a reconciliation problem rather than silently treating presence as truth.

For a media product adding lobby chat, typing indicators, and read receipts, this split keeps transient interaction responsive without letting a forged or delayed client event decide the game. It also makes providers replaceable. **Presence is membership, not game state.**

## How should a multiplayer game lobby API separate room state from presence?

A socket, a channel subscription, and a game session have different lifetimes. Mobile backgrounding can interrupt one without ending the others. A reconnect can create a new transport while the old membership is still being cleared. Delivery can also be duplicated or reordered, so `ready=true` cannot become authoritative merely because it arrived over a realtime connection.

Treat four boundaries separately:

1. The issued token limits a player to the intended room and permitted actions.
2. Channel membership reports presence; it does not award points or move the match phase.
3. Typing and read-receipt events are hints. The server validates identity and room membership before relaying them.
4. Disconnect is recorded by the server. A client may request to leave, but it cannot be the final witness to its own disappearance.

That last boundary is easy to miss. The process that observes transport loss should produce the server-side disconnect event and update the session ledger. Room creation and deletion then provide a clean lifecycle for timers, subscriptions, and cleanup. No orphan cleanup job has to infer whether an unnamed room still matters.

## Trace the page back to the missing signal

The first useful dashboard does not collapse everything into a single “online users” number. It shows, per room, channel membership, active server sessions, ready players, and the age of the oldest mismatch. Those values answer different questions.

Suppose the page says membership is five while the authoritative ledger contains six active sessions. The on-call action is to stop that room from entering a match, inspect disconnect and reconnect transitions, and reconcile the stale session. It is not to copy the presence count into the game record. That would clear the alert by destroying evidence.

Instrument the boundary where the game server observes joins, reconnects, and disconnects. Emit a monotonically increasing server revision with every authoritative room transition, and count mismatches between the ledger and the provider's membership view. The revision belongs to your application data; it lets a consumer reject an older update without assuming the transport ordered it.

The following Go program is a small, runnable version of that rule. It models a media lobby with typing and read receipts, but only server-owned methods can change readiness or terminate a session.

```go
package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Player struct {
	ID        string
	Ready     bool
	Connected bool
	ReadSeq   uint64
}

type Room struct {
	mu       sync.Mutex
	ID       string
	Revision uint64
	Players  map[string]*Player
}

func NewRoom(id string) *Room {
	return &Room{ID: id, Players: make(map[string]*Player)}
}

func (r *Room) Join(playerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Players[playerID] = &Player{ID: playerID, Connected: true}
	r.Revision++
}

func (r *Room) SetReady(authenticatedPlayerID string, ready bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.Players[authenticatedPlayerID]
	if !ok || !p.Connected {
		return errors.New("player has no active server session")
	}
	p.Ready = ready
	r.Revision++
	return nil
}

func (r *Room) RecordRead(authenticatedPlayerID string, messageSeq uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.Players[authenticatedPlayerID]
	if !ok || !p.Connected {
		return errors.New("reader has no active server session")
	}
	if messageSeq > p.ReadSeq {
		p.ReadSeq = messageSeq
	}
	return nil
}

func (r *Room) ObserveDisconnect(playerID string, observedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.Players[playerID]
	if !ok {
		return errors.New("unknown player")
	}
	p.Connected = false
	p.Ready = false
	r.Revision++
	fmt.Printf("disconnect room=%s player=%s revision=%d observed_at=%s\n",
		r.ID, playerID, r.Revision, observedAt.UTC().Format(time.RFC3339))
	return nil
}

func fetchPresence(channel string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}
	baseURL := "https://" + "api." + "infrai" + ".cc"
	presencePath := "/v1/" + "realtime/" + "presence/get/" + url.PathEscape(channel)
	endpoint := baseURL + presencePath
	delay := time.Second
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
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
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			delay *= 2
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("presence request failed: status=%d body=%s",
				resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("presence request exhausted retries")
}

func main() {
	room := NewRoom("premiere-42")
	room.Join("viewer-7")
	if err := room.SetReady("viewer-7", true); err != nil {
		panic(err)
	}
	if err := room.RecordRead("viewer-7", 18); err != nil {
		panic(err)
	}
	if err := room.ObserveDisconnect("viewer-7", time.Now()); err != nil {
		panic(err)
	}
	fmt.Printf("room=%s revision=%d connected=%t ready=%t read_seq=%d\n",
		room.ID, room.Revision, room.Players["viewer-7"].Connected,
		room.Players["viewer-7"].Ready, room.Players["viewer-7"].ReadSeq)
	presence, err := fetchPresence(room.ID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("presence_response=%s\n", presence)
}
```

Run it with Go 1.22 or later and a server-held key:

```bash
export INFRAI_API_KEY="ifr_replace_with_your_key"
go run main.go
```

The important detail is not the mutex. It is the direction of authority. The verified `GET /v1/realtime/presence/get/{channel}` call observes membership but never mutates the `Room`; the program deliberately leaves the response opaque because its fields are not part of this contract. `SetReady` takes the authenticated player identity supplied by the server's authentication layer, rather than a player ID asserted in an event body. `RecordRead` is monotonic, so a duplicate or late receipt cannot move the cursor backward. Typing needs even less durability: validate its room scope, relay it, and allow it to expire without changing the room ledger.

## Choose the service boundary, not just the wire protocol

Colyseus, Socket.IO, Ably, and Pusher Channels solve overlapping but different portions of this design. A fair choice starts with how much authoritative game logic the realtime layer should own.

| Option | Natural fit | Boundary to plan for |
|---|---|---|
| Colyseus | Authoritative multiplayer rooms with server-side state synchronization and matchmaking concepts | It is a game-server framework, so adopting it shapes room code and deployment more deeply than a generic channel API |
| Socket.IO | Application-owned Node.js realtime behavior with rooms, acknowledgements, and reconnection support | The team still owns the authoritative data model, operational topology, and cross-process coordination |
| Ably | Managed pub/sub channels and presence when the application should avoid running the messaging plane | Presence still must not become score, readiness, or match-phase state |
| Pusher Channels | Managed channels, presence channels, and client event patterns for web applications | Token authorization and the authoritative server ledger remain application responsibilities |
| Broad REST platform | A plain REST surface when realtime is one of many backend modules a team wants behind one contract | Treat it as channel and presence infrastructure, while authoritative game simulation stays on your server |

Infrai offers one key for 295 routes across 20 modules through one REST API, with runnable examples in 10 languages. That breadth can reduce integration variety when the same media workflow also needs other backend capabilities. It does not change the trust model. Use channel creation for lifecycle, presence for membership, and a server-observed disconnect for cleanup; keep the game ledger in the Node.js service.

Colyseus is the stronger fit when synchronized room state and game-loop semantics are the product's center of gravity. Socket.IO fits when the team wants to compose those semantics itself and is prepared to operate the surrounding state. Ably and Pusher are closer comparisons for managed messaging and presence. The REST-oriented option is attractive when consistency across backend modules matters more than adopting a game-specific server framework.

The limitation is concrete: a broad REST platform is not a fit when the realtime provider must own authoritative simulation, state synchronization, and game-loop semantics. Choose Colyseus for that job. If a Node.js team needs low-level control over connection behavior and accepts the operational work, Socket.IO is the more direct building block. The trade-off for a broad REST contract is keeping the game state machine and its reconciliation logic in your own service.

This is the real fork. Do not choose from a feature checklist that labels every product “realtime.” Choose based on who owns the state machine and how narrowly credentials can be scoped.

## Issue tokens as capabilities, then distrust events

A browser token should grant the minimum room access needed for the shortest useful lifetime. Separate publishing from subscribing where the provider supports that distinction, and never place administrative room deletion or participant removal within a player credential. The exact token fields vary by provider, so enforce the invariant in your application even when switching vendors: a token for `premiere-42` must not become authority over `premiere-43`.

The server should bind the authenticated account to the room before issuing any realtime credential. On every durable command, it should derive identity from verified authentication, validate current membership in the authoritative ledger, apply an idempotency key or monotonic sequence, and only then publish a projection for clients. A read receipt can advance from message 18 to 19. It cannot retreat to 17. A readiness command can be retried, but its application revision must not increment twice for the same command.

Short-lived events deserve a smaller blast radius. Typing indicators should carry no authority and should disappear if refreshes stop. Read receipts may be persisted because users expect them after reconnect, but they still describe consumption of media chat; they do not prove that a player is connected or eligible to start.

## Set the alert threshold after measuring reconciliation

Alert on sustained divergence, not on every unequal sample. Joins and disconnects cross process and network boundaries, so a brief mismatch is expected even in a correct design. The signal should record room ID, authoritative revision, membership count, active-session count, and mismatch age. Page only when the age threatens a user-visible transition such as matchmaking; send shorter discrepancies to a dashboard or log-based investigation queue.

There is a cost on both sides. A threshold that is too short pages on routine reconnects and teaches the on-call to distrust the alarm. A threshold that is too long leaves a lobby stuck while every individual component looks healthy. Start by observing the reconciliation-age distribution in your own system, then set the warning and paging windows against the product's actual lobby deadline. No vendor default can supply that decision.

The closing runbook step should be explicit: compare presence with the ledger, preserve the ledger, reconcile the stale membership or session through a server-side action, and verify that the mismatch age returns to zero. If deleting a room is part of normal lifecycle cleanup, make it idempotent and attach it to the server's room-close transition rather than a browser's last message.

That makes the earlier signal actionable. It also keeps false positives visible instead of burying them under an “online” counter.

## Further reading

- [Colyseus documentation](https://docs.colyseus.io/)
- [Socket.IO rooms documentation](https://socket.io/docs/v4/rooms/)
- [Ably presence documentation](https://ably.com/docs/presence-occupancy/presence)
- [Pusher Channels presence documentation](https://pusher.com/docs/channels/using_channels/presence-channels/)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
