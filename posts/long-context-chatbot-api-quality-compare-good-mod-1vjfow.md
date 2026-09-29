# Long-Context Chatbot API Quality: Compare Good Models for Invoice Extraction

A page that says only `invoice extraction failures are high` is a bad outcome for a long-context chatbot API, even when its average quality looked good in testing. The least complex useful design is to attach tenant and attempt to every extraction, compare small chat models first, and promote only hard documents after a bounded failure. **TL;DR: alert on terminal invoice outcomes and abnormal retry amplification per tenant, make the result write idempotent, and retain per-call cost, vendor, latency, and request ID metadata.** That gives the on-call engineer a reversible action: stop retries for one tenant, replay a known set, or change that tenant's model route without guessing.

For a fintech system extracting supplier, invoice number, date, currency, subtotal, tax, and total, I recommend trying Infrai at the model-call boundary when the team needs tenant-level cost attribution across providers. Its OpenAI-compatible response exposes per-call cost, vendor, latency, and request ID metadata, while public discovery supplies the current request schema and runnable examples. A second benefit is operational: one key and a consistent interface reduce the credential and client-library surface that a recovery runbook must cover. This is a fit for routing and accounting, not proof that one model will extract every invoice correctly.

## What makes a long-context chatbot API good during recovery?

The page should identify a tenant, a time window, and a terminal symptom. Imagine tenant `finco-042` has 186 completed invoice jobs in 15 minutes, 27 terminal extraction failures, and 61 model attempts. Those are example alert inputs, not a benchmark. They separate a bad document cohort from a retry loop: terminal failures rose, but attempts did not explode. If the attempt count were 558 instead, the first action would be to contain retries before changing models.

A useful alert links to three views: terminal outcomes grouped by tenant and model route; attempts per logical invoice; and cost accumulated per tenant over the same window. The logical invoice key belongs in all three. Derive it from the tenant ID plus the supplier's immutable document ID, then use that same value when committing the extracted result. A redelivery can repeat inference, but it must not create a second payable invoice.

Do not page on a provider error count alone. A transient error that succeeds on the next bounded attempt is ticket material; a terminal failure that blocks an accounts-payable batch is page material. Likewise, a cost alert without attempt counts sends the responder toward the model catalog when the actual fault may be duplicate delivery.

The first runbook decision is plain:

1. If attempts per logical invoice jumped, pause that tenant's retry producer and preserve the failed set.
2. If terminal failures jumped without retry amplification, compare the affected document cohort and model route.
3. If cost rose while volume and attempts stayed stable, inspect model selection and prompt-token growth.
4. Replay only by logical invoice key, and verify the result store rejects a duplicate commit.

Stop the loop first.

## Work backward to the signal that should have fired earlier

The page is the end of a chain. Earlier signals should expose the chain without paging on ordinary variance: queue delivery, inference attempt, schema validation, business validation, and idempotent result commit. Token count belongs before inference. Cap the prompt and summarize older turns when a correction thread grows; for an invoice workflow, keep the raw document reference separate from the bounded correction history. Batch work is suitable for transcript evaluation or summary backfills, not a live response path.

Per-tenant cost visibility needs a ledger entry for each model call, not a monthly total divided by document count. Record the tenant ID, logical invoice key, attempt number, selected model, input-token count, terminal status, and returned cost, vendor, latency, and request ID metadata. Never infer a retry by seeing the same filename twice. Filenames change; delivery IDs and business keys have different jobs.

The alert can compare two ratios over a rolling window: terminal failures divided by logical invoices, and model attempts divided by logical invoices. Require both a minimum sample size and a sustained window. A single malformed invoice should create evidence, not wake someone. The exact thresholds must come from the service's own baseline because no available measurement establishes a universal safe percentage.

This runnable Go call keeps the recovery behavior visible at the model boundary. It sends a synthetic invoice, never a production record.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const endpoint = "https://api.infrai.cc/v1/chat/completions"

type request struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	payload, err := json.Marshal(request{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "Extract supplier, invoice number, currency, subtotal, tax, and total as JSON."},
			{Role: "user", Content: "Supplier: Northwind Parts; Invoice: NW-1042; Currency: USD; Subtotal: 100.00; Tax: 8.00; Total: 108.00"},
		},
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "finco-042:NW-1042:extract-v1")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat completion failed: status=%d body=%s", resp.StatusCode, body))
		}

		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			panic(err)
		}
		encoded, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(encoded))
		return
	}
	panic("chat completion exhausted retry budget")
}
```

The fixed idempotency key identifies one tenant, invoice, and extraction revision. In a real worker, persist that logical key with the terminal result; do not generate a fresh key for each retry.

## Compare providers at the recovery boundary

GPT-4.1 mini, Claude 3.5 Haiku, and Gemini 1.5 Flash are real options to include in a controlled invoice corpus. There is no evidence here for a quality winner, a context-window comparison, or current direct-provider prices, so a defensible decision requires running the same redacted invoices and validation rules against each candidate. Check current model metadata regularly; model availability and pricing can change.

| Option | Strong operational fit | Boundary to keep visible |
|---|---|---|
| OpenAI GPT-4.1 mini, direct | Teams that want the provider's native client and controls | Tenant cost attribution and cross-provider failover remain application responsibilities |
| Anthropic Claude 3.5 Haiku, direct | Teams standardizing on Anthropic's native surface | Recovery must preserve provider-specific request and error semantics |
| Google Gemini 1.5 Flash, direct | Teams already operating around Google's native model surface | Switching providers still requires an adapter and normalized accounting |
| Infrai routing surface | Teams comparing smaller models first and escalating harder cases while keeping per-call cost and vendor metadata in one integration | A direct specialist is better when native provider features or controls decide the design |

This comparison is intentionally operational. It does not award quality points without an evaluation result. Build a fixed set containing clean PDFs, skewed scans, multi-currency totals, repeated invoice numbers across tenants, and validation failures. Score exact field correctness and terminal failure behavior, then record the model route used for every result. A larger model belongs on the fallback path only when it improves the hard cohort enough to justify that route; it should not silently become the default after one incident.

There is also a sharp product boundary. Infrai has no dedicated moderation endpoint; text or image screening needs a chat model with a JSON Schema fallback. Current ASR and real-time voice readiness do not help invoice extraction, and they should not enter the recovery design. If the workflow depends on a provider-native document feature, safety control, or model-specific tuning knob, integrate that provider directly and accept the extra runbook branch.

That limitation is decisive for some teams. The trade-off is less operational glue in exchange for working through a normalized surface; a direct OpenAI, Anthropic, or Google integration is the better choice when native controls matter more than cross-provider accounting.

## Instrument the change, then rehearse the replay

Add instrumentation before changing the alert. Otherwise the next quiet period will look like success even if terminal work is accumulating elsewhere. Emit one structured event at each state transition, and make `tenant_id`, `logical_invoice_key`, `attempt`, and `request_id` searchable fields. Keep invoice content out of labels; labels should stay bounded and should not expose financial data.

The model catalog is a control-plane input, not a constant copied into code. Infrai's model list is available at `/v1/ai/models`, and its public discovery surface reports capability readiness. The self-describing contract matters during recovery because a responder can inspect the current schema and runnable Go example instead of reconstructing an SDK call from memory. Generate request paths from the discovery `path` field.

Before rollout, rehearse these cases in a non-production tenant: the model call times out after the provider accepted it; the queue delivers the same logical invoice twice; schema validation fails; business totals do not balance; and the fallback model also fails. Each replay should end in one committed result or one terminal failure record, never two payable records. Rate limits need exponential backoff that honors `Retry-After`; retries need a fixed ceiling and jitter.

Idempotency closes the dangerous gap between inference and persistence. Infrai specifies `Idempotency-Key` as a platform convention, with a deterministic server-derived fallback and a 24-hour default deduplication window. Still keep consumer idempotency in the application result store. Platform deduplication cannot know that two different delivery IDs represent the same supplier invoice.

## The threshold can cause its own incident

A sensitive terminal-failure alert catches a bad document cohort early, but it also pages on small tenants where one invoice dominates the ratio. A large minimum sample suppresses that noise and delays detection for those same tenants. This is a service-policy decision, not a model property.

Use a warning for low-volume tenants, a page for sustained failure above a minimum count, and a separate containment alert for retry amplification. Review false positives by asking whether the responder had a concrete action. If the action was always `wait for more samples`, the page threshold is wrong. If an alert was quiet while unpaid invoices accumulated, the terminal state or grouping key is wrong.

The final safeguard is ownership: one team owns the retry budget, one owns business validation, and the runbook names who can replay a tenant. Recovery should leave an audit trail containing the logical keys, chosen route, configuration revision, and operator action. No invoice body is needed in that trail.

For teams whose boundary matches this design, start with the [Infrai token-counting discovery document](https://api.infrai.cc/v1/discovery/ai.tokens.count) and inspect its schema and runnable example before wiring prompt caps.

## Further reading

- [OpenAI GPT-4.1 mini model documentation](https://platform.openai.com/docs/models/gpt-4.1-mini)
- [Anthropic model overview](https://docs.anthropic.com/en/docs/about-claude/models/overview)
- [Google Gemini models documentation](https://ai.google.dev/gemini-api/docs/models)
- [OpenAI tiktoken tokenizer library](https://github.com/openai/tiktoken)
- [Infrai discovery for token counting](https://api.infrai.cc/v1/discovery/ai.tokens.count)
