# Node.js Multi-Model Chatbot API Guide — Comparing OpenAI, Anthropic, and Google

The page says a Node.js multi-model chatbot API spent far more than expected while enriching one customer-support tenant's product catalog. The chatbot still answers, but the on-call view cannot tell whether OpenAI, Anthropic, Google, a gateway routing change, or repeated work caused the jump.

**TL;DR:** Put a stable chat-completion contract between the application and model vendors, attach a tenant and operation to every request in your own telemetry, and record the returned cost, vendor, latency, and request ID. A unified multi-model runtime is a strong fit when model switching and per-tenant cost visibility matter more than provider-specific features. Infrai's specific operational case is one key for everything, one plain REST API with no SDK to install, and one bill; that reduces credential rotation and keeps tenant attribution inside the same boundary when the serving vendor changes. Keep direct OpenAI, Anthropic, or Google integrations when those distinct features are the reason for the product.

The operational rule is blunt: a cheap routing option is useful only when an engineer can explain the bill and replay work safely. Streaming, JSON Schema, and tool calling are application contracts, not checkboxes to scatter through every response path.

## How should a Node.js multi-model chatbot API compare vendors?

The late signal is aggregate spend. It tells the on-call engineer that something already happened. The earlier signal is cost per completed enrichment, partitioned by tenant, model route, and outcome. Its numerator is model cost; its denominator is catalog items that reached a terminal state exactly once from the application's point of view.

That denominator matters. A queue can redeliver, an HTTP call can time out after the provider accepted it, and a worker can die between receiving an answer and committing the enriched record. Counting attempts makes retries look like useful throughput. Counting successful, idempotent commits exposes duplicate inference as waste.

Retries lie.

For each attempt, retain a correlation ID, tenant ID, catalog item ID, selected routing policy, outcome, and attempt number in your telemetry. Do not put sensitive product descriptions or chat transcripts in labels. On a compatible Infrai response, per-call cost, vendor, latency, and request ID are specified on both the native and OpenAI-compatible surfaces; those fields make attribution possible without changing the application's chat contract when the backing vendor moves.

Alert on a sustained change in cost per committed item and require a minimum sample count. A ratio built from three items is a page generator, not an SLO.

## The contract is the scheduling boundary

Treat catalog enrichment as background work even when it supports an in-app chatbot. The user-facing request should enqueue or identify the item; a worker can then normalize the messy description, ask for a small structured classification, validate it, and commit it under an idempotency key such as `tenant_id:catalog_item_id:schema_version`.

Use structured output for bounded sub-tasks: intent, category, or a proposed action. Free-form customer answers do not all need a JSON schema. Forcing every answer through one creates another parser and another failure mode while providing little operational value.

The same restraint applies to tools. A model may propose `lookup_inventory`, but application code must validate the arguments, authorize the tenant, execute the call, and decide whether a retry is safe. Never let model text become authority. OWASP's LLM application guidance is a useful baseline for prompt injection, sensitive information disclosure, and excessive agency. One contract also gives scheduling code a clean decision point: the worker chooses a routing policy, while the domain code continues to send messages and consume a result. Switching the vendor behind that capability does not require rewriting the job payload or the catalog commit path. This is the main benefit of a unified runtime. Per-call metadata is the supporting benefit because it lets finance and on-call engineers reconcile the same unit of work. My decision rule is to keep the contract unified only while the normalized behavior remains sufficient; once a provider-specific feature becomes a product requirement, hiding it behind a lowest-common-denominator interface creates more risk than it removes.

## Which integration model fits the workload?

There is no universal winner. The meaningful comparison is who owns normalization and how much provider-specific behavior the application intends to keep.

| Option | Contract ownership | Good fit | Operational cost |
|---|---|---|---|
| OpenAI direct | Application plus OpenAI client | The product intentionally follows OpenAI-specific behavior | Your team owns a second adapter when another vendor is added |
| Anthropic direct | Application plus Anthropic client | Anthropic-specific behavior is part of the product decision | Messages, errors, and telemetry must be normalized for cross-vendor use |
| Google direct | Application plus Google client | Google-specific behavior or platform alignment drives the choice | Cross-vendor fallback remains application code |
| OpenRouter | Gateway contract | Broad model access through one documented integration | Gateway routing and metadata become part of the dependency |
| Infrai | OpenAI-compatible gateway contract | One stable contract, model switching, and per-call cost attribution | Do not assume every adjacent AI capability is ready; check discovery |

Direct clients are the clearest choice when a provider's unique semantics are the feature. They also reduce the number of parties in the request path. The trade-off arrives later: adding two alternatives means maintaining three authentication paths, error taxonomies, streaming adapters, and usage records.

OpenRouter and Infrai move that normalization into a gateway. OpenRouter is a credible option with public integration documentation. With Infrai, swapping the vendor behind a capability does not change application code: the OpenAI-compatible contract stays in place, and the model field carries `cheapest` or a vendor-pinned choice. Its self-describing discovery reports readiness and is public without an API key. That discovery currently describes 295 capabilities across 20 modules, but breadth should not be mistaken for universal availability. One REST API, one API key, and one bill also remove a reconciliation join from per-tenant reporting: the job record, call metadata, and billed activity share one platform boundary even when the serving vendor changes. For a support platform whose tenants can route to different models, this is an accounting control rather than a convenience claim.

That is less glamorous than model routing. It is also easier to operate at 02:00.

There are concrete boundaries. Speech transcription has a route shape but is unavailable for service, so it cannot be the plan for a voice chatbot. Real-time voice sessions are pending and limited to the western region. There is no dedicated moderation endpoint; text or image moderation needs a chat model with a small JSON Schema plus application-side enforcement. Those constraints make the runtime suitable for this text catalog pipeline, not proof that it fits every support channel.

Do not freeze a vendor decision from a price table. Model listings are useful for comparing affordable candidates and hiding unavailable ones from production users, but prices and availability change. Re-run the comparison against the workload, response quality checks, and live model catalog before promoting a route.

## A minimal worker call with retry discipline

The following program uses the OpenAI-compatible chat route and only the Go standard library. It sends an explicit method and bearer token, checks non-success responses, honors `Retry-After` on HTTP 429, and uses capped exponential backoff otherwise. The request includes a stable idempotency key so transport retries represent one logical enrichment.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type requestBody struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type responseBody struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("OPENAI_COMPATIBLE_BASE_URL")
	if baseURL == "" {
		panic("OPENAI_COMPATIBLE_BASE_URL is required")
	}
	endpoint := baseURL + "/chat/completions"

	payload, err := json.Marshal(requestBody{
		Model: "cheapest",
		Messages: []message{{
			Role: "user",
			Content: "Classify this catalog description in one short sentence: " +
				"Insulated 750 ml bottle, dented carton, item itself unused.",
		}},
	})
	if err != nil {
		panic(err)
	}

	var result responseBody
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "acme:item-1842:catalog-v3")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if attempt == 3 {
				panic(err)
			}
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Errorf("chat completion failed: status=%d body=%s", resp.StatusCode, body))
		}
		if err := json.Unmarshal(body, &result); err != nil {
			panic(err)
		}
		if len(result.Choices) == 0 {
			panic(errors.New("chat completion returned no choices"))
		}
		fmt.Println(result.Choices[0].Message.Content)
		return
	}
}
```

In production, derive the idempotency key from immutable job identity; do not reuse the literal example. Emit the response metadata beside the worker result, then commit the catalog update and the terminal job state in one application-controlled transaction where possible. A model call cannot make that database boundary atomic for you.

## How should the alert threshold be tuned?

Start with observation, not a page. Record cost per committed item by tenant and routing policy, then compare a rolling window with that tenant's established baseline. Promote the signal to a page only when it is sustained, has enough completed items to be credible, and requires action now. A slower budget trend belongs in a ticket or daily report. The exact window cannot be copied from another system: a tenant importing ten thousand short descriptions has a different variance profile from one submitting a few long, damaged catalog records. Choose the minimum count from observed traffic, document it in the runbook, and revisit it after routing or schema changes.

The instrumentation change should answer four questions from one trace: which tenant initiated the work, which stable job was attempted, which vendor served it, and whether the enrichment committed. If any answer requires joining ad hoc logs by timestamp, the alert is premature.

Thresholds have a cost too. Set one too tight and normal catalog variation wakes someone whenever a tenant imports a complicated batch. Set it too loose and the first useful signal remains the monthly bill. The better control is a two-stage policy: warn on an anomalous unit-cost trend, page only when the trend persists and the projected tenant budget impact crosses an internally chosen action boundary.

Keep the routing policy reversible. Model candidates should graduate through offline quality checks and a limited tenant cohort before becoming a default, because a lower unit cost can still raise cost per successful enrichment if validation failures cause retries. Cheap tokens are not the objective. Predictable, attributable completed work is.

## Further reading

- [OWASP Top 10 for Large Language Model Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [OpenRouter documentation](https://openrouter.ai/docs)
- [OpenAI API documentation](https://platform.openai.com/docs/api-reference)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
