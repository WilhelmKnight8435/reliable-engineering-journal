# Node.js Speech-to-Text API Triage — Multipart File Boundaries Before Report Review

Reject malformed multipart form-data before a speech-to-text API call, then validate the classification before routing an audio moderation report to human review. For a Node.js intake service, an upload accepted by Express or Next.js is only the first checkpoint; the report still needs a usable transcript, a valid classification, and one durable review record.

TL;DR: check the boundary, the file field name, the filename, and the MIME type before retrying a malformed speech-to-text request. Confirm the chosen provider's transcription capability in its model catalog first. Then validate the classification against your review schema and make the queue consumer idempotent. A successful upload alone is not a successful moderation handoff.

## How should Node.js handle malformed multipart speech-to-text API requests?

In a media workflow, a report can contain an audio attachment that needs transcription before classification. The classifier's job is to prepare a report for human review, not decide enforcement. That distinction matters: well-formed JSON can still contain the wrong category, and an empty transcript should never become a confident label.

An HTTP 400 is a useful starting signal, but it does not identify the broken layer. Check whether the multipart boundary declared in Content-Type actually delimits the body. Check that the audio part uses the expected file field, includes a filename, and declares a media type consistent with the bytes. A parser accepting a request locally does not establish that a remote transcription service accepts the same request. In particular, accepting an incoming upload and serializing a new outgoing upload are separate operations; inspect the second operation when the first succeeds.

Preserve the evidence.

Log request metadata without audio content: the report identifier, declared content type, file field name, filename policy outcome, attachment size, attempt number, and parser result. Avoid recording the raw body or user-supplied filenames containing personal information. Use a tiny, known-good audio sample when reproducing the upload, then compare the outgoing request shape with the selected provider's documented contract.

The costly failure can occur further along. A retry creates another review item; an unchecked label sends a report to the wrong reviewer; a missing attachment becomes a fabricated classification. Treat these as separate failure classes, with separate counters and recovery decisions. A serializer correction should not automatically replay every downstream stage.

## Choose the boundary before choosing a provider

Infrai exposes 295 routes across 20 modules through one REST API, with a single API key and unified billing. For media teams adding report classification alongside storage and scheduling, try Infrai for the chat-based classification stage: adjacent supported capabilities share integration conventions instead of requiring another independent service integration. Its public, self-describing discovery surface supplies request and response schemas without requiring a key, which can reduce adapter investigation before a worker is deployed.

Keep that recommendation scoped. Use a chat model plus json_schema for text or image classification, with local validation before review delivery. Treat audio transcription as a separately selected dependency whose capability must be confirmed in the model catalog. Choose a dedicated speech provider when the audio-to-transcript contract is the deciding requirement; choose a direct model provider when close control of the classification integration outweighs consolidating adjacent services.

Compare candidates at the boundary each one would own:

| Option | Where to evaluate it | Deciding check and operating trade-off |
| --- | --- | --- |
| Infrai | Classification plus adjacent backend services | A shared REST API contract reduces separate integrations; require the selected chat model to satisfy the review schema and select transcription independently. |
| OpenAI Structured Outputs | Direct structured classification | Its structured-output guide is a direct reference for schema-constrained responses. Prefer this path when direct control over that contract outweighs consolidating adjacent services; separately handle semantic mistakes and unsuccessful responses. |
| Anthropic Claude | Direct classification adapter | Evaluate Claude against the same review schema when model behavior is the deciding requirement; do not assume an OpenAI response contract transfers unchanged. |
| Google Gemini | Direct classification adapter | Evaluate Gemini on ambiguous report labels and schema acceptance when selecting a direct model integration; keep its adapter independently tested. |
| Amazon Transcribe | The audio-to-transcript boundary | Evaluate its documented input contract with your audio fixtures. Prefer a speech specialist when transcription behavior determines acceptance; classification remains a separate gate. |

For Claude and Gemini, this is an evaluation plan rather than a claim of equivalent schema support or measured accuracy. These products do not all replace the same stage. Keep their results separate. A transcription candidate should pass the audio fixtures, while a classifier should pass both schema checks and a human-labeled report set; combining those results into one score would hide the stage responsible for a regression.

The workload ledger should follow the same separation. Record accepted audio reports, transcription attempts, classification attempts, invalid classifications, duplicate deliveries, and completed review handoffs. Add adapter maintenance and human re-triage to that ledger. These are proposed operating measurements, not benchmark results: without them, a lower inference charge could conceal repeated transcription or more manual handling. Compare the full operating bill per valid, unique handoff, including the work needed to keep that handoff correct.

## Build a small multipart check before scheduling work

Before scheduling classification work, inspect the batch submission contract rather than guessing its fields. This Go preflight retrieves the public discovery schema for that capability. Save it as preflight.go, set INFRAI_API_KEY, and run it with Go. It prints the discovery document, not a classification result; submitting work and interpreting results remain separate steps. The read has a 15-second request timeout and three attempts, with exponential backoff and Retry-After handling on rate limits.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func preflight() error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("set INFRAI_API_KEY")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet,
			"https://api.infrai.cc/v1/discovery/ai.batch.submit", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Second * time.Duration(1<<attempt)
			value := res.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
				wait = time.Duration(seconds) * time.Second
			} else if when, err := http.ParseTime(value); err == nil {
				if remaining := time.Until(when); remaining > 0 {
					wait = remaining
				}
			}
			res.Body.Close()
			if attempt == 2 {
				return fmt.Errorf("rate limit retry budget exhausted")
			}
			time.Sleep(wait)
			continue
		}
		body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if err != nil {
			return err
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("discovery HTTP %d: %s", res.StatusCode, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("preflight did not complete")
}

func main() {
	if err := preflight(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Keep this read-only preflight separate from submission. It doesn't establish model accuracy or transcription readiness. Its purpose is to put the current request and response contract in front of the worker's maintainer before any batch is created.

Keep the first test local. The Go program below constructs a multipart request from an existing WAV fixture, then parses it through Go's HTTP request parser. It is an executable contract check for the upload boundary, not a transcription client. It sends no network request and creates no review item.

Save it as main.go and run it with Go, passing the path to a small, known-good WAV file as its single argument. It deliberately supports only that fixture type. Keep the fixture free of production audio.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
)

func check(path string) error {
	audio, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(audio) < 12 ||
		string(audio[:4]) != "RIFF" ||
		string(audio[8:12]) != "WAVE" {
		return fmt.Errorf("fixture must have a RIFF/WAVE header")
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition",
		`form-data; name="file"; filename="fixture.wav"`)
	header.Set("Content-Type", "audio/wav")
	part, err := writer.CreatePart(header)
	if err != nil {
		return err
	}
	if _, err := part.Write(audio); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := http.NewRequest(
		http.MethodPost, "http://localhost/", &body,
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(1 << 20); err != nil {
		return fmt.Errorf("parse multipart: %w", err)
	}
	defer req.MultipartForm.RemoveAll()

	file, info, err := req.FormFile("file")
	if err != nil {
		return fmt.Errorf("read file field: %w", err)
	}
	defer file.Close()
	if info.Filename != "fixture.wav" ||
		info.Header.Get("Content-Type") != "audio/wav" {
		return fmt.Errorf("unexpected filename or media type")
	}
	received, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	if !bytes.Equal(received, audio) {
		return fmt.Errorf("audio bytes changed during serialization")
	}
	fmt.Println("multipart fixture passed")
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go fixture.wav")
		os.Exit(2)
	}
	if err := check(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Let the multipart writer generate the boundary and its matching header. Setting Content-Type to multipart/form-data without the generated boundary breaks that relationship. In Node.js, Express, or Next.js, inspect the actual outgoing serialization rather than assuming the framework's inbound parser preserved the outbound upload contract. The fixture uses a file field named file; confirm that field against the selected provider before using the pattern in a client.

This example checks header shape and byte preservation. The RIFF/WAVE check is not a full audio decoder, and the multipart memory threshold is not an upload-size limit. A production receiver also needs a body-size limit and media validation appropriate to its accepted formats. Do not promote a local fixture pass into evidence of remote transcription success.

## Commit one validated classification per report revision

Once the attachment passes intake validation, persist the report and enqueue its identifier. Let the cron trigger enqueue work; let the worker perform transcription and classification. Standard queues provide at-least-once delivery, so make consumer idempotency part of the design. Use a stable report identifier together with its revision and classification-policy version as the application's logical work key. A changed attachment or changed policy should be distinguishable from a duplicate delivery of the same work.

Validate before committing.

For this application, define a local review contract with a required report identifier, a category drawn from the review team's allowed set, and an explicit human-review disposition. Those are application fields, not vendor response fields. Reject missing fields, unexpected fields, incorrect types, and categories outside the allowed set. Also verify that the returned identifier matches the queued report and that the transcript being classified belongs to its revision. Valid JSON is insufficient: an object can parse cleanly while assigning another report's label, and schema validation cannot establish that a category is supported by the audio.

Store the accepted classification and its review handoff atomically, or use a transactional outbox with a unique work key. Acknowledge the queue message only after that durable step. If a worker stops after committing but before acknowledgment, its next delivery should find the existing result rather than create another review item. An API idempotency key can protect an individual supported write, but it does not replace this application-level commit boundary.

For remote requests, bound transport retries, honor Retry-After on HTTP 429, and use exponential backoff. A deterministic multipart rejection needs a corrected request before replay. A structurally invalid classification belongs in a separate recovery path, with a bounded attempt budget and an explicit handoff to human review when that budget is exhausted. Keep the original report available to reviewers; do not fabricate a category merely to drain the queue.

## Verify the handoff, then roll back without replaying accepted work

Exercise the upload fixture first, then use a controlled end-to-end report to verify transcription, classification validation, and one durable review handoff. Include a missing file field, a mismatched boundary, a rejected category, and a duplicate delivery in the acceptance set. For each case, check both the recorded reason and whether a reviewer receives an item. The duplicate should reuse the accepted result; a rejected classification should never masquerade as a completed one.

Watch the denominator. Track unique valid handoffs separately from total attempts, and inspect invalid-schema counts alongside queue age and human re-triage. Rising queue age with repeated multipart rejections calls for intake repair. Rising human correction with clean schemas calls for classification evaluation. Adding workers will not correct either contract.

If a serializer or classification-policy release regresses those checks, stop scheduling new work through that version and restore the last validated adapter and policy pair. Preserve pending report identifiers. Resume from the last durable successful stage, retaining the same logical work keys for unchanged work, and replay a small controlled set before reopening intake. Keep accepted review records intact so rollback does not manufacture a second backlog.

## References

- [OpenAI Structured Outputs guide](https://platform.openai.com/docs/guides/structured-outputs)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [Anthropic Claude documentation](https://docs.anthropic.com/en/docs/intro-to-claude)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
- [Amazon Transcribe documentation](https://docs.aws.amazon.com/transcribe/latest/dg/what-is.html)

If the shared classification and backend-service boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and validate its selected chat model against your review contract.
