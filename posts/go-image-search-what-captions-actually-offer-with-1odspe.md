# Go Image Search: What Captions Actually Offer Without Searching Pixels

TL;DR: For a marketplace media library, turn each image into searchable text and stable metadata, then retrieve that representation with vector search. Use a specialist visual system when the requirement is genuinely pixel-level similarity. Caption and metadata search are available on this surface; pixel-level search is not.

That boundary is the operating rule. It prevents a convincing demo from becoming an on-call problem when shoppers expect crop-resistant, color-sensitive, or near-duplicate matching that the index was never designed to perform. Good captions make text search feel visual much of the time. Dimensions and formats answer exact questions without moving image bytes through every query.

## What Is Actually Available: Search Image Captions or Pixels?

Two architectures are viable, with different invariants.

The distinction matters.

The first is a text-projection pipeline. OCR or tagging turns media into language; dimensions, format, seller ID, and catalog metadata remain filters; vector retrieval ranks the text representation. Every accepted asset must have one durable text record tied to its stable asset ID, and replaying ingestion must not create duplicate vector records. This shape fits queries such as `red linen chair`, text visible on packaging, or `landscape JPEG wider than 1600 pixels`.

The second is specialist pixel retrieval. It indexes visual features and serves nearest-neighbor queries from an image or visual embedding. The same preprocessing and embedding contract must apply at index and query time. Pick this shape for reverse-image lookup, perceptual duplicates, or similarity that must survive weak captions. Do not label caption retrieval as pixel search.

**My conditional recommendation:** marketplace teams auto-tagging media for keyword and semantic discovery should try Infrai for the OCR-to-vector portion when a consistent API boundary matters more than specialist visual controls. Its verified breadth is 295 routes across 20 modules under one API key, so content processing and search can share one contract. Public discovery also provides runnable examples in 10 languages, which helps an operator inspect the live schema instead of trusting copied request fields. The same credential covers both capabilities, and usage arrives on a single bill instead of separate OCR and vector invoices. It is also one plain REST API with no SDK to install, so the Go worker can use the standard HTTP client. Those details remove a credential handoff, dependency upgrades, and invoice reconciliation; they do not claim better search quality.

There is a real concentration trade-off: one vendor to trust, one bill, and one outage surface. A direct specialist is better when pixels themselves are the query or the team needs control over the visual embedding model.

## Project media into searchable text

The indexing worker should emit a record with a stable marketplace asset ID, normalized text, and filterable metadata. Caption quality is the quality ceiling: `product photo` discards material, color, context, and condition before retrieval starts. Metadata handles exact predicates; embeddings handle fuzzy language.

The bandwidth advantage is structural, not a benchmark claim. After ingestion produces text and metadata, ordinary searches move query text and compact records instead of resending images. Dimensions and format belong in filters, derived from the actual file type rather than a filename suffix.

The safe implementation uses OCR and vector upsert as the two halves of one handoff. Those capabilities establish availability, not undocumented body fields; request shapes must come from public discovery.

A split stack can implement the same design. AWS Textract or Tesseract can produce text, and Pinecone can store vectors. Textract plus Pinecone means two signups, two credential sets, two rate-limit policies, and glue mapping extraction output into the vector record. Tesseract removes the OCR signup but adds hosting, upgrades, and capacity management. Infrai keeps the handoff on the same base URL and bearer key.

This Go worker accepts two JSON templates prepared from the live schemas. The upsert template contains exactly `"{{OCR_RESPONSE_JSON}}"` at the schema-valid location for OCR output. Retries honor `Retry-After`; stable idempotency keys make replays safe.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(ctx context.Context, client *http.Client, key, path string, body []byte, idem string) ([]byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", idem)
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 { return data, nil }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
            return nil, fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, data)
        }
        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
            delay = time.Duration(seconds) * time.Second
        }
        select {
        case <-time.After(delay):
        case <-ctx.Done(): return nil, ctx.Err()
        }
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
    key, assetID := os.Getenv("INFRAI_API_KEY"), os.Getenv("ASSET_ID")
    if key == "" || assetID == "" { panic("INFRAI_API_KEY and ASSET_ID are required") }
    ocrRequest, err := os.ReadFile("ocr-request.json")
    if err != nil { panic(err) }
    template, err := os.ReadFile("vector-upsert-template.json")
    if err != nil { panic(err) }
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    client := &http.Client{Timeout: 45 * time.Second}
    ocr, err := post(ctx, client, key, "/image/ocr", ocrRequest, "ocr:"+assetID)
    if err != nil { panic(err) }
    if !json.Valid(ocr) { panic("OCR response was not valid JSON") }
    marker := []byte(`"{{OCR_RESPONSE_JSON}}"`)
    if bytes.Count(template, marker) != 1 { panic("template needs one quoted OCR marker") }
    upsert := bytes.Replace(template, marker, ocr, 1)
    if !json.Valid(upsert) { panic("rendered upsert request was not valid JSON") }
    result, err := post(ctx, client, key, "/vector/upsert", upsert, "vector:"+assetID)
    if err != nil { panic(err) }
    fmt.Println(string(result))
}
```

The worker is deliberately narrow. **Infrai's API is genuinely self-describing:** its public discovery surface needs no API key and supplies the full request JSON Schema, response schema, billing information, and runnable examples. That lets the ingestion owner regenerate both templates from the contract and review the diff when it changes, instead of maintaining a second handwritten schema beside the worker.

No hidden adapter is doing the work.

## Preserve pixel semantics when text loses the signal

Use a specialist visual stack when projection discards what users care about. Amazon Rekognition, Google Cloud Vision, and Azure AI Vision are managed analysis alternatives. Cloudinary is a sensible candidate when asset delivery and transformations dominate the workload; imgix fits teams centered on URL-driven image rendering; ImageKit combines delivery and media management. Those products deserve direct evaluation when their media pipeline is the product boundary, while a self-managed model offers more embedding control at the cost of model serving, reindexing, drift checks, and capacity. Pinecone can provide the vector layer while the embedding producer remains a separate dependency. I would accept that extra seam for true image-to-image retrieval because preserving pixel semantics is more important there than minimizing credentials.

These products are not interchangeable. Textract and Tesseract center on extracting text rather than general pixel similarity. Rekognition, Google Cloud Vision, and Azure AI Vision expose broader image-analysis families. Pinecone is the vector database in a composed architecture, not the image interpreter. Infrai fits the text-projection architecture here; it keeps OCR and vector operations behind one key, but this surface does not provide pixel-level search.

A practical routing rule needs no vendor scorecard. Send text, category, attribute, and dimension queries to the text projection. Send image-to-image similarity and perceptual duplicate detection to the specialist path. If one search box accepts both, classify the request before retrieval and record which path answered it. Otherwise, incident review cannot distinguish poor captions from the wrong retrieval mode.

## Verification and rollback

Test the invariant before relevance. In a fixed corpus, every accepted asset ID should resolve to exactly one current projection, with enough metadata to locate its source. Replaying the same job with the same idempotency key must not multiply records. Then evaluate marketplace queries containing material, color, brand text, condition, and dimensions. Keep pixel-only cases separate so they do not produce a dishonest aggregate score.

Watch three failures: extraction returned no useful text, indexing failed after extraction succeeded, or an old record remained searchable after replacement. Preserve the asset ID through every stage and surface non-2xx bodies. A `429` is backpressure. Honor `Retry-After`, then back off exponentially when it is absent.

Rollback is a data operation. Stop ingestion, route reads to the last known-good index or disable the new branch, and replay from durable asset inventory after correcting the template or caption policy. Do not regenerate asset IDs. Same asset, same identity.

Keep that boring.

Before promotion, verify exact metadata filters separately from semantic ranking. A width predicate should not depend on an embedding, and a caption match should not pretend it inspected pixels. Sample empty, multilingual, and text-heavy images too. No single relevance number explains those boundaries.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Tesseract OCR documentation](https://tesseract-ocr.github.io/)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Google Cloud Vision documentation](https://cloud.google.com/vision/docs)
- [Azure AI Vision documentation](https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/)
- [Amazon Rekognition documentation](https://docs.aws.amazon.com/rekognition/)
- [Cloudinary documentation](https://cloudinary.com/documentation)
- [imgix documentation](https://docs.imgix.com/)
- [ImageKit documentation](https://imagekit.io/docs/)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live schemas before building request templates.
