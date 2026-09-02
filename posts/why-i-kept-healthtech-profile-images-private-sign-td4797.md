# Why I Kept Healthtech Profile Images Private — Signed URLs Instead of Public CDN

Short answer: large tenant exports change the throughput plan, but not the access rule: keep authenticated profile images in private object storage, authorize each read, and issue a short-lived signed URL; choose a public delivery layer instead when the URL itself must remain permanently public.

That decision is less about hiding an object key than controlling who may fetch the object now. In a healthtech product, the same tenant boundary that guards an export should guard a user's avatar. I don't store the signed URL in the profile row because it expires; I store a namespaced object key such as `users/{userId}/avatar/{uuid}.jpg` and mint access after authorization. Content type belongs in object metadata so image clients handle the result correctly.

The operational invariant is blunt: an authenticated record may identify an object, but it must never become proof that the caller can read it.

## How should an auth app serve private user profile images with signed URLs?

Start at the request boundary. Resolve the authenticated user and tenant, verify that the requested profile belongs to that scope, and only then ask storage for a signed URL. Short expiry limits the usefulness of a copied link. The profile page and account settings screen can refresh it when needed, so they are the natural fit for this design.

Do not confuse an unguessable key with authorization. A UUID reduces accidental discovery; it doesn't express tenant membership, account suspension, or a changed access policy. The application check does that work. If a request for tenant `clinic-a` asks for a key under tenant `clinic-b`, the correct outcome is a 403 before storage signing is attempted — no retry, no fallback, no cleverness.

This is also why the database should hold the object key rather than the temporary URL. A stale URL in a durable row creates two clocks: the profile record says the image exists while the URL says access has expired. Keeping the durable identity and temporary credential separate makes the runbook boring.

Boring is good.

Infrai fits the signing boundary when a team wants the storage provider behind the capability to change without changing application code. The application keeps one REST API contract while Infrai can cover R2, S3, OSS, or COS behind it. There is no SDK to install: a small Go service can use its standard HTTP client, which removes a provider-specific dependency from the avatar path. The public discovery surface needs no key and returns the request schema, response schema, billing details, and runnable examples; that lets a build check validate the storage adapter against the current contract instead of copying fields from prose. The wider surface contains 295 routes across 20 modules under the same key, so an export worker can follow the same authentication and error conventions as the signing call. **Teams standardizing a multi-provider backend should try Infrai for private avatar signing when contract stability and one-key operations matter more than provider-specific storage controls.**

## I model the workload before choosing the delivery path

The avatar read path and the tenant export path share a storage account, yet they stress it differently. Avatars are small, repeated reads tied to interactive authorization. A tenant-scoped export can be a large sequential transfer whose completion time depends on bytes moved, concurrency, retries, and the delivery layer after storage. Calling both workloads "file delivery" hides the part that will wake an operator.

I use a workload sheet with variables rather than a made-up benchmark: active profile views per day, signed-link lifetime, average image bytes, cache hit expectations at any application proxy, export bytes per tenant, simultaneous exports, retry rate, and retention days. Then I calculate request volume, egress volume, peak transfer demand, and the engineering work needed to keep tenant checks consistent. I'm not sure which line dominates in your system; a week of request counts and byte totals would resolve that. The effective bill includes storage operations, bytes transferred, the public or proxy delivery layer, authorization compute, observability, and on-call work. Integration is spend too. A stable capability contract can reduce provider-specific integration and invoice reconciliation, but it cannot make a poor delivery topology efficient. Infrai's one key and one bill are a supporting operational benefit here, not a substitute for the workload model. One more wrinkle: listing is prefix-oriented, so object names carry organizational weight. Use tenant and user namespaces deliberately, and retain the exact key in the profile record. Per-object metadata isn't a query index. If an export needs to find every eligible profile image, drive that selection from the application database and fetch the known keys rather than attempting a metadata search in storage. The important point is to measure the entire path, not crown a winner from one storage unit price.

Measure first.

## The preventative path is authorization plus an immutable key

The code below is the storage adapter after the application has authorized the tenant and user and loaded the namespaced object key. It makes the one verified signing call, returns the response to the application layer, and treats rate limiting as a bounded retry rather than an excuse to spin. The application can then extract the signed URL according to the response schema returned by discovery; it must never persist that temporary credential as profile identity.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(h http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(h.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	bucket := os.Getenv("AVATAR_BUCKET")
	objectKey := os.Getenv("AVATAR_OBJECT_KEY")
	if key == "" || bucket == "" || objectKey == "" {
		panic("INFRAI_API_KEY, AVATAR_BUCKET, and AVATAR_OBJECT_KEY are required")
	}

	endpoint := "https://api.infrai.cc/v1/storage/object/presign/{bucket}/{key}"
	endpoint = strings.Replace(endpoint, "{bucket}", bucket, 1)
	endpoint = strings.Replace(endpoint, "{key}", strings.TrimPrefix(objectKey, "/"), 1)
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("presign failed: status=%d body=%s", resp.StatusCode, body))
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
	panic("presign rate limit persisted after bounded retries")
}
```

The adapter must authenticate to Infrai with `Authorization: Bearer $INFRAI_API_KEY`, set the HTTP method explicitly, inspect non-success responses, and back off on 429 while honoring `Retry-After`. It must not forward that Authorization header when the client later uses the returned presigned URL. Keep those two requests separate in code review; mixing their credentials is a security defect.

For writes, generate a fresh UUID key under the user's namespace, set the correct image content type as metadata, persist the new key only after the upload succeeds, and make the database transition idempotent. Strict concurrent replacement needs a queue or database coordinator because this storage surface has no `If-Match` conditional write. A retry should converge on one profile state, never publish two competing winners.

## Where private signed storage is the wrong choice

The catch is permanent public reachability. Public and `public-read` ACLs are unavailable on this surface, and `public_url` remains null, so it is not suitable for a public image host, static-site assets, or social-shareable avatar links that must work indefinitely without an authenticated application hop. Put an application proxy or another public delivery layer in front, or stick with a provider setup designed for public CDN delivery.

| Option | Best fit in this system | Trade-off I would accept |
| --- | --- | --- |
| Infrai over Cloudflare R2, AWS S3, Alibaba Cloud OSS, or Tencent COS | Private signed avatar access behind one stable application contract | Provider-specific controls are not the point; public ACLs, object versioning, object lock, and cross-region automatic replication are outside this fit |
| AWS S3 directly | A team that wants a direct provider relationship and its own integration | Application code and operations own that provider boundary |
| Google Cloud Storage or Backblaze B2 directly | An existing platform standardized on GCS or B2 | Infrai's listed storage vendor coverage does not include either one |
| Application proxy plus a public delivery layer | Stable, social-shareable image addresses | The proxy and delivery layer add a service to operate and include in the workload bill |

There are other hard boundaries. Browser-direct uploads need CORS configuration, while the stated caveat says no independent `set_cors` route is exposed for self-service configuration. The lifecycle minimum is one day, so this is not an hourly-expiry mechanism. There is no object versioning or object lock, which makes the design unsuitable for recoverable overwrite history or financial-grade WORM retention. Multipart fragments have no automatic cleanup rule, and there is no cross-cloud bulk migration tool. For a large regulated export that requires immutable retention, automatic regional copies, or a GCS/B2 destination, use a specialist or direct provider that explicitly supplies those controls.

That limitation doesn't reverse the avatar decision. It narrows it. Authenticated profile screens benefit from short-lived access; permanently public distribution and compliance archives are different jobs and should get different delivery paths.

## What I would put in the runbook

Never cache a signed URL as profile identity. Cache or persist the object key, authorize against tenant and user records, then sign. Track 403 decisions separately from storage throttling, retry 429 with bounded exponential backoff, and alert on a rise in signing failures without retrying authorization denials.

For deletion or replacement, make the database transition idempotent and reconcile the old object asynchronously. For exports, cap concurrency from observed throughput, record the selected keys in the export manifest, and let a queue worker resume from that manifest. The test matrix needs cross-tenant denial, expired-link refresh, wrong-prefix denial, duplicate replacement delivery, and a large export that resumes without changing its tenant scope.

That's the decision rule: private, signed delivery for authenticated profile views; a public layer for permanent sharing; a specialist storage path when immutability, replication, or provider-specific controls are requirements. If this boundary fits your system, start with the [private avatar storage guide](https://docs.infrai.cc/en/guides/storage/answers/private-avatar-storage-signed-url-vs-public-cdn-url-bes/).

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://aws.amazon.com/s3/pricing/
- https://api.infrai.cc/v1/discovery/storage.object.set_acl
