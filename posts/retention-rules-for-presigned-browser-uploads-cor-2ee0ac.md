# Retention Rules for Presigned Browser Uploads: CORS, Signed Download, No Proxy Server

Use a direct browser upload to object storage only when the account can hand out two narrow grants with no proxy server in the byte path: a per-object write grant (a presigned PUT, or a POST policy carrying a size condition) and a per-object read grant (a signed download URL with a short expiry). Where the only credential a provider offers is zone-wide or bucket-wide, the browser becomes a place that credential lives, and no amount of CORS configuration repairs that. Everything else in this decision — request pricing, European region placement, multipart tuning — is downstream of it.

The rule is about access control. Not throughput.

I run cron and queue infrastructure for a property management platform, and the artifacts in question are boring: inspection photos shot on phones over patchy LTE, plus nightly exports of labeled maintenance tickets. They train a triage model that decides whether "water on the kitchen floor" is a plumbing job or an appliance job. Two obligations sit on that pile at once. Any training run has to be reconstructable months later — exactly which objects were in it, byte for byte — and a tenant erasure request has to actually remove bytes without erasing the record that a run once used them.

Those two obligations are what make the direct-to-storage question interesting, because the moment you take your own server out of the upload path, you also take out the place where you used to enforce both.

## Should a direct browser upload skip the proxy server, or keep it for storage access control?

Removing your app from the byte path removes real problems: request body buffering, timeouts on 40 MB photos from a basement with one bar, and paying for the same bytes twice as they transit your compute. It removes none of the control plane. Authentication, key allocation, size and content-type limits, and completion state all stay yours. The browser gets one narrow capability, and nothing more.

On the S3 protocol the write grant comes in two shapes, and the difference matters more than most comparisons admit. A presigned PUT signs a method, a key, and an expiry; the client cannot change the key, but it also cannot be stopped from sending 4 GB when you expected 4 MB. A browser POST policy signs a document that can carry a `content-length-range` condition, so the store itself rejects an oversized body. If you need a hard size ceiling without a proxy, that policy is the only lever the protocol gives you. Otherwise you accept the object first and reconcile after, which means a cleanup job and a bill for the bytes you rejected.

CORS is the other half, and it's a deploy-time concern disguised as a storage setting. The browser sends a preflight `OPTIONS` before a cross-origin PUT, so the bucket has to allow your origin and method, allow whatever headers you sign, and expose `ETag` if you assemble multipart uploads client-side. Every preview deployment mints a new origin. Decide early whether the app team can edit those rules on its own or whether each new preview origin waits on a security review, because that answer, not the API surface, is what people actually feel day to day.

Reads follow the same boundary. Keep the bucket private, authorize the request in your application, then issue a signed GET with a short life. The `response-content-disposition` override is worth knowing here: it lets a signed URL for a content-addressed object still download as `inspection-4417.jpg` rather than a hash, without a redirect through your server.

## The duplicate-delivery failure mode, applied to browser uploads

I have been paged for missed jobs and for duplicate deliveries more often than for anything else, and the second failure mode is the one that transfers directly to browser uploads. Two tabs. An offline retry that fires when the phone reconnects. A completion callback delivered twice because the first acknowledgement was lost, not because the work happened twice.

A presigned URL authorizes bytes. It says nothing about intent.

The invariant that fixed this for us is small enough to put on one line of a runbook: the object key must be a function of the content, and the state transition must be a claim. The browser hashes the file with SubtleCrypto before asking for a grant, the object key is derived from that digest, and a retry therefore rewrites identical bytes to an identical key — an overwrite that changes nothing. The database row is inserted before the URL is minted, and promotion to `ready` is a conditional update. If the update touches zero rows, an earlier delivery already won, and the correct response is to do nothing at all.

```go
package intake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Presigner is everything the intake path knows about the object store.
// Keeping the surface this small is what lets the provider stay a config value.
type Presigner interface {
	PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error)
	Stat(ctx context.Context, key string) (size int64, err error)
}

var ErrSizeMismatch = errors.New("stored object does not match the reserved artifact")

func objectKey(runID, digest string) string {
	return fmt.Sprintf("incoming/%s/%s", runID, digest)
}

// Reserve records the artifact before any bytes move, then hands the browser a
// short-lived grant for exactly one key. The digest comes from the client, so the
// same photo retried from a second tab lands on the same object.
func Reserve(ctx context.Context, db *sql.DB, ps Presigner, runID, digest string, size int64) (string, error) {
	_, err := db.ExecContext(ctx, `
		INSERT INTO artifacts (run_id, digest, bytes, state)
		VALUES ($1, $2, $3, 'pending')
		ON CONFLICT (run_id, digest) DO NOTHING`, runID, digest, size)
	if err != nil {
		return "", err
	}
	return ps.PresignPut(ctx, objectKey(runID, digest), 15*time.Minute)
}

// Complete runs on a queue worker, never in the browser. Deliver it twice and the
// second call promotes nothing: promoted=false is the normal, quiet outcome.
func Complete(ctx context.Context, db *sql.DB, ps Presigner, runID, digest string, want int64) (promoted bool, err error) {
	got, err := ps.Stat(ctx, objectKey(runID, digest))
	if err != nil {
		return false, err
	}
	if got != want {
		return false, fmt.Errorf("%w: run %s digest %s", ErrSizeMismatch, runID, digest)
	}
	res, err := db.ExecContext(ctx, `
		UPDATE artifacts SET state = 'ready', ready_at = now()
		WHERE run_id = $1 AND digest = $2 AND state = 'pending'`, runID, digest)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	// n == 0 means an earlier delivery already claimed this artifact.
	// Append the manifest entry only on the transition that actually happened.
	return n == 1, nil
}
```

The worker verifies against a stat call because a signed upload that returns 200 to a phone on a dying connection is not proof the object is intact. Size is the cheap check; a checksum header, where the provider supports one, is the better one.

Multipart uploads deserve a paragraph of their own, since direct browser uploads make them common. An abandoned multipart upload leaves parts that are stored, billed, and absent from an ordinary object listing. Set the lifecycle rule that aborts incomplete uploads on day one, or discover the parts a quarter later while reconciling a bill.

## Manifests, not object timestamps: reconstructing what a model was trained on

Lifecycle rules expire objects by age. Reproducibility is a statement about a set. Those are different things, and conflating them is how a training run quietly stops being reproducible: someone re-uploads a corrected photo, the object's clock resets, a `curated/` expiry rule fires on a neighbouring object that a manifest still references, and nothing in the pipeline notices until a retrain produces a different model on the same nominal inputs.

We split the bucket by policy rather than by team. `incoming/` holds raw uploads with a short expiry, `curated/` holds the deduplicated artifacts, and `runs/<run-id>/manifest.json` is written once and never rewritten — the digests, sizes, and labels that define one training set. Retention decisions attach to the manifest. The lifecycle rules only implement them.

```json
{
  "Rules": [
    { "ID": "abort-stale-multipart", "Status": "Enabled",
      "Filter": { "Prefix": "incoming/" },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 } },
    { "ID": "expire-raw-intake", "Status": "Enabled",
      "Filter": { "Prefix": "incoming/" },
      "Expiration": { "Days": 30 } },
    { "ID": "purge-noncurrent-versions", "Status": "Enabled",
      "Filter": { "Prefix": "curated/" },
      "NoncurrentVersionExpiration": { "NoncurrentDays": 30 } }
  ]
}
```

That third rule is the one teams forget. On a versioned bucket a delete is a delete marker, so the previous version — the actual personal data — survives until a noncurrent expiration rule removes it. An erasure request that only issues DELETE has not erased anything. The manifest, meanwhile, keeps the digest and a tombstone, so a later audit can say precisely that item 3,182 was withdrawn on request rather than silently vanishing from the record.

## Comparing the options on control rather than convenience

Comparisons of this decision usually rank providers by price per GB. For a no-proxy design the first question is narrower: can this account issue a per-object write grant and a per-object read grant, and who administers CORS?

| Option | Browser write grant | CORS control | Signed download | Retention controls |
| --- | --- | --- | --- | --- |
| AWS S3 | Presigned PUT and POST policy (SigV4) | Per-bucket CORS you configure | Presigned GET, with `response-content-disposition` | Versioning, lifecycle, Object Lock |
| Cloudflare R2 | S3-compatible presigned URLs | Per-bucket CORS you configure | Presigned GET | Lifecycle rules; check the published S3 compatibility list for the operations you rely on |
| Backblaze B2 | Native per-upload URL and token, plus S3-compatible presigned URLs | Per-bucket CORS rules | Download authorization token, or S3 presigned GET | Lifecycle rules, Object Lock |
| Bunny Storage | Edge Storage HTTP API authenticated with a storage-zone key, not a per-object signature | Delivery-side configuration | Token authentication at the pull zone | Your own tooling |

The last row is the interesting one, and it's a design position rather than a shortcoming. Bunny's Edge Storage API is built around a zone credential and a CDN in front of it, which is a fine fit for public assets and a poor one for a browser that must never hold zone-wide write access. Backblaze B2 gives you both a native upload flow and an S3-compatible surface, so a migration is mostly a question of which of the two your client library speaks. Cloudflare R2 publishes an explicit compatibility list for its S3 API, which is the document to read before assuming a lifecycle or versioning call behaves identically.

Pricing belongs in this comparison as a shape, not a number. Storage per GB-month, request charges split into cheap reads and expensive writes, egress, and — on some tiers — a minimum storage duration that bills a 30-day intake object as though it lived far longer. Signed downloads for a model training pipeline are read-heavy and internal, so egress terms dominate the total; a provider that charges nothing for egress and more per operation can invert the ranking of an otherwise identical workload. European placement is a separate axis again: a bucket in an EU region and a documented jurisdiction guarantee are not the same claim, and current per-region prices are on each vendor's own pricing page rather than in an article.

## Where this advice doesn't apply, and the one test that settles it

Keep the proxy when the files are small, the volume is low, and the team is small. One less CORS surface and one less signed-URL lifetime to reason about is a real saving when nobody is being paged over upload throughput. Stick with a server-side path when bytes must be scanned, transcoded, or virus-checked before they become durable — or route the direct upload into a quarantine prefix and promote after the scan, which buys most of the benefit and costs you one more state.

If delivery simplicity is genuinely the job — public marketing photos, floor plans on a listings page — the whole signed-download apparatus is overhead, and a public bucket behind a CDN is the honest answer.

The caveat I'd flag hardest: the S3 protocol is a common language, but S3-compatible does not mean lifecycle-identical. I'm not certain that noncurrent-version expiry, abort-incomplete-multipart, and object lock behave the same on every implementation that accepts the same JSON, and the resolving test is cheap — run your own expiry against a scratch bucket for a week and read the listing yourself, before the migration rather than after.

Where your bytes live is a two-way door. Who is allowed to write them, and what proves which ones a model saw, is not.

## References

- MDN, `Content-Disposition`: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
- MDN, Cross-Origin Resource Sharing (CORS): https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS
- MDN, `SubtleCrypto.digest()`: https://developer.mozilla.org/en-US/docs/Web/API/SubtleCrypto/digest
- AWS, S3 pricing: https://aws.amazon.com/s3/pricing/
- AWS, Using presigned URLs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- AWS, Managing the lifecycle of objects: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- Cloudflare, R2 S3 API compatibility: https://developers.cloudflare.com/r2/api/s3/api/
- Backblaze, B2 S3-compatible API: https://www.backblaze.com/docs/cloud-storage-s3-compatible-api
- bunny.net, Edge Storage API: https://docs.bunny.net/reference/storage-api
