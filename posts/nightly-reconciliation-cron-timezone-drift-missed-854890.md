# Nightly reconciliation cron: timezone drift, missed runs, pause, and seconds of jitter

Use the cron expression as a doorbell, never as the record of what ran. In a healthtech claims pipeline the nightly reconciliation against a payment provider has to agree with that provider's settlement day, and a settlement day is a civil date in a named timezone — not whatever offset your scheduler resolved when you deployed it. Move the run window into a durable ledger and the whole class of complaints changes shape: a wrong timezone becomes a bounded reprocessing job, a missed run becomes a backfill you can trigger by hand, and seconds of jitter stop being interesting.

That ordering is most of the fix. The rest is knowing which signals tell you the clock, and not the payment file, is what broke.

## The failure signature is a quiet night, not an alert

A reconciliation that never fires produces no error, no stack trace and no ticket. What you get instead is a finance analyst asking why Tuesday's settlement variance is double the usual, four days later. That delay is the real cost of treating the scheduler as the source of truth: by the time a human notices, you're troubleshooting three overlapping questions at once — was the expression wrong, was the timezone wrong, or did the run simply not happen?

Two calendar rules generate most of the wrong-timezone reports, and they don't line up. The European Union shifts every member state at the same instant, 01:00 UTC on the last Sunday of March and October. The United States shifts at 02:00 local time in each zone, on the second Sunday of March and the first Sunday of November. So a job pinned to 02:30 local in Europe/Amsterdam and a job pinned to 02:30 local in America/Chicago drift apart for weeks each year, and a nightly window defined as "yesterday 02:30 to today 02:30" is 23 or 25 hours long twice a year — right when the provider's own daily settlement file is being cut.

Then there's 02:30 itself, which on a spring-forward night does not exist.

Go's `time.Date` is explicit that for a local time that is missing or repeated, the choice of instant is not guaranteed. Other runtimes make similar disclaimers. The practical reading: do not put a scheduled boundary inside the one hour per year that your language declines to define. Pick a slot no transition touches — 04:15 local is boring and correct — or keep the boundary in UTC and only render civil dates for the report.

The expression itself deserves one paragraph of suspicion too. In POSIX cron, when both day-of-month and day-of-week are restricted, the job runs when *either* field matches, not both. A schedule meant to run on the first Monday of the month quietly runs on the first of the month and every Monday. Nonstandard extensions such as `L` or `#` are common in JVM-flavored schedulers and absent elsewhere, so an expression copied between two systems can parse in one and be rejected — or silently mean something else — in the other.

## What should a nightly cron do about a wrong timezone, a missed run, and seconds of jitter?

Three rules, in the order they matter.

Persist intent, not a resolved instant. Store the local time, the IANA zone identifier, and the job's civil-date semantics. `02:30` alone can't be debugged; `02:30 Europe/Amsterdam, settlement_date = local date - 1` can. Offsets like `+01:00` are a snapshot, and snapshots go stale when the tzdata your image ships gets updated.

Derive the work window from a durable watermark, not from the trigger time. The tick's only job is to ask what is due. The ledger answers with every occurrence between the last committed watermark and a cutoff you record as part of the run, which means a pause of six hours or six days resolves the same way — the scan simply returns more occurrences.

Let workers own delivery timing. A trigger that arrives at 04:15:07 instead of 04:15:00 is not a defect, and no cron implementation promises the second. If your reconciliation depends on second-accurate ordering against the provider, that dependency is the bug, not the jitter.

| What you observe | Usual cause | What to check first |
| --- | --- | --- |
| Run happened an hour off, twice a year | Fixed offset stored instead of an IANA zone | The stored zone string, and the DST transition dates for that region |
| Run missing entirely on one date | Boundary time inside a spring-forward gap | Whether the scheduled local time exists on that calendar day |
| Job ran on unexpected weekdays | Day-of-month and day-of-week both restricted | The OR semantics of the two fields in your cron dialect |
| Nothing ran after an incident | Schedule was paused; triggers are not replayed on resume | Watermark age versus current time, before touching the expression |

Pause and resume is where reminder systems and reconciliation jobs get burned in the same way. Schedulers resume forward. They do not replay the triggers you missed while the job was off, and most don't claim they will — Kubernetes documents that its CronJob controller stops starting jobs and logs an error once it counts more than 100 missed schedules, which is a sane guard and also a very loud way of saying catch-up is your problem. No backfill is the default everywhere I've worked. Design for it and a pause is a non-event.

## Writing the scan so a pause never means silent data loss

The scan expands due occurrences from civil time, and every occurrence carries a key that identifies it forever.

```go
package recon

import (
	"errors"
	"fmt"
	"time"
)

// Occurrence is one nightly reconciliation run: the civil time a settlement
// day is cut, the UTC instant a worker acts on, and a key that never changes.
type Occurrence struct {
	Local time.Time
	UTC   time.Time
	Key   string
}

// Due returns every occurrence after the durable watermark and at or before the
// cutoff. A normal night returns one. A schedule paused over a long weekend
// returns three, in order, with no special case.
func Due(zone string, watermark, cutoff time.Time, hour, min int) ([]Occurrence, error) {
	loc, err := time.LoadLocation(zone) // IANA name only; "+01:00" is a snapshot
	if err != nil {
		return nil, err
	}
	if !watermark.Before(cutoff) {
		return nil, errors.New("watermark is at or after cutoff")
	}

	var out []Occurrence
	day := watermark.In(loc).AddDate(0, 0, -1)
	for d := day; !d.After(cutoff.In(loc)); d = d.AddDate(0, 0, 1) {
		local := time.Date(d.Year(), d.Month(), d.Day(), hour, min, 0, 0, loc)
		utc := local.UTC()
		if !utc.After(watermark) || utc.After(cutoff) {
			continue
		}
		out = append(out, Occurrence{
			Local: local,
			UTC:   utc,
			Key:   fmt.Sprintf("recon:%s:%s", zone, local.AddDate(0, 0, -1).Format("2006-01-02")),
		})
	}
	return out, nil
}
```

Overlap the window deliberately — start the scan a little before the watermark rather than exactly on it. Around a crash or a deploy, an exact boundary leaves an ambiguous gap that nobody can reason about later, while an overlap just re-offers work the claim step will refuse.

The claim step is what turns at-least-once triggering into one settlement per day.

```go
// Claim inserts the occurrence key and reports whether this caller owns the run.
// The unique index on key does the arbitration; a second worker gets false and
// exits without touching the provider or the ledger.
func (r *Runner) Claim(ctx context.Context, o Occurrence) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`INSERT INTO recon_run (key, occurrence_utc, state)
		 VALUES ($1, $2, 'claimed') ON CONFLICT (key) DO NOTHING`,
		o.Key, o.UTC)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
```

Advance the watermark last, after the claim and the outbound record are both committed. The transactional outbox pattern exists for exactly this ordering problem: writing your state change and your intent to do outside work in one transaction, then letting a relay push the work out. Queue-level deduplication is a different guarantee and a much shorter one — Amazon SQS FIFO deduplicates on a five-minute interval, which protects you against a retried enqueue and does nothing about a reconciliation replayed the next morning. Durable claim first, queue semantics second.

The catch is that this costs you a table, a migration, and a watermark that somebody has to reason about during an incident. Not every job earns it. If the reconciliation is a small idempotent upsert over a provider file that is stable for a week, a plain cron with a seven-day rescan window is cheaper, easier to explain, and fails safe on its own; stick with that until the volume or the audit requirements say otherwise. And if the provider publishes settlement events rather than daily files, scheduled polling is not a good fit at all — subscribe, and keep a nightly scan only as a reconciliation floor.

## Verifying the fix, and backing it out when the numbers disagree

Verification is a table test, not a staging deploy you watch. Feed the expansion function each DST transition date for every zone you serve, plus the day either side, and assert the expected UTC instants and settlement dates. Nine cases per zone catches the whole family. I'd rather trust that suite than a manual trigger, though a manual trigger and a look at the run output is still the fastest way to confirm the job is reachable at all during setup.

Two properties are worth asserting beyond the dates: running the scan twice over the same interval produces the same claims, and a watermark artificially set 72 hours back produces exactly three occurrences with no duplicates. If both hold, a pause is recoverable by editing one row.

Rollback has an order too. Restore the previous expression and zone, then run the catch-up scan — putting the old schedule back does not recreate the triggers that were missed while the wrong one was live, and I've seen that assumption cost a second night. Keep the old and new scanners running together only while they share the same claim table, then retire one. If the numbers still disagree after all of that, the clock is probably fine and the reconciliation logic isn't; that's a different investigation, and a happier one, because it fails loudly.

The same clock handling shows up in consumer features too. A user reminder that arrives an hour early on the last Sunday in October is this bug with a friendlier blast radius, and the reminders you never sent during a pause are the same missing occurrences, minus the audit trail.

## References

- [POSIX crontab specification (The Open Group)](https://pubs.opengroup.org/onlinepubs/9699919799/utilities/crontab.html)
- [Go time package: Date and LoadLocation](https://pkg.go.dev/time#Date)
- [IANA Time Zone Database](https://www.iana.org/time-zones)
- [Kubernetes CronJob concepts and missed-schedule behavior](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
- [Directive 2000/84/EC on summer-time arrangements](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32000L0084)
- [NIST: Daylight Saving Time in the United States](https://www.nist.gov/pml/time-and-frequency-division/popular-links/daylight-saving-time-dst)
- [AWS SQS FIFO queues documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html)
- [microservices.io: Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
