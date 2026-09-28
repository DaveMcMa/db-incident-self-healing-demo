# Runbook: DB PROD-01 latency spike

Applies to entity `db-prod-01`, a production OLTP database.

## Symptoms
- `latency-alert` events for entity `db-prod-01`.
- Query latency (p95) above 500 ms sustained for 30 seconds.
- Alert count may exceed 100 in a single window (storm).

## Likely causes
1. Missing or stale index on a hot table.
2. Lock contention / block on a high-write table.
3. Sudden query-parameter change causing a plan regress.

## Fix steps
1. Confirm with `SHOW PROCESSLIST` for blocking locks.
2. Check slow-query log for plan change on the hot table.
3. Add or rebuild the index on the affected table.
4. If locks persist, kill the oldest blocking transaction and requeue.

## Verification
- Run after fix: p95 query latency back under 100 ms.
- Alert stream for `db-prod-01` stops producing new `latency-alert` events.

## Auto-run assessment
If latency is DBA-visible and fix is a standard index rebuild, safe to
auto-run. If lock contention on a transactional table, require human verify.
