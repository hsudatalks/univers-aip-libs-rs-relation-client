# Relation Port client

`univers-aip-lib-relation-client@0.1.0` exposes `WorldRelationClient` at the crate
root and under `relation`. Construct it with a caller-selected public
`Arc<dyn DataRelationPort>` and an explicit owner label.

This is a transport-free helper for canonical relation-record-owned edges. It
preserves natural identity (including symmetric endpoint normalization),
existing edge IDs, recorded-at replay behavior, explicit durable idempotency
keys, same-owner metadata checks and atomic replace requests. Reported-value
convenience methods retain their explicit `Reported` provenance and system time.
It does not implement the Relation Port, own a store, select a World or inspect
an environment. Source-object-owned edges remain the source owner's concern.

The owner label is metadata, not authenticated identity or authorization. The
provided World Port must enforce actual scope, authority and atomic persistence.
Durability/replay guarantees come from that Port; this library is not a durable
queue or cross-process lock. Test memory Ports model calls and do not establish
production persistence acceptance.

Consumer-selected C0 Data/World rc.1 compatibility is checked explicitly.
Run `bash scripts/check.sh`, `bash scripts/build.sh` and
`bash scripts/publish.sh [--dry-run]` in this independent checkout.
