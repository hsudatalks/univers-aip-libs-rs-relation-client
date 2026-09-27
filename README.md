# Withdrawn Relation helper

`univers-aip-lib-relation-client@0.1.0` is withdrawn and yanked from the Univers
Registry. Its coordination of canonical edge identity, recorded time and replay
belongs to World authority, not to a generic client library. Do not add it to new
consumers. Existing immutable artifacts and source history remain available.

Use the public World Relation intent contracts and
[`univers-aip-contracts-world-ipc`](https://github.com/hsudatalks/univers-aip-contracts-world-ipc)
wire values with an owner adapter. Trusted Host issuance, World semantic acceptance
and original durable receipts remain with their respective owners. A caller's
owner label or a later equal read is not authentication or a canonical receipt.

The last identified production consumer, Resource, removed this dependency in
`0ad7b8fbb63d0555379cbe65324c1318dd32f01d`, with independent owner checks and builds.
No active owner Cargo manifest or lock in the current `univers-aip` checkout
inventory references this package; this observation is not a claim about
unseen external consumers. Existing lockfiles can continue to fetch yanked artifacts.

## Historical interface (retained for source readers)

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
