# Genesis and replicated write-safety contract

This is the code contract for the fixed three-member pre-production launch profile,
not a claim that every production recovery or settlement gate has passed.

## Canonical genesis v2

The reserved Oak namespace is
`/oak-chain/00/00/00/0x0000000000000000000000000000000000000000`.
Its `content/genesis` child is the code-authored network bootstrap contract. The
zero address names Oak content; it is not an EVM account whose private key is needed
for bootstrap. `DO IT LIVE!` remains the motto.

Genesis declares:

> Oak Chain turns enterprise content into shared, verifiable state. Aeron orders
> commands; Oak stores their meaning. Every validator must produce the same typed
> logical state. Acceptance is not commitment: commitment requires a verified
> durable quorum. Genesis is reserved, versioned, and verified; failures must remain
> explicit rather than being reported as success.

`CanonicalGenesisContent` is the single authoring implementation. Genesis v2:

- accepts only a parameter-free `CREATE_GENESIS` trigger, takes creation time from
  Aeron, and derives the bootstrap member URL from configured membership;
- formats dates in UTC, with JCR `NAME`, `DATE`, and `BINARY` property types;
- omits node-local gateway URLs, backend enablement flags, and BlobStore IDs;
- stores the bundled image through the NodeStore, with a pinned byte SHA-256;
- records `version=2.0.0`, `integrityFormat=oak-chain-genesis-v1`, and an
  `integrityDigest` over the entire zero-wallet subtree.

The image remains the original 22,216-byte `genesis-assets/do-it-live.jpeg`:
`ed06f9cbf0fe878013ccb266170e6b3ba676933a6f065675cc0115c840bf1442`.
Oak may externalize the binary according to the configured store. Its content bytes,
not its storage reference, define logical identity. Independent binary availability
is a separate operational requirement.

The digest is SHA-256 over a length-framed stream: format identifier, relative node
paths, sorted property names, exact type tags, scalar/array flags, value counts and
values, and sorted children. Sorting uses Java's natural string ordering; strings
are UTF-8 with a four-byte length prefix. Binary values contribute their byte count
and SHA-256. Only `/content/genesis@integrityDigest` is excluded. Segment RecordIds,
TAR layout, and blob references are not part of the digest.

Verification checks the stored seal and independently re-authors the expected tree
on a detached memory builder, including the pinned image. It does not write blobs
or mutate the repository. Corrupt, incomplete, unsupported-version, or differently
authored content is rejected. The `genesisValidator` field denotes the configured
bootstrap endpoint: the lexicographically first URL in the common configured HTTP
member set. It is not an Aeron member-ID, ingress-caller, or elected-leader claim.
All members must have the same configured HTTP endpoint set; local3 verifies this
alongside the genesis digest.
Configured self and peer URLs share a pure lexical canonicalizer (including the
`localhost`/`::1` loopback alias); public hostnames remain hostnames. DNS resolution
is not allowed to change the identity recorded in genesis.
A local digest is not a signature or an external
commitment: replacing both legitimate genesis proposal inputs and all derived state
requires an independently retained proposal commitment to detect adversarially.

There is no implicit v1-to-v2 migration. Disposable pre-production stores can be
reseeded deliberately. A deployed network would require an explicit migration plan.

## Mutation and failure boundaries

Ordinary registration, writes, deletes, binary admission, queue admission, and
replicated write/delete application reject the zero wallet. Mutation-path checks
also reject the zero-wallet subtree and its ancestors. Internal replicated genesis
creation is the explicit bootstrap exception.

Standalone replicated writes carry Aeron's command timestamp through immutable
`MutationAuditMetadata.appliedAt`. It is never accepted from ingress JSON. Content
`timestamp`, wallet `walletCreated`, and wallet `lastWrite` use that one value.
Legacy direct-call helpers retain a local-clock fallback; they are not the
standalone replicated ingress path.

Aeron's committed log is the apply authority. The dispatcher never filters committed
commands using a locally synthesized HTTP leadership term: the term it compares is
Aeron's `leadershipTermId`, applied in log order from `onNewLeadershipTermEvent`, so
every member reaches the same decision. Malformed/stale input rejection
(`MutationRejectedException`) remains distinct from a member-local apply failure.

A valid committed write/delete/genesis that cannot be applied latches a quarantine
and terminates the Aeron clustered-service agent with a FATAL cause, which takes the
fail-stop path (crash record, Aeron shutdown, then the configured process-exit
callback). Later callbacks cannot continue applying commands. Health reports failure
and ordinary HTTP routes return 503 until restart and repair. Startup is not
write-ready until canonical genesis is verified.

Every replicated write, delete, batch item, and genesis records its applied-log
watermark (Aeron log position, batch item, and term) under the hidden root node
`:consensus` in the same Oak merge as the mutation. Replay skips entries at or below
the watermark, so a restarted member re-applies only what its store does not yet hold;
an entry that failed on a member is above its watermark and is applied again after
restart. Startup refuses an Aeron log or snapshot that does not belong to the store.
The watermark is outside the zero-wallet subtree and is not part of the genesis digest.

## Durability acknowledgement

`202 Accepted` means queue admission only. Poll
`/v1/ops/operations/{proposalId}`; success is `state=COMMITTED`, corresponding to
`sourceState=PROCESSED` plus `durabilityState=ACKED`.

Every local durability vote must follow a successful FileStore flush covering that
application. A repeated write, an already absent delete target, or an existing
`oak:proposalId` does not prove disk persistence: those paths request a flush too.
Flush callbacks are assigned to a captured cohort. Work registered while a flush is
in progress waits for a subsequent flush. Failed flushes retain callbacks for retry
and emit no durability-success vote. Callback HEADs are captured before the flush,
not sampled from potentially newer state when the callback runs.

## Startup feature gate

The startup gate `oak.consensus.safety.enabled` defaults to true. The explicit value
`false` prevents a v2 validator from starting; a misspelling does not disable safety.
It is read at startup, not used to switch replicated semantics while a member is
running. Clock, durability, reservation, integrity, and failure invariants are
unconditional at apply, so mixed or changing node-local settings cannot restore an
unsafe state machine. This is a deployment gate, not a v1 migration or unsafe
behavior rollback switch.

## Regression and release gates

The module tests cover failed-flush write/delete replay, command-derived clocks,
flush-cohort races, genesis corruption and re-sealing, native Segment store reopen,
zero-wallet HTTP/apply rejection, and apply-failure quarantine. Existing nominal
and protocol assertions remain part of the module suite.

These changes do not claim to finish cluster-wide proposal-ID/intent idempotency,
leader-transfer of the proposal ledger, transaction/GC correctness, complete
snapshot restoration, independent IPFS durability, or production signed-intent and
EVM settlement. Keep those release gates explicit. A three-member deployment still
needs tested leader/follower failure, quorum loss, preserved-store restart,
replacement-member restore, and bounded soak evidence.
