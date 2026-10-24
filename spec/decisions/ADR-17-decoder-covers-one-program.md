# ADR-17 — An instruction decoder covers one program/ABI

Status: Accepted

## Context

A Solana instruction is identified by a discriminator — the leading bytes of its data.
The width is a property of the program, not the instruction: Anchor programs use an
8-byte discriminator derived as `sha256("global:<name>")`, native/SPL programs a single
byte. Crucially the derivation is **program-independent** — every Anchor `swap` shares
the same 8-byte discriminator regardless of which program emitted it.

The portal's instruction request carries the discriminator in a width-tagged field
(`d1`/`d2`/`d4`/`d8`). Two constraints hold on the wire, both confirmed against a live
portal: a single request MAY NOT set more than one width (`400`, "filters can't be
specified simultaneously"); and the `instructions` array ORs across its elements, so
multiple widths are expressed as multiple elements. There is no `d0` field — the portal
rejects it as unknown — though the SDK's request type and the ABI-instruction type both
declared one, a dead, `400`-triggering footgun.

The reference decoder mishandled this on both sides. It collapsed all widths to the
first non-empty one through an `else if` chain, so a decoder carrying mixed widths
silently under-fetched. And its local re-check matched the program-id list as a whole
and then the discriminator, so two instructions sharing a discriminator — two Anchor
programs each with a `swap` — both decoded the *same* raw instruction, emitting it under
both output keys with the wrong codec and account layout. The generator compounded it:
same-range programs were merged into one decoder, manufacturing exactly this collision.

The array in `programId` is not the cause. Its legitimate use is several **deployments
of one program** (Token / Token-2022, a redeployed address) sharing one ABI; the
program-independent discriminator identifies the instruction correctly across all of
them.

## Decision

A decoder covers a single program/ABI. From that, one width and mutually distinct
discriminators follow, and the decoder enforces both at construction, refusing with a
coded configuration error (E0003, per ADR-4) when:

- discriminators span more than one width (the portal forbids it, and mixed widths mean
  unrelated programs were combined);
- two instructions share a discriminator (they would decode each other's data);
- any instruction carries no discriminator (with none it matches every instruction of the
  program and decodes foreign data under its key), or more than one (an instruction is
  identified by exactly one — several are a malformed ABI entry).

`programId` stays an array — for the multi-deployment case above, emitted as one
instruction request of the single width over all addresses. The dead `d0` field is
removed from the wire request type and the ABI-instruction type. The `sqd init`
generator groups by program identity (the reference-deployment address, not the display
name) and range, so it emits one decoder per (program, range) and never merges distinct
programs — including two whose names normalise to the same identifier.

## Event decoders

The evm event decoder follows the same rule for logs. An entry matches a log by topic0,
topic count and its indexed-parameter filter, and the decoder re-checks that filter on
the encoded topics its query sent (WP-22). The portal answers the union of every
output's requests (WP-25), so without the re-check a filtered decoder emitted logs that
another output had asked for, and two entries for one event left one empty while the
other received records its filter excludes — reported in a log, not refused.

The decoder refuses at construction, with a coded configuration error (E0004), two
entries that can match one log: the same topic0 and topic count, and no indexed parameter
that both filter on value lists with no value in common. A log goes to one entry only
(WP-24), so the other would silently miss it. Entries with disjoint filters, or with a
different topic count such as ERC-20 and ERC-721 `Transfer`, stay legal. Overlapping
filters belong in separate decoders, where each output receives every log its filter
matches. The refusal is fatal, as for instructions; a logged report would leave the
records split.

## Alternatives considered

*Accommodate mixed widths* by emitting one request per width and letting both streams
arrive — rejected. It treats the symptom (under-fetch) and leaves the disease: within
one decoder, mixed widths still mean unrelated programs, cross-width prefix overlap and
shared-name discriminators make the decode ambiguous no matter what is fetched. The
sound unit is one decoder per program.

*Keep `d0` for forward compatibility* — rejected. The portal returns `400` on it today;
a field that cannot be sent is a trap, not a reserve.

## Consequences

The mixed-program decoders the generator itself could produce — previously a silent
corruption — now fail loudly at startup (REQ-2, ADR-9's fail-at-startup posture). The
Token / Token-2022 multi-deployment configuration remains a single decoder. IB-8 no
longer lists `d0`, and IB-50 registers E0003 and E0004.

INV-24 is **not** mechanically enforced by this decision. `programId` is decoder-level,
so nothing binds an instruction definition to the program it was generated from; the
guards reject only what is visible in the discriminator set, which is a proxy, not a
proof. A decoder over `[JUP, RAY]` with `{ jupSwap: jupiter.swap, rayDeposit:
raydium.deposit }` passes — one width, distinct discriminators — yet Raydium's on-chain
`swap` matches `jupSwap`'s discriminator and decodes under Jupiter's codec. The
collisions that *are* caught are the likely ones (two programs tracked for the same
instruction name derive the same discriminator), so the guard covers the accident this
ADR came from; the general case stays the caller's contract. Closing it means binding
`programId` per instruction — an API change, tracked as GAP-36.
