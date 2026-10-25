# ADR-17 — An instruction decoder covers one program/ABI

Status: Accepted

## Context

A Solana instruction is identified by a discriminator — the leading bytes of its data.
The width is a property of the program, not the instruction: Anchor programs use an
8-byte discriminator derived as `sha256("global:<name>")`, native/SPL programs a single
byte. Crucially the derivation is **program-independent**. By default, every Anchor
`swap` has the same 8-byte discriminator, whatever program emitted it. A program can
set a custom discriminator for an instruction, but the discriminator still does not
contain the program address.

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
  identified by exactly one — several are a malformed ABI entry);
- an instruction is not one of `program.instructions` (a discriminator does not identify
  its program, see the binding below);
- the `programId` of `program` is empty.

The decoder gets its program as the typegen module, `program`, which holds `programId`
and `instructions`. Each instruction of the decoder must be one of `program.instructions`.
The types examine this when the instruction types are different. At construction, the
decoder compares object identity, so the typegen and its generated code stay as they are.

A decoder reads one address, `program.programId`, and sends one instruction request of the
single width for it. Token and Token-2022 are two programs, so they get two decoders. A
second deployment of the same program gets its own decoder, with a copy of the module that
replaces the address (`{ ...module, programId }`).

`programId` without `program` stays, so that old code continues to work, but it is
deprecated. With it, the decoder examines only the discriminator set. When it lists more
than one address, the pipe logs a warning at start.

The dead `d0` field is removed from the wire request type and the ABI-instruction type.
The `sqd init` generator groups by program identity (the reference-deployment address, not
the display name) and range, so it emits one decoder per (program, range) and never merges
distinct programs — including two whose names normalise to the same identifier. It emits
one decoder for each deployment. Each decoder sets `program` with the configured address,
because typegen takes the address from the IDL when the IDL has one, and that address can
be different.

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

*Put `programId` on each instruction in the typegen* — rejected for now. The typegen is a
different package, and the code that users generated before does not change. The
`program` binding works with that code.

*Keep a list of addresses beside `program`* — rejected. The list trusts the caller that
each address uses the ABI of `program`, and the binding is there to remove this trust.
Token-2022, the usual example, is a different program with its own ABI.

*Let one decoder hold several programs* (`programs: [{ program, instructions }]`, with a
match on the pair of address and discriminator) — rejected. It is a larger API change,
and one decoder for each program gives the same result.

## Consequences

The mixed-program decoders the generator itself could produce — previously a silent
corruption — now fail loudly at startup (REQ-2, ADR-9's fail-at-startup posture). IB-8 no
longer lists `d0`, and IB-50 registers E0003 and E0004.

With `program`, the decoder enforces INV-24 for instructions: it reads one address, and
only the instructions of that program get into it. A copy of the module with a different
`programId` is the only way to change the address, and the code shows it.

With the deprecated `programId`, the guards examine only the discriminator set. A decoder
over `[JUP, RAY]` with `{ jupSwap: jupiter.swap, rayDeposit: raydium.deposit }` has one
width and distinct discriminators, so it starts. But on chain, the `swap` of Raydium has
the discriminator of `jupSwap`, and the decoder decodes it with the codec of Jupiter. The
start warning shows this risk when `programId` has more than one address.
