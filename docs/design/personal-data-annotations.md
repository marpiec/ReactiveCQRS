# Proposal: marking personal data in application classes

Status: **proposal** (18/10/2026, the owner's idea during step 1 of the risk assessment). Not planned yet.

## Why

Step 1 of the risk assessment (F190, F191) made the library keep values out of its logs, failure records and replies:
a failure is described by its classes, SQLState, constraint, table and column, never by the event, state, order or
document it was about. That is safe, but it hides everything, including the values that would help an operator (an
order's status, a cart's item count). And the library still cannot say *which* stored data is personal, which erasure
(chapter 15), the stored data check and an organization's export would like to know.

An annotation on the application's classes could tell the library both.

## The idea

Two annotations, in `api`, both opt-in:

- **`@Loggable`** on a field of an event, root, saga order or document: a value the library may show when it describes
  a failure (`Errors.describe` would print `status=Paid` instead of nothing). An allowlist: a field without it stays
  hidden, so a field added later is hidden until someone decides otherwise.
- **`@PersonalData`** on a field: a value that identifies a person. The library would not show it even with
  `reactivecqrs.log-values=true`, and tools could use it:
  - `StoredDataCheck` reports which stored classes carry personal data, and where (events, snapshots, documents,
    saga state, idempotent responses);
  - the complete rewrite (`HistoryRewriter`) checks after an erasure that no marked field still holds the erased
    value;
  - an organization's export can be made without personal data, for a test copy.

## What it is not

Not the safety mechanism. The default stays "no values" (F190, F191): an unmarked or wrongly marked class must not leak.
The annotations only let a host show more, or check more.

## Open questions

- Scala annotations on case class parameters must be runtime-retained Java annotations with a `@field` meta-annotation
  to be seen by reflection; mpjsons would have to expose them, or the library reads them itself, cached per class.
- Nested values: a `@PersonalData` field of a class used inside another, collections, `Option`.
- The cost of describing a value field by field, in a failure path only.
- Whether `@Loggable` is worth it, or a host-provided `describe` per class is simpler.
