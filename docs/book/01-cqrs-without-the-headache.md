# Chapter 1. CQRS and Event Sourcing Without the Headache

By the end of this chapter you will know what *commands*, *events*, *aggregates* and *projections*
are, why anyone would build software this way, and which parts of the job ReactiveCQRS does for you.
There is no code to type yet. Grab a coffee.

---

## The whiteboard and the diary

Imagine a shopping list on a whiteboard in your kitchen. Someone writes "milk", someone else adds
"bread", somebody wipes "milk" off because they bought it on the way home. At any moment the
whiteboard shows the *current* list. What it cannot tell you is how the list got that way: who
added the bread? Was there milk on it this morning? Did anyone ever write "chocolate" and quietly
rub it out?

Now imagine that instead of a whiteboard, the family keeps a **diary** of the list:

1. Monday 08:10, Alice: *added milk*
2. Monday 12:30, Bob: *added bread*
3. Monday 18:05, Alice: *removed milk*

The diary never changes what was written; it only gets new lines at the bottom. And whenever you
want the current list, you read the diary from the top and replay each line: milk, then bread, then
no milk. Result: bread. Same answer as the whiteboard, plus the whole story.

That is **event sourcing** in one paragraph. Each line of the diary is an **event**: a fact about
something that happened, written in the past tense. The current list is the **state**, and you get
it by replaying the events in order. The diary is the source of truth; the whiteboard is just a
convenient picture you can redraw at any time.

> **Remember:** events are facts about the past. "Milk was added" stays true forever, even after
> someone removes the milk. A removal is a *new* event, not an eraser.

## Commands: asking nicely

Nobody writes in the diary directly. You *ask* for a change: "please add chocolate to the list".
That request is a **command**. Unlike an event, a command can be refused: maybe the list is full,
maybe chocolate is banned until after dinner. Someone has to look at the current list, apply the
house rules and decide.

- If the answer is yes, the decision is written down as one or more events ("chocolate was added").
- If the answer is no, nothing is written, and you are told why.

So the flow is always the same:

```
command  →  decision (look at the current state, apply the rules)  →  events (or a refusal)
```

In ReactiveCQRS, commands are Scala case classes such as `AddItem(...)`, the decision is a function
you write called a **command handler**, and events are case classes such as `ItemAdded("milk")`.

## Aggregates: one diary per thing

A real application does not have one shopping list; it has thousands of shopping carts, one per
customer. Each cart gets its own diary. The thing that owns a diary, and the rules that guard it,
is called an **aggregate**. A shopping cart is an aggregate; so is an invoice, a user account or a
project board.

Each aggregate has:

- an **id**, so you can find its diary (an `AggregateId`, a number),
- a **version**: how many events its diary holds. Think of it as the page number of the last line.
  A new cart starts at version 0; after its first event it is at version 1, and so on,
- a **state**: what you get by replaying its events (for a cart: its name and its items).

Why draw the line around one cart and not around "all carts"? Because the rules of a cart only ever
need that one cart. "No more than five items" can be checked by looking at a single cart. Keeping
aggregates small means many of them can change at the same time without stepping on each other's
toes.

### When two people write at once

Alice opens the cart on her phone, Bob on his laptop. Both see version 3. Alice adds milk; the cart
moves to version 4. Bob, still looking at version 3, adds bread. Should Bob's change go through?
Maybe; maybe the rules would have said something different about version 4. ReactiveCQRS lets you
choose:

- With an ordinary command you say "I am changing version 3". If the cart has moved on meanwhile,
  the command is refused with a *concurrent modification* answer, and you can look again and retry.
  This is called **optimistic locking**: nobody locks anything up front; clashes are noticed when
  they happen.
- With a *concurrent* command you say "whatever version it is now", and the library retries for you
  on a clash.

Chapter 5 shows both.

## CQRS: one model for writing, others for reading

Replaying a diary every time someone wants to look at a cart works, but it is slow if the question
is "show me every cart that contains chocolate". For questions like that you want a table you can
search, with one row per cart, kept up to date.

So you keep **two sides**:

- the **write side** (commands, aggregates, events), where decisions are made and the truth is
  stored;
- the **read side**, where events are turned into whatever shape is easiest to query: a list of
  carts per customer, a count of items per product, a search index.

That split has a name: **CQRS**, Command Query Responsibility Segregation. Big words for a simple
habit: *the model you write with does not have to be the model you read from.*

The pieces of the read side are called **projections**. A projection listens to events and updates
its own tables. Picture a bank: the ledger records every transaction (events), while the banking
app shows you a neat summary of your accounts (a projection of the ledger). If the summary ever got
corrupted, the bank could rebuild it from the ledger. You can do exactly the same with projections
(chapter 10).

> **Remember:** the read side is *eventually* up to date. After a command succeeds, its events reach
> the projections a moment later, usually within milliseconds. When a screen must show your own
> change straight away, there are tools for that (chapters 7 and 8).

## What ReactiveCQRS does for you

Writing all of this by hand is a lot of plumbing: storing events safely, handling two writers at
once, delivering every event to every projection even after a crash, rebuilding read models. The
library does the plumbing; you write the parts that are about your business.

**You write:**

- the aggregate's state (a case class),
- its commands and events (more case classes),
- the command handlers (the rules) and the event handlers (how each event changes the state),
- the projections (how events become read models),
- and, when a job spans several aggregates, sagas (chapter 12).

**ReactiveCQRS takes care of:**

- routing each command to the right aggregate, and making sure two commands can never both write
  the same version of it,
- keeping aggregates in memory while they are busy and putting them to sleep when they are idle,
- storing events in PostgreSQL in one transaction, with the optimistic lock,
- delivering events to projections and sagas *at least once, in order per aggregate*, with each
  event applied *once*, even across crashes and restarts,
- retrying, recording and reporting failures, so nothing gets lost silently,
- rebuilding projections, snapshots for long histories, squashing old history, and running on a
  cluster of several machines.

Here is the whole journey of one command:

```
you ──command──▶ command bus ──▶ aggregate owner ──events──▶ PostgreSQL (events + outbox)
                                       │
                                       └──publish──▶ event bus ──┬──▶ projections ──▶ read models
                                                                 └──▶ sagas
```

- The **command bus** is the front door for one aggregate type (one for carts, one for invoices).
- The **aggregate owner** looks after one aggregate (one cart): it keeps its state, runs your
  command handler and stores the events.
- **PostgreSQL** keeps the events, plus an **outbox**: a to-do list of events that some subscriber
  has not confirmed yet. That is how nothing gets lost when a machine restarts halfway.
- The **event bus** hands the events to everybody who subscribed, and waits for each of them to
  say "got it".

> **Under the hood:** the full tour, with every table and message, is in
> [ARCHITECTURE.md, section 1](../ARCHITECTURE.md).

## Actors in sixty seconds

ReactiveCQRS is built on **Apache Pekko** actors. You will meet them, so here is all you need for
now.

An **actor** is a small worker with a mailbox. You never call its methods; you put a message in its
mailbox, and it handles messages one at a time, in the order they arrived. Because it only ever does
one thing at a time, it can keep its own state without locks.

You talk to actors in two ways:

- **tell**, written `actor ! message`: drop the message in the mailbox and walk away;
- **ask**, written `actor ? message`: drop the message in and get back a `Future` with the answer.

```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(5.seconds)

val answer = commandBus ? AddItem(userId, cartId, AggregateVersion(1), "milk")  // a Future
```

The command bus, the aggregate owners, the event bus, projections and sagas are all actors. Your
command handlers and event handlers are plain functions; the actors call them for you.

## When to use it, and when not to

ReactiveCQRS shines when:

- you need to know **how** data got the way it is: audits, "who changed this?", undo;
- several screens need the same data in **different shapes**;
- the business rules live around **things with an identity** (a cart, an order, a project) that
  change over time;
- you want read models you can **rebuild** when requirements change.

It is probably overkill when:

- the application is mostly simple forms over a table, with no history to keep;
- you need cross-record transactions everywhere ("move money between any two of a million
  accounts, atomically, all the time"). Aggregates are consistent one at a time; work across them
  goes through sagas, which take more care.

## What you learned

- An **event** is a fact in the past tense; the diary of events is the source of truth.
- A **command** is a request that can be refused; a **command handler** decides.
- An **aggregate** is one thing with its own diary, an id, a version and a state.
- Two writers at once are handled with **optimistic locking** (or with concurrent commands that retry).
- **CQRS** keeps a write side and a read side; **projections** build the read side from events.
- ReactiveCQRS does the plumbing (routing, storing, delivering, retrying, rebuilding) on Pekko
  actors and PostgreSQL; you write the business parts.

Next: [chapter 2](02-setting-up.md) gets the tools on your desk.
