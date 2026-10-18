# ReactiveCQRS, Step by Step

*A friendly guide to building event-sourced Scala applications, one small step at a time.*

---

## Who this book is for

You write Scala. You have heard words like *CQRS*, *event sourcing* and *projections*, maybe nodded
politely in a meeting, and now you have to build something with ReactiveCQRS. Good news: you do not
need to be an expert in any of it. This book starts from zero and adds one idea at a time.

You will need:

- some Scala (case classes, pattern matching, `Future`),
- a little SQL (you will mostly watch the library write it),
- a vague memory of what an actor is. If you have none, that is fine too: chapter 1 explains
  the bits you need.

## How this book is organized

The chapters go from simple to advanced. Read parts I and II in order; after that, jump to whatever
your project needs today.

- **Part I, First steps**: what the library is, how to install it, your first aggregate and your
  first command.
- **Part II, Commands and events in depth**: every kind of command, everything events can do, and
  how to read the past.
- **Part III, Read models**: projections that turn events into data people can query, how they
  recover from failures, how they are rebuilt, and how they push live updates.
- **Part IV, Processes**: sagas, for work that spans several aggregates.
- **Part V, Living with long histories**: snapshots, squash, the audit archive and history rewrites.
- **Part VI, Organizations, clusters and production**: organizations (spaces), running on several
  nodes, operating it all, and testing.
- **Part VII, Scratch paper**: ephemeral aggregates, which live in memory until you keep them.
- **Appendices**: a glossary, a cheat sheet of messages, a troubleshooting guide, the architecture
  in pictures, and every setting in one place.

## Icons used in this book

> **Tip:** a shortcut or a good habit that saves you time.

> **Remember:** something important enough to keep in your head after you close the book.

> **Warning:** a mistake that is easy to make and expensive to fix. Read these twice.

> **Under the hood:** how it works inside. Safe to skip; useful when something surprises you.

## Table of contents

**Part I: First steps**

1. [CQRS and Event Sourcing Without the Headache](01-cqrs-without-the-headache.md)
2. [Setting Up Your Workshop](02-setting-up.md)
3. [Your First Aggregate: a Shopping Cart](03-first-aggregate.md)
4. [Wiring It Up and Sending Your First Command](04-first-command.md)

**Part II: Commands and events in depth**

5. [Commands Come in Flavors](05-command-flavors.md)
6. [Events: Your Single Source of Truth](06-events.md)
7. [Looking Back in Time](07-reading-the-past.md)

**Part III: Read models**

8. [Projections: Views People Can Query](08-projections.md)
9. [When Things Go Wrong in Projections](09-projection-failures.md)
10. [Rebuilding Projections Without Downtime](10-rebuilding-projections.md)
11. [Live Updates with Subscribable Projections](11-subscribable-projections.md)

**Part IV: Processes**

12. [Sagas: Getting Aggregates to Cooperate](12-sagas.md)

**Part V: Living with long histories**

13. [Keeping Aggregates Fast with Snapshots](13-snapshots.md)
14. [Squash: Folding Old History Away](14-squash.md)
15. [Rewriting History, Carefully](15-rewriting-history.md)

**Part VI: Organizations, clusters and production**

16. [Organizations (Spaces)](16-organizations.md)
17. [Going Distributed: Running in a Cluster](17-cluster.md)
18. [Running It in Production](18-production.md)
19. [Testing Your Domain](19-testing.md)

**Part VII: Scratch paper**

20. [Ephemeral Aggregates: Drafts That Live in Memory](20-ephemeral-aggregates.md)

**Appendices**

- A. [Glossary](appendix-a-glossary.md)
- B. [Cheat Sheet: Messages and Answers](appendix-b-cheat-sheet.md)
- C. [Troubleshooting](appendix-c-troubleshooting.md)
- D. [The Architecture in Pictures](appendix-d-architecture.md)
- E. [Configuration Reference](appendix-e-configuration.md)

## Where the examples come from

The code in this book is compiled and run. It comes from three places in this repository:

- **The book's examples, E1 to E11**, in `examples/src/main/scala/io/reactivecqrs/examples/`: small
  programs, each with a `main` you can run and a spec that runs the same code.
- **The test domain's shopping cart**, in `testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`,
  which chapters 5 to 18 use. It is the cart the library's own specs run.
- **The specs of those chapters**, in `examples/src/test/scala/io/reactivecqrs/examples/cart/`: each
  block that sends something is a region of a spec, right before the lines that check what the
  chapter says it answers.

A block taken from one of them is marked in the chapter's source with a comment that names its file
and region, and `BookSnippetsSpec` checks that every such block is that region, word for word. The
few blocks without a mark show one call or a configuration line, or code the chapter presents as
hypothetical, or wrong on purpose.

## The version this book describes

ReactiveCQRS **1.0** (currently `1.0.0-SNAPSHOT`), on Scala 2.13, Apache Pekko 1.7, Java 21 and
PostgreSQL. Every feature the book shows is built and tested; where a part of 1.0's verification is
still to come (tests that kill a node or cut the network, [chapter 17](17-cluster.md)), the chapter
says so plainly.

For maintainers: how the library works inside is in [`../ARCHITECTURE.md`](../ARCHITECTURE.md);
known problems and decisions are in [`../KNOWN-ISSUES.md`](../KNOWN-ISSUES.md); behaviour under
heavy load is in [`../SCALING.md`](../SCALING.md). How to write for this book is in
[`STYLE.md`](STYLE.md).
