# Writing for "ReactiveCQRS, Step by Step"

How to write and update the chapters of this book. The book is the user's way in: it teaches, step
by step, how to *use* the library. How the library works inside belongs in `../ARCHITECTURE.md`.

## Voice

- **Friendly and light.** Write the way a patient colleague explains things at a whiteboard. Short
  sentences, short paragraphs, "you" for the reader, "we" for the two of you working together.
- **Everyday pictures first, jargon second.** Introduce an idea with something familiar (a diary, a
  bank statement, a shop receipt, a kitchen order ticket), then give it its proper name, then show
  the code. Never the other way round.
- **A little humour is welcome; sarcasm is not.** Never make the reader feel slow.
- **One new idea at a time.** If a section needs two new ideas, split it.
- **Plain English.** Prefer "use" to "leverage", "start" to "instantiate" where the meaning is the same.

## Shape of a chapter

1. An opening paragraph: what you will be able to do by the end, and why you would want to.
2. Sections that each teach one thing: picture, name, code, what happens when you run it.
3. Numbered steps for anything the reader does in order (setting up, wiring, operating).
4. Callouts where they help (see below), not on every page.
5. A closing **"What you learned"** list of three to seven points, and a pointer to the next chapter.

Chapters go from simple to advanced across the book, and inside each chapter too: the common case
first, the corner cases after.

## Callouts

Use exactly these four, as block quotes with a bold label:

> **Tip:** a shortcut or a good habit.

> **Remember:** something to keep in mind after closing the book.

> **Warning:** an easy mistake with an expensive result.

> **Under the hood:** how it works inside; always safe to skip. Point to the right section of
> `../ARCHITECTURE.md` for the full story.

## Examples

- Base examples on the shopping-cart domain in `testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`
  and the specs in `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/`. When a chapter needs
  something the cart does not have, write a small new example in the same spirit (a to-do list, a
  library book loan) and keep it consistent within the chapter.
- **Every class, method, message and configuration key you name must exist in the code.** Check with
  a search before you write it down. If you simplify an example, it must still compile in spirit:
  the right types, the right message names, the right answers.
- Show imports when they are not obvious (`io.reactivecqrs.api._`, `org.apache.pekko.pattern.ask`).
- Keep code blocks short: show the part that matters and say what you left out.
- Show what the reader gets back ("the bus answers `SuccessResponse(cartId, AggregateVersion(2))`").

## Accuracy rules

- Describe only what is built. A feature that is planned but not built yet is mentioned as
  "coming in 1.0" (or later), with no promises about its API.
- Numbers (timings, sizes, throughput) come from `../perf/` or `../SCALING.md`, and say whether they
  were measured (where, when) or estimated.
- Units: kilograms, kilometres, litres, °C, and megabytes for sizes. Dates: DD/MM/YYYY.
- No real people's or customers' data in examples: use made-up names ("Groceries", "Alice").

## Keeping it current

Every change that alters what a user writes or runs updates its chapter in the same commit: a new
option, a new message, a changed default, a new host step. A feature that lands replaces its
"coming" note with a section of its own. The Polish translation in `../book_pl/` follows in the same
commit or the next: same file names, the same code blocks (`BookSnippetsSpec` checks them), and the terms
of its own `STYLE.md`.
