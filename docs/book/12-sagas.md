# Chapter 12. Sagas: Getting Aggregates to Cooperate

By the end of this chapter you will be able to write a saga: a small process that drives a job
across several aggregates, step by step, remembers where it got to across restarts, and undoes its
work when a step fails. We will read the saga of the test domain line by line, follow two of its
runs, and then sketch a saga of our own.

---

## When one aggregate is not enough

An aggregate keeps its own rules, on its own (chapter 1). Some jobs need several of them:

- "create five carts, one for each of the shops we deliver to": five aggregates;
- "reserve the items in stock, then charge the customer": a stock aggregate and a payment aggregate.

No transaction spans them. So how do you get all of it done, or none of it?

Think of a travel agent booking a trip: a flight, a hotel and a car. Each booking is a separate
phone call that can succeed or fail on its own. The agent keeps a checklist of what is booked so
far. When the car rental says no, she does not leave you with a flight and a hotel you cannot use:
she calls the hotel and the airline back and cancels, in reverse order.

That checklist-keeping agent is a **saga** (also called a *process manager*). The calls that undo
earlier work are **compensations**; in ReactiveCQRS they are called **reverts**. A saga sends one
command at a time, writes its checklist to PostgreSQL after every step, and when a step fails, it
reverts what the earlier steps did.

## The parts of a saga

Everything below lives in `io.reactivecqrs.core.saga`, except `SagaStep`, which is in
`io.reactivecqrs.api`.

| Part | What it is |
| --- | --- |
| `SagaActor` | the class you extend; one actor runs every saga of its kind |
| order, `SagaOrder` | the message a caller sends to start a saga |
| internal order, `SagaInternalOrder` | the message a saga hands itself for its next step (a `SagaOrder` is one too) |
| step | one call of your `handleOrder` (or `handleRevert`) with one order |
| `SagaStep(sagaId, step)` | the saga's id and the step's number, given to every step |
| response, `SagaResponse` | what the caller gets back |
| `SagaState` | where progress is stored; `PostgresSagaState` keeps it in the `sagas` table |

A step returns a `Future` of one of three statuses:

- `SagaContinues(order)`: this step is done; the next one runs with `order`.
- `SagaSucceded(response)`: the whole job is done; the caller gets `response`.
- `SagaFailed(response)`: the job failed; the caller gets `response`, and the saga starts reverting.

A revert step returns a `Future` of one of these:

- `SagaRevertContinues(order)`: this much is undone; the next revert step runs with `order`.
- `SagaRevertSucceded`: everything is undone.
- `SagaRevertFailed(exceptions)`: undoing failed.

> **Tip:** mind the spelling. The library writes `SagaSucceded` and `SagaRevertSucceded`, with one
> "e" fewer than the dictionary. Let your editor complete them.

A saga is always in one of three **phases** (`SagaPhase`): `CONTINUES` while it works forward,
`REVERTING` while it undoes, and `ERROR`, which is reserved: the runner never puts a saga there, and
a stored saga found in that phase is not run (a warning is logged at start).

## The test domain's saga: many carts at once

`MultipleCartCreatorSaga` (in `testdomain/.../shoppingcart/`) creates several carts named
"Groceries 1", "Groceries 2" and so on, one per step. If one of them is refused, it deletes the ones
it made. Let us read it from the top.

### The messages

```scala
object MultipleCartCreatorSaga {

  case class CreateMultipleCarts(userId: UserId, cartName: String, cartsCount: Int) extends SagaOrder
  private case class CreateRemainingCarts(userId: UserId, cartName: String, cartsCount: Int,
                                          createdCarts: List[AggregateIdWithVersion]) extends SagaInternalOrder

  trait MultipleCartCreatorSagaResponse extends SagaResponse
  case class CartsCreated(carts: List[AggregateIdWithVersion]) extends MultipleCartCreatorSagaResponse
  case class CartsCreationFailure(exceptions: List[String]) extends MultipleCartCreatorSagaResponse
}
```

`CreateMultipleCarts` is public: callers send it. `CreateRemainingCarts` is private: only the saga
hands it to itself, and it carries the carts made so far. Every order has a `userId`;
`SagaInternalOrder` asks for one.

> **Remember:** the order carries everything the next step, or a revert, will need. The orders are
> what the saga stores; nothing else survives a restart.

### The class

```scala
class MultipleCartCreatorSaga(val state: SagaState, val uidGenerator: ActorRef, shoppingCartCommandBus: ActorRef)
  extends SagaActor {

  import context.dispatcher

  override val name = "MultipleCartCreatorSaga"

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case order: CreateMultipleCarts => handleCreateMultipleCarts(sagaStep, order)
    case order: CreateRemainingCarts => handleCreateRemainingCarts(sagaStep, order)
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case revert: CreateRemainingCarts =>
      revertCreateRemainingCarts(sagaStep, revert.userId, revert.cartName, revert.cartsCount, revert.createdCarts)
  }

  // ... the steps, below
}
```

Every saga gives the runner three values and two functions:

- `state`: where progress is stored.
- `uidGenerator`: the uid generator actor your command buses use (chapter 4). Saga ids come from it.
- `name`: the name of this kind of saga. Its rows in the `sagas` table are found by it.
- `handleOrder(sagaStep)`: a `ReceiveOrder`, that is a
  `PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]`.
- `handleRevert(sagaStep)`: a `ReceiveRevert`, a
  `PartialFunction[SagaInternalOrder, Future[SagaRevertHandlingStatus]]`.

`import context.dispatcher` gives the `Future`s an execution context to run their `map`s on.

### A step

Both orders end up in the same method. It sends one command and turns the answer into a status:

```scala
private def createShoppingCart(sagaStep: SagaStep, userId: UserId, cartName: String, cartsCount: Int,
                               createdCarts: List[AggregateIdWithVersion]) = {
  val currentCartId = createdCarts.size + 1
  (shoppingCartCommandBus ? CreateShoppingCart(Some(sagaStep), userId, cartName + " " + currentCartId))
    .mapTo[CustomCommandResponse[_]]
    .map {
      case c: SuccessResponse if createdCarts.length + 1 < cartsCount =>
        SagaContinues(CreateRemainingCarts(userId, cartName, cartsCount,
          AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
      case c: SuccessResponse =>
        SagaSucceded(CartsCreated(AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
      case c: FailureResponse =>
        SagaFailed(CartsCreationFailure(c.exceptions))
    }
}
```

A cart is made, and more are to come: continue, with the new cart added to the list. The last cart
is made: succeed, with the whole list. The cart is refused: fail. The `?` uses the saga's own
implicit timeout of 60 seconds, from `SagaActor`. `Some(sagaStep)` is the idempotency id of the
command; it gets a section of its own below.

(`handleCreateMultipleCarts` calls this method when `cartsCount > 0`. Otherwise it returns
`Future.successful(SagaSucceded(CartsCreationFailure(List("No carts to create"))))` at once: the
saga ends normally, with nothing to do.)

### The revert

```scala
private def revertCreateRemainingCarts(sagaStep: SagaStep, userId: UserId, cartName: String, cartsCount: Int,
                                       createdCarts: List[AggregateIdWithVersion]) = {
  (shoppingCartCommandBus ? DeleteShoppingCart(Some(sagaStep), userId, createdCarts.head.id, createdCarts.head.version))
    .mapTo[CustomCommandResponse[_]]
    .map {
      case c: SuccessResponse if createdCarts.length > 1 =>
        SagaRevertContinues(CreateRemainingCarts(userId, cartName, cartsCount, createdCarts.tail))
      case c: SuccessResponse =>
        SagaRevertSucceded
      case c: FailureResponse =>
        SagaRevertFailed(c.exceptions)
    }
}
```

One cart per revert step: delete the newest, then continue with the rest of the list.

### A run that succeeds

The caller sends `CreateMultipleCarts(alice, "Groceries", 3)`:

| Step | Order | Command sent | Status |
| --- | --- | --- | --- |
| 0 | `CreateMultipleCarts` | create "Groceries 1" | `SagaContinues(CreateRemainingCarts(..., [cart 1]))` |
| 1 | `CreateRemainingCarts([cart 1])` | create "Groceries 2" | `SagaContinues(CreateRemainingCarts(..., [cart 2, cart 1]))` |
| 2 | `CreateRemainingCarts([cart 2, cart 1])` | create "Groceries 3" | `SagaSucceded(CartsCreated([cart 3, cart 2, cart 1]))` |

After step 2, the caller gets `CartsCreated` with the three carts (newest first), and the saga's
row is deleted.

### A run that fails

The test domain's cart handler has a trap for tests: it refuses any cart whose name ends with
"M 4". So the caller sends `CreateMultipleCarts(alice, "Groceries M", 5)`:

| Step | Phase | Order | What happens |
| --- | --- | --- | --- |
| 0 to 2 | `CONTINUES` | as above | "Groceries M 1" to "Groceries M 3" are created |
| 3 | `CONTINUES` | `CreateRemainingCarts([3, 2, 1])` | "Groceries M 4" is refused: `SagaFailed(CartsCreationFailure(List("Cannot add cart *M 4")))`, and the caller gets that response now |
| 4 | `REVERTING` | `CreateRemainingCarts([3, 2, 1])` | cart 3 is deleted: `SagaRevertContinues(CreateRemainingCarts([2, 1]))` |
| 5 | `REVERTING` | `CreateRemainingCarts([2, 1])` | cart 2 is deleted: `SagaRevertContinues(CreateRemainingCarts([1]))` |
| 6 | `REVERTING` | `CreateRemainingCarts([1])` | cart 1 is deleted: `SagaRevertSucceded`, and the row is deleted |

Look at step 4: the revert starts from the order of the step that failed. That order knows what the
steps before it did (three carts), which is exactly why `CreateRemainingCarts` carries
`createdCarts`. Notice also that the step numbers keep growing while the saga reverts: every revert
step has a `SagaStep` of its own.

## Starting a saga and talking to it

1. **Create the saga state**, once:

   ```scala
   import io.reactivecqrs.core.saga.PostgresSagaState

   val sagaState = new PostgresSagaState(mpjsons, typesNames)
   sagaState.initSchema()
   ```

2. **Start the saga actor**, as `ReactiveTestDomainSpec` does:

   ```scala
   val cartsSaga: ActorRef = system.actorOf(
     Props(new MultipleCartCreatorSaga(sagaState, uidGenerator, carts)), "cartsSaga")
   ```

   It needs no special dispatcher: it runs its database writes on the blocking dispatcher by itself.

3. **Send it orders**, and wait for the response:

   ```scala
   import io.reactivecqrs.core.saga.SagaResponse
   import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.CreateMultipleCarts

   implicit val timeout: Timeout = Timeout(2.minutes) // long enough for the whole saga, not one step

   val answer: Future[SagaResponse] = (cartsSaga ? CreateMultipleCarts(alice, "Groceries", 3)).mapTo[SagaResponse]
   ```

The answer is one of these:

- the response of `SagaSucceded` or of `SagaFailed`: here `CartsCreated(...)` or
  `CartsCreationFailure(...)`;
- `SagaFailureResponse(exceptions)` when a step went wrong in a way it did not plan for (its `Future`
  failed, it threw, or `handleOrder` does not handle the order). The saga then reverts, as for
  `SagaFailed`;
- `SagaFailureResponse(exceptions)` also when the saga could not start: no saga id could be had, or
  it could not be stored. Then nothing was done.

The caller hears nothing about the revert: it was answered when the step failed.

> **Warning:** answers are *at least once*. A saga answers its caller right before it stores the
> write that ends the step. If its runner stops in between (the application restarts, or its node
> goes down), the step runs again later and answers again. An `ask` keeps only the first answer; an
> actor that sends orders and lives on should treat a second answer for the same saga as a repeat.

> **Tip:** a saga that resumes after a restart answers the caller it stored, if that caller still
> exists. An `ask` that ran out of time is gone, and the answer goes to dead letters. For work that
> matters, check the outcome where it lands (the carts, through a projection), not only in the
> answer.

## How progress is stored

The `sagas` table has one row per saga in flight: its name and id, the caller to answer, the phase,
the step number and the current order, as JSON with its class. Every step goes the same way:

1. Your handler runs and returns a status.
2. The runner stores the progress: the next order and step number, or the move to `REVERTING`.
3. The next step runs.

When the job is done (`SagaSucceded`, or a revert that has finished), the row is deleted.

A write that fails, because the database is away for a moment, is tried again: after 100 ms, then
twice as long each time, up to 30 seconds, ten attempts in all. If every attempt fails, the saga
stays at its last stored step until the next start. The rule is the `persistRetries` value of
`SagaActor`, a `Backoff` (from `io.reactivecqrs.core.util`) you can override; this is the default:

```scala
override protected val persistRetries: Backoff = Backoff(100.millis, 30.seconds, maxAttempts = 10)
```

## After a restart

When a saga actor starts, it reads every stored saga of its `name` and continues each one from its
stored step and phase. A saga that was creating carts goes on creating them; one that was reverting
goes on reverting.

The step that was running when the application stopped runs **again**, and its command may already
have been applied. That is the subject of the next section.

> **Warning:** the saga's `name` and its order classes are stored. Do not change the `name` while
> sagas are in flight (their rows would no longer be found), and never give two saga actors the same
> one. Treat order classes like event classes (chapter 6): a saga whose stored order no longer reads
> does not resume (the runner logs it as an error and goes on with the others).
> `StoredDataCheck` (chapter 18) reads every stored order on a copy of your production data.

> **Warning:** `handleOrder` and `handleRevert` run on the saga actor itself, which serves every
> saga of its kind. They should only start the work and return the `Future`: do the waiting inside
> it. And keep no state in the actor's fields: the `Future`'s callbacks run elsewhere, and only the
> orders survive a restart.

## Steps that may run twice: `SagaStep`

Back to the travel agent. Her phone line drops right after the airline says "booked", before she
ticks the box on her checklist. When she calls back, she must not book a second flight, so she
quotes her reference: "booking 42, step 1". The airline answers: "already done, here is your
ticket."

`SagaStep(sagaId, step)` is that reference. Every step gets one, unique to its saga and its step.
Pass it as the idempotency id of the command the step sends; the command is an
`IdempotentCommand[SagaStep]` (chapter 5), as the cart commands of the test domain are:

```scala
case class CreateShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
```

The first time, the command is applied, and its response is stored in the same transaction as its
events. When the same step runs again after a restart, the same key finds that stored response:
the command bus answers with it and applies nothing. For `CreateShoppingCart` that means the same
cart id as the first time. `SagaRecoverySpec` checks exactly this: a saga that created its first
cart, but could not store that it had, resumes after a restart, and each cart exists once.

> **Warning:** idempotency keys are global, across all aggregate types. A second command sent with
> the same `SagaStep` counts as a copy of the first: it is not applied, and it gets the first one's
> stored response. Send one idempotent command per step; a job of two commands is two steps.

> **Remember:** stored responses are kept for 30 days by default (the retention of
> `PostgresCommandResponseState`). A saga that resumes later than that would send its step's command
> afresh.

## A saga of your own: reserve, then charge

Here is the shape of a saga with two different steps. `Stock` and `Payment` are aggregates made up
for this example, with commands like this one (`ReleaseItems` and `ChargePayment` look the same):

```scala
case class ReserveItems(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, items: Int)
  extends ConcurrentCommand[Stock, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
```

```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.saga._
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import scala.concurrent.Future

object CheckoutSaga {
  case class Checkout(userId: UserId, stockId: AggregateId, paymentId: AggregateId, items: Int, amountCents: Long)
    extends SagaOrder
  private case class Charge(userId: UserId, stockId: AggregateId, paymentId: AggregateId, items: Int, amountCents: Long)
    extends SagaInternalOrder

  case class CheckedOut(paymentId: AggregateId) extends SagaResponse
  case class CheckoutRefused(reasons: List[String]) extends SagaResponse
}

class CheckoutSaga(val state: SagaState, val uidGenerator: ActorRef, stock: ActorRef, payments: ActorRef)
  extends SagaActor {
  import CheckoutSaga._
  import context.dispatcher

  override val name = "CheckoutSaga"

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case o: Checkout => // step 1: reserve the items
      send(stock, ReserveItems(Some(sagaStep), o.userId, o.stockId, o.items)).map {
        case Right(_) => SagaContinues(Charge(o.userId, o.stockId, o.paymentId, o.items, o.amountCents))
        case Left(reasons) => SagaFailed(CheckoutRefused(reasons))
      }
    case o: Charge => // step 2: charge the customer
      send(payments, ChargePayment(Some(sagaStep), o.userId, o.paymentId, o.amountCents)).map {
        case Right(_) => SagaSucceded(CheckedOut(o.paymentId))
        case Left(reasons) => SagaFailed(CheckoutRefused(reasons))
      }
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case _: Checkout => Future.successful(SagaRevertSucceded) // step 1 failed: nothing was reserved
    case o: Charge => // step 2 failed: give the reserved items back
      send(stock, ReleaseItems(Some(sagaStep), o.userId, o.stockId, o.items)).map {
        case Right(_) => SagaRevertSucceded
        case Left(reasons) => SagaRevertFailed(reasons)
      }
  }

  // Any answer but a success counts as a refusal, so no answer falls through the match.
  private def send(bus: ActorRef, command: AnyRef): Future[Either[List[String], AggregateId]] =
    (bus ? command).mapTo[CustomCommandResponse[_]].map {
      case SuccessResponse(id, _) => Right(id)
      case FailureResponse(reasons) => Left(reasons)
      case other => Left(List(other.toString))
    }
}
```

Three habits are worth copying:

- **One command per step.** Each step has its own `SagaStep`, so each command has its own key.
- **A revert case for every order a step can fail at,** the first one included, even when there is
  nothing to undo. A revert of an order that `handleRevert` does not handle fails, which the runner
  logs as an error. (The test domain's saga leaves `CreateMultipleCarts` out: when its very first
  cart is refused, that error is logged, with nothing to undo.)
- **Every answer handled.** An answer the `match` does not expect, such as
  `AggregateConcurrentModificationError`, fails the step's `Future`. In a forward step that means a
  `SagaFailureResponse` and a revert; in a revert step, see the next section.

## When a revert fails

If a revert step fails (its `Future` fails, it throws, or it returns `SagaRevertFailed`), the runner
logs an error and removes the saga. Nothing retries a revert, and the caller is not told again.

So make reverts hard to refuse. The test domain's revert deletes each cart at the version it was
created with: if someone added an item to that cart in the meantime, the delete meets a version
conflict, and the revert ends there, leaving the older carts in place. A compensation sent as a
`ConcurrentCommand`, like `ReleaseItems` above, does not trip over a version that moved on.

## Sagas in a cluster

A saga runner must run once in the whole cluster. Start it with `Singletons.start` on every node,
and send orders to the proxy it returns (chapter 17 explains singletons):

```scala
import io.reactivecqrs.core.cluster.Singletons

val cartsSaga: ActorRef = Singletons.start(system, "cartsSaga",
  Props(new MultipleCartCreatorSaga(sagaState, uidGenerator, carts)))
```

On an actor system that is not a cluster, `Singletons.start` simply starts the actor. In a cluster,
orders and responses travel between nodes, so they must be classes the library's serializer can
write and read (chapter 17).

## Sagas of an organization

When the orders of a saga belong to one organization, say so, and a purge of that organization
([chapter 16](16-organizations.md)) removes the saga with the rest. Say `Checkout` above also
carried its shop, as `shop: SpaceId`:

```scala
import io.reactivecqrs.api.id.SpaceId

override protected def spaceOf(order: SagaInternalOrder): Option[SpaceId] = order match {
  case o: Checkout => Some(o.shop)
  case _ => None
}
```

The space is stored with the saga when it starts. `None`, the default, keeps a saga out of purges.

**Coming in 1.0:** a simpler way to write a saga, with the library owning the runner and you
describing the steps. This chapter shows what is built today.

> **Under the hood:** every step result and every write of progress comes back to the saga actor as
> a message, so all decisions are taken on its thread; the writes run on the blocking dispatcher.
> Saga ids come from pools fetched ahead from the uid generator. In a cluster the runner stores its
> caller's address with the node's address, so it answers from whichever node it runs on (D46, F136,
> F137). The full story: [ARCHITECTURE.md, section 9](../ARCHITECTURE.md).

## What you learned

- A saga drives a job across aggregates one step at a time: each step sends one command and returns
  `SagaContinues`, `SagaSucceded` or `SagaFailed`.
- Everything a later step or a revert needs travels in the orders, because the orders are what is
  stored.
- On failure the caller is answered at once, and `handleRevert` undoes the earlier steps, starting
  from the order of the step that failed.
- Progress is stored after every step; after a restart, sagas continue from their stored step, which
  runs again.
- `SagaStep` as the idempotency id makes a repeated step harmless; send one idempotent command per
  step.
- Answers are at least once, and failed reverts are logged, not retried.

Next: [chapter 13](13-snapshots.md) keeps aggregates with long histories fast.
