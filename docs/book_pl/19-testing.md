# Rozdział 19. Testowanie domeny

Kod oparty na event sourcingu przyjemnie się testuje: reguły to zwykłe funkcje, historia to zwykłe
dane, a biblioteka ma zamienniki działające w pamięci dla swoich części bazodanowych. Pod koniec tego
rozdziału będziesz testować agregaty na trzech poziomach: ich handlery jako zwykłe funkcje, cały
system w pamięci i całość na prawdziwym PostgreSQL. Zobaczysz też, jak pilnować formatu zapisanych
zdarzeń, jak sprawdzać komunikaty, które przechodzą między węzłami, i jak biblioteka uruchamia klaster
w jednym teście.

---

## Kuchnia testowa z trzema stanowiskami

Dobra szefowa kuchni sprawdza nowe danie na trzy sposoby. Najpierw próbuje samego sosu: to szybkie i
może to zrobić dwadzieścia razy w ciągu godziny. Potem gotuje całe danie na turystycznej kuchence, żeby
zobaczyć, czy wszystko ze sobą współgra. Na koniec, zanim danie trafi do karty, podaje je raz w
prawdziwej kuchni, z prawdziwymi piecami.

Testy domeny w ReactiveCQRS mają te same trzy stanowiska:

| Poziom | Co działa | Czego wymaga | Co wyłapuje |
|---|---|---|---|
| 1. Zwykłe funkcje | twoje handlery poleceń i zdarzeń, wywoływane bezpośrednio | niczego | złe reguły, złe zmiany stanu |
| 2. W pamięci | prawdziwi aktorzy (szyna poleceń, właściciele, szyna zdarzeń, projekcje) na magazynach w pamięci | systemu aktorów | błędy w składaniu systemu, wersjach, współbieżności, idempotencji i w tym, co tworzy projekcja |
| 3. PostgreSQL | wszystko, tak jak na produkcji | lokalnego PostgreSQL | błędy w zapisanych formatach, SQL-u, transakcjach i dostarczaniu przez outbox |

Pisz dużo testów na poziomie 1, sporo na poziomie 2, a na poziomie 3 tyle, żeby objąć każdą ścieżkę,
która dotyka bazy danych. Czwarty poziom, kilka węzłów w jednej JVM, pojawi się pod koniec rozdziału.

## Co biblioteka daje ci do testów

Dwa rodzaje pomocy:

- **W module `core`:** zamienniki działające w pamięci (w tym rozdziale nazywamy je *dublerami*) i
  `RecordingMetrics`. Twoje testy mogą używać ich bezpośrednio.
- **W test kicie** `reactivecqrs-testkit`, osobnym module dla twoich testów:

  ```scala
  libraryDependencies += "io.reactivecqrs" %% "reactivecqrs-testkit" % "1.0.0" % Test
  ```

  Są w nim `TestDatabase` (osobny schemat na każde uruchomienie testów), `TestIds`, `TestClock`,
  `SchemaReference` (wzorcowy test zapisanych formatów), `TestSystems` (systemy aktorów, które na
  życzenie działają jako klaster z jednego węzła), `TestCluster` (kilka węzłów w jednej JVM) i
  `ActorAskSupport` (`askFor`, zapytanie, które czeka na odpowiedź), wszystkie w
  `io.reactivecqrs.testkit`. Test kit nie zależy od żadnego frameworka testowego: jego funkcje
  pomocnicze zwracają wartości albo rzucają wyjątki.

Przykłady poniżej to specy E11 z `examples/src/test/scala/io/reactivecqrs/examples/e11/`; testują
koszyki z E1 z rozdziału 2. Są napisane w stylu feature spec ze ScalaTest 3.2, z matcherami `must`, i
korzystają z TestKitu Pekko (`pekko-testkit`, który dodajesz do projektu samodzielnie). Te, które
potrzebują aktorów, rozszerzają małą klasę bazową, którą piszesz raz:

<!-- snippet: e11/ShopSpec.scala#base -->
```scala
import io.reactivecqrs.testkit.{ActorAskSupport, TestSystems}
import org.apache.pekko.testkit.TestKit
import org.scalatest.featurespec.AnyFeatureSpecLike
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

/** Pekko's TestKit on an actor system of the test kit: a cluster of one when the tests run with
  * -Dreactivecqrs.test.clusterOfOne=true. */
abstract class ShopSpec extends TestKit(TestSystems.newSystem()) with AnyFeatureSpecLike
  with GivenWhenThen with ActorAskSupport with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)
}
```

> **Pamiętaj:** każdy przykład w tym rozdziale to spec, który działa razem z zestawami testów
> biblioteki. Po więcej zajrzyj do własnych speców biblioteki w
> `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/`; tekst podpowiada, który otworzyć.

## Poziom 1: handlery poleceń jako zwykłe funkcje

Twój `AggregateContext` trzyma reguły jako funkcje: `commandHandlers` bierze bieżący stan i daje
funkcję częściową, która zamienia polecenie w wynik. Możesz ją wywołać bez żadnego aktora w zasięgu
wzroku:

<!-- snippet: e11/CartRulesSpec.scala#rules -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.examples.e1.{AddItem, Item, ItemAdded, ShoppingCart, ShoppingCartContext}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class CartRulesSpec extends AnyFeatureSpec with Matchers {

  private val context = new ShoppingCartContext
  private val user = UserId(1)
  private val cartId = AggregateId(1001)

  /** What the handler decides for `command` on `cart`; a decision made in a Future is awaited. */
  private def decide(cart: ShoppingCart, command: AnyRef): GenericCommandResult[Any] =
    context.commandHandlers(cart)(command) match {
      case AsyncCommandResult(future) => Await.result(future, 5.seconds)
      case answered => answered
    }

  private def cartWith(items: String*): ShoppingCart =
    ShoppingCart("Groceries", items.zipWithIndex.map { case (name, i) => Item(i + 1, name) }.toVector)

  Feature("Adding items") {

    Scenario("a cart of four items takes a fifth") {
      decide(cartWith("a", "b", "c", "d"), AddItem(user, cartId, AggregateVersion(5), "e")) mustBe
        CommandSuccess(ItemAdded("e"))
    }

    Scenario("a cart of five items refuses a sixth") {
      decide(cartWith("a", "b", "c", "d", "e"), AddItem(user, cartId, AggregateVersion(6), "f")) mustBe
        CommandFailure("Cannot have more than 5 items in your cart")
    }
  }
}
```

Na co warto zwrócić uwagę:

- **Wersja w poleceniu nie ma tu znaczenia.** Sprawdzanie `expectedVersion` to zadanie właściciela,
  nie handlera; to przetestujesz na poziomie 2.
- **Niektóre handlery odpowiadają później.** Handler może zwrócić `AsyncCommandResult` (`Future`)
  zamiast od razu `CommandSuccess` (rozdział 4). `decide` traktuje oba przypadki tak samo, więc spec
  się nie zmienia, kiedy zmienia się handler.
- **Wyniki porównuje się po wartości.** `CommandSuccess(ItemAdded("e"))` jest równe temu, co zbudował
  handler, więc `mustBe` czyta się jak sama reguła.

> **Wskazówka:** testuj brzegi każdej reguły: ostatnią wartość, która przechodzi, i pierwszą, która
> nie przechodzi. Tutaj: koszyk z czterema produktami przyjmuje piąty, a z pięcioma odmawia szóstego.
> Reguła zapisana jako `> 5` zamiast `>= 5` wpuściłaby szósty produkt, a drugi scenariusz powiedziałby
> o tym na długo przed klientem.

## Poziom 1: handlery zdarzeń i odtwarzanie historii

Handlery zdarzeń to też funkcje: `eventHandlers(userId, timestamp, state)` daje funkcję częściową,
która zamienia zdarzenie w następny stan. Odtworzenie historii to fold, czyli to samo, co robi
właściciel, kiedy czyta agregat.

Ten sam fold sprawdza twoje **zdarzenie squasha** ([rozdział 14](14-squash.md)). Każdy squash
sprawdza, czy handlery zdarzeń, zastosowane do samego zdarzenia squasha, oddają dokładnie ten stan,
który ono zastępuje; jeśli nie, squash zostaje odrzucony. Lepiej dowiedzieć się tego z testu. Koszyki
z E1 nie mają zdarzenia squasha, więc drugi scenariusz bierze listy zakupów z E5, które je mają:

<!-- snippet: e11/ReplaySpec.scala#replay -->
```scala
import java.time.Instant

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.api.{AggregateContext, Event}
import io.reactivecqrs.examples.e1.{Item, ItemAdded, ItemRemoved, ShoppingCart, ShoppingCartContext, ShoppingCartCreated}
import io.reactivecqrs.examples.e5.{ItemWritten, ListCreated, ShoppingListContext}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

class ReplaySpec extends AnyFeatureSpec with Matchers {

  /** The state that `events` make, as an owner makes it when it reads the aggregate. */
  private def replay[R](context: AggregateContext[R], events: Event[R]*): R =
    events.foldLeft(context.initialAggregateRoot) { (state, event) =>
      context.eventHandlers(UserId(1), Instant.EPOCH, state)(event)
    }

  Feature("Replaying a history") {

    Scenario("a cart's history replays to the cart") {
      replay(new ShoppingCartContext, ShoppingCartCreated("Groceries"), ItemAdded("apples"), ItemAdded("oranges"), ItemRemoved(1)) mustBe
        ShoppingCart("Groceries", Vector(Item(2, "oranges")))
    }

    Scenario("a shopping list's squash event gives the list back (E5)") {
      val lists = new ShoppingListContext
      val list = replay(lists, ListCreated("Camping"), ItemWritten("tent"), ItemWritten("stove"))
      replay(lists, lists.squash.get.toEvent(list)) mustBe list
    }
  }
}
```

> **Ostrzeżenie:** fold handlerów zdarzeń nic nie wie o **cofnięciu** ani o **duplikowaniu**. W
> magazynie cofnięcie oznacza wcześniejsze zdarzenia jako anulowane, a odtworzenie je pomija; kopia
> najpierw czyta historię agregatu, z którego powstała. Twoje handlery zdarzeń w ogóle nie mają
> przypadku dla zdarzenia cofnięcia (rozdział 6), więc fold, który na nie trafi, kończy się błędem.
> Cofnięcie i duplikowanie testuj przez szynę poleceń, na poziomie 2 albo 3.

## Poziom 2: dublery w pamięci

Każda bazodanowa część biblioteki ma w `core` swojego dublera działającego w pamięci:

| Dubler | Zastępuje | O czym pamiętać |
|---|---|---|
| `MemoryEventStoreState` (`core.eventstore`) | `PostgresEventStoreState` | trzyma obiekty, nie JSON; nie ma transakcji, którą można by wycofać; odczyty stanu z wybranej chwili kończą się błędem |
| `MemoryCommandResponseState` (`core.commandhandler`) | `PostgresCommandResponseState` | nie jest transakcyjny: odpowiedź zostaje zapisana, nawet gdy późniejszy zapis zdarzeń się nie powiedzie |
| `MemorySubscriptionsState` (`core.projection`) | `PostgresSubscriptionsState` | te same reguły „porównaj i ustaw” dla wersji projekcji; bez transakcji |
| `MemoryDocumentStore[T]`, `MemoryDocumentStoreAutoId[T]` (`core.documentstore`) | `PostgresDocumentStore` | warunki zapytań, kolejność, przesunięcie, limit i `maxRows` działają tak jak w PostgreSQL; każdy dokument zachowuje przestrzeń, z którą zapisano go pierwszy raz, a `clearSpace` usuwa dokumenty przestrzeni |
| `MemoryUidGenerator` (`core.uid`) | `PostgresUidGenerator` | pule po 100 identyfikatorów, od 1001 |
| `MemoryTypesNamesState` (`core.types`) | `PostgresTypesNamesState` | identyfikatory nazw klas, numerowane w kolejności, w jakiej padają pytania |
| `MemoryProjectionRegistry` (`core.rebuild`) | `PostgresProjectionRegistry` | usunięcie budowy sprowadza się do zapomnienia o niej |

Nie ma stanu sag w pamięci: sagi testuj na PostgreSQL (tak robi `SagaRecoverySpec`).

> **Ostrzeżenie:** dublery są dla szybkości, nie dla dowodu. Trzymają obiekty i nigdy nie piszą
> JSON-a, więc klasa zdarzenia albo dokumentu, której mpjsons nie umie odczytać z powrotem, przechodzi
> każdy test w pamięci i wykłada się na PostgreSQL. Nic nie jest wycofywane i żadna procedura SQL nie
> pilnuje wersji. Ważne ścieżki uruchamiaj też na poziomie 3.

## Poziom 2: cały system koszyków w pamięci

Oto kompletny system bez bazy danych: E1 złożony tak jak w rozdziale 2 (generator identyfikatorów,
szyna zdarzeń, szyna poleceń koszyków i lista koszyków), w całości na dublerach. Wzoruje się na
`AggregateOwnerSpec` i `SerialCommandsSpec`:

<!-- snippet: e11/CartsInMemorySpec.scala#memory -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState}
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.MemoryEventStoreState
import io.reactivecqrs.core.projection.MemorySubscriptionsState
import io.reactivecqrs.core.uid.{MemoryUidGenerator, UidGeneratorActor}
import io.reactivecqrs.examples.e1._
import org.apache.pekko.actor.{ActorRef, Props}

import scala.util.Try

class CartsInMemorySpec extends ShopSpec {

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new MemoryUidGenerator, new MemoryUidGenerator, new MemoryUidGenerator)))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  private val carts: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartContext,
    uidGenerator, new MemoryEventStoreState, new MemoryCommandResponseState, eventBus))
  private val cartsList: ActorRef = system.actorOf(Props(new CartsListProjection(
    new MemorySubscriptionsState, new MemoryDocumentStore[CartSummary])))
  eventBus ! RegisterSubscribers(List(cartsList)) // RegisterSubscribers(Nil) when there are none

  private val user = UserId(1)

  private def send(command: AnyRef): CustomCommandResponse[_] = carts.askFor[CustomCommandResponse[_]](command)

  private def newCart(): AggregateId = send(CreateShoppingCart(user, "Groceries")) match {
    case SuccessResponse(cartId, _) => cartId
    case refused => fail("Not created: " + refused)
  }

  Feature("A cart in memory") {

    Scenario("an item added reaches the read model") {
      val cartId = newCart()
      send(AddItem(user, cartId, AggregateVersion(1), "apples")) mustBe SuccessResponse(cartId, AggregateVersion(2))
      val summary = cartsList.askFor[Option[CartSummary]](GetCartSummaryAtLeast(cartId, AggregateVersion(2)))
      summary mustBe Some(CartSummary("Groceries", 1, 2))
    }

    Scenario("the command that expects an old version is refused") {
      Given("a cart with apples in it")
      val cartId = newCart()
      send(AddItem(user, cartId, AggregateVersion(1), "apples")) mustBe SuccessResponse(cartId, AggregateVersion(2))

      When("someone still looking at version 1 adds pears")
      val late = send(AddItem(user, cartId, AggregateVersion(1), "pears"))

      Then("the command is refused, and the cart keeps only the apples")
      late mustBe AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart], AggregateVersion(1), AggregateVersion(2))
      val cart = carts.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId))
      cart.get.aggregateRoot.get.items.map(_.name) mustBe Vector("apples")
    }
  }
}
```

Ten kod jest krótki dzięki dwóm pomocnikom:

- **`ShopSpec`**, klasa bazowa pokazana wyżej: `TestKit` Pekko, do którego domieszano feature spec ze
  ScalaTest, `Given`/`When`/`Then`, matchery `must` i `ActorAskSupport`. `system` to system aktorów
  TestKitu.
- **`askFor[T](message)`** (z `ActorAskSupport`) pyta aktora, czeka na odpowiedź do 10 sekund i
  zwraca ją jako `T`; `askFor[T](message, 3.seconds)` czeka tyle, ile podasz. Odpowiedź innej klasy kończy się
  błędem, którego opis wymienia aktora, komunikat i obie klasy, a odpowiedź `Status.Failure(e)` kończy
  się wyjątkiem `e`. Typ w nawiasach kwadratowych podawaj za każdym razem: to z nim porównywana jest
  odpowiedź.

Zwróć uwagę na `eventBus ! RegisterSubscribers(...)`: szyna nic nie publikuje, dopóki nie pozna
swoich subskrybentów, nawet gdy ich nie ma (`RegisterSubscribers(Nil)`).

Na tym poziomie przetestujesz wszystko, co robi właściciel: sprawdzanie wersji, polecenia
idempotentne, przetwarzanie szeregowe (`SerialCommandsSpec` wysyła naraz 1 000 poleceń do jednego
koszyka i dostaje 1 000 kolejnych wersji), limity czasu (`AggregateOwnerSpec`), cofnięcie i
duplikowanie.

## Poziom 2: projekcja w pojedynkę

Do przetestowania projekcji nie potrzebujesz strony zapisu. Zagraj rolę szyny zdarzeń za pomocą sondy
`TestProbe`: przedstaw się projekcji jako szyna, przekaż jej dostarczenie i zobacz, co odpowie i co
zbuduje. Ten przykład wzoruje się na `ProjectionDeliverySpec` i używa listy koszyków z E1:

<!-- snippet: e11/CartsListAloneSpec.scala#alone -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.EventsBusActor.{AggregatesDelivery, GetSubscriptionRequests, MessageAck, SubscriptionRequests}
import io.reactivecqrs.core.projection.MemorySubscriptionsState
import io.reactivecqrs.examples.e1.{CartSummary, CartsListProjection, GetCartSummary, ShoppingCart}
import org.apache.pekko.actor.Props
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

class CartsListAloneSpec extends ShopSpec {

  Feature("The carts list, fed by hand") {

    Scenario("a new cart's summary appears in the list") {
      val projection = system.actorOf(Props(new CartsListProjection(
        new MemorySubscriptionsState, new MemoryDocumentStore[CartSummary])))
      val bus = TestProbe()
      bus.send(projection, GetSubscriptionRequests(projection))
      bus.expectMsgType[SubscriptionRequests](3.seconds)

      val cartId = AggregateId(1001)
      bus.send(projection, AggregateWithType[ShoppingCart](AggregateType.of[ShoppingCart], cartId,
        AggregateVersion(1), Seq(AggregateVersion(1)), Some(ShoppingCart("Groceries", Vector.empty))))
      bus.expectMsg(3.seconds, MessageAck(projection, cartId, Seq(AggregateVersion(1)), AggregatesDelivery))

      val summary = projection.askFor[Option[CartSummary]](GetCartSummary(cartId))
      summary mustBe Some(CartSummary("Groceries", 0, 1))
    }
  }
}
```

Co się stało: projekcja powiedziała sondzie, co subskrybuje, zastosowała wersję 1 koszyka 1001 (nowy
koszyk, więc jej listener wstawił dokument), potwierdziła ją przez `MessageAck` i odpowiedziała na
zapytanie ze swojego magazynu dokumentów w pamięci.

Ten sam trik sprawdza trudniejsze przypadki: wyślij wersję 3 przed wersją 2, a projekcja poprosi o
uzupełnienie luki (`RequestRepublish`); wyślij wersję 1 dwa razy, a druga zostanie tylko potwierdzona.
Projekcji, która przyjmuje zdarzenia zamiast stanów, wysyłaj `IdentifiableEvents` zamiast
`AggregateWithType`.

## Poziom 3: na PostgreSQL

Prawdziwa baza wyłapuje to, czego dublery nie potrafią: JSON, którego nie da się odczytać z powrotem,
SQL, transakcje, outbox. Specy biblioteki używają lokalnego PostgreSQL z bazą danych `reactivecqrs`,
której użytkownik i hasło też brzmią `reactivecqrs`, oraz `TestDatabase` z test kitu:

- **`TestDatabase.init()`** ustawia pulę połączeń ScalikeJDBC, raz na JVM, na **osobnym schemacie dla
  tego uruchomienia testów** (`test_run_<seconds>_<random>`). Uruchomienie nigdy nie widzi tego, co
  zostawiło poprzednie, i każde tworzy schemat od zera; schematy uruchomień starszych niż sześć godzin
  są usuwane. Żeby zachować stały schemat (i obejrzeć go potem), uruchom testy z
  `-Dreactivecqrs.test.schema=<name>`. Inną bazę wskazujesz przez `-Dreactivecqrs.test.url=<jdbc url>`,
  razem z `-Dreactivecqrs.test.user` i `-Dreactivecqrs.test.password`; niech to będzie baza tylko do
  testów, bo schematy starych uruchomień są w niej usuwane.
- **Zestawy testów działają po kolei**, bo dzielą bazę danych (`Test / parallelExecution := false` i
  jedno zadanie testowe naraz w `build.sbt`).
- **Specy jednego uruchomienia dzielą schemat.** Identyfikatory z pul nigdy się nie powtarzają, więc
  koszyki tworzone poleceniami (jak w E1) same się nie mieszają. Dla polecenia, które samo wybiera
  identyfikator (`CreateShoppingCartWithId` z domeny testowej), `TestIds.aggregate()` daje nowy,
  znacznie powyżej identyfikatorów rozdawanych przez pule.

Oto spec poziomu 3. Uruchamia sklep własną funkcją E1, `BasicSetup.start`, tą samą, którą uruchamia
jego aplikacja (host), więc test obejmuje też składanie systemu, a metryki biblioteki zapisuje przez
`RecordingMetrics`:

<!-- snippet: e11/CartsOnPostgresSpec.scala#postgres -->
```scala
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.examples.e1.{BasicSetup, CartSummary}
import io.reactivecqrs.testkit.TestDatabase
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

class CartsOnPostgresSpec extends ShopSpec {

  TestDatabase.init() // the connection pool, on a schema of its own for this test run

  private val metrics = new RecordingMetrics
  ReactiveMetrics(system).use(metrics)
  private val shop = BasicSetup.start(system) // what the host starts

  Feature("Carts in PostgreSQL") {

    Scenario("a cart is stored, reaches the read model through the outbox, and is measured") {
      implicit val timeout: Timeout = Timeout(30.seconds)
      BasicSetup.fillCart(shop, UserId(1)) mustBe Some(CartSummary("Groceries", 1, 2))
      metrics.counted("reactivecqrs.events.stored", Map("type" -> "ShoppingCart")) mustBe 2L
    }
  }
}
```

- `fillCart` (rozdział 2) pyta listę koszyków opóźnionym zapytaniem (`GetCartSummaryAtLeast`,
  rozdział 8), więc odpowiedź przychodzi dopiero wtedy, gdy zdarzenia przeszły z magazynu przez outbox
  i szynę do projekcji.
- **Czekaj na efekt, nigdy nie śpij.** Dostarczanie, projekcje i outbox są ostatecznie spójne.
  Opóźnione zapytanie czeka w projekcji; `awaitAssert` (z TestKitu Pekko) i `eventually` ze ScalaTest
  powtarzają asercję, aż przejdzie albo skończy się czas. Nie używaj `Thread.sleep`.
- **`RecordingMetrics`** trzyma każdy pomiar w pamięci: `counted(name, tags)` sumuje liczniki,
  `timed(name, tags)` zwraca listę czasów, `gauged(name, tags)` podaje ostatnie wartości wskaźników.
  Podajesz tylko tagi, które cię interesują; pomiary z większą liczbą tagów też pasują. W ten sposób
  `MetricsSpec` sprawdza główne pomiary z [rozdziału 18](18-production.md).

### Testy, które trwają minuty, w milisekundach

Poller outboxu czeka całe minuty, zanim opublikuje coś ponownie ([rozdział 18](18-production.md)).
`TestClock` to `java.time.Clock`, który przesuwa się tylko wtedy, gdy przesunie go test.
`DeliveriesSpec` z E11 daje taki zegar pollerowi, tak jak robi to `OutboxPollerSpec` biblioteki, i
ustawia przeglądy tak daleko od siebie, żeby poller przeglądał outbox tylko wtedy, gdy test mu każe:

<!-- snippet: e11/DeliveriesSpec.scala#clock -->
```scala
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions}
import io.reactivecqrs.testkit.TestClock

val clock = new TestClock()
val poller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
  OutboxPollerOptions(firstPollAfter = 1.hour, pollInterval = 1.hour), clock)))

clock.advance(3.minutes + 1.second)
poller ! OutboxPollerActor.Poll   // polls now, as if 3 minutes had passed
```

## Pilnowanie zapisanych formatów

Zapisane zdarzenia żyją latami i każde musi dać się przeczytać kodem, który napiszesz za rok (dlatego
[rozdział 6](06-events.md) radzi dodać klasę `V2` zamiast zmieniać tę już zapisaną). `SchemaReference`
zamienia tę zasadę w test. Dajesz mu po jednej próbce każdej zapisywanej klasy; porównuje każdą z
referencyjnym plikiem JSON nazwanym tak jak klasa i zgłasza, czego nie da się już przeczytać. Oto
`StoredFormatsSpec` z E11, który pilnuje tego, co zapisują wszystkie przykłady:

<!-- snippet: e11/StoredFormatsSpec.scala#formats -->
```scala
import java.nio.file.Paths

import io.mpjsons.MPJsons
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.examples.e1.{ItemAdded, ItemRemoved, ShoppingCartCreated}
import io.reactivecqrs.examples.e4.CheckoutSaga.{Charge, Checkout}
import io.reactivecqrs.examples.e4.{ItemsReleased, ItemsReserved, PaymentCharged, PaymentOpened, StockOpened}
import io.reactivecqrs.examples.e5.{ChangesUndone, ItemWritten, ListCopied, ListCreated, ListSquashed, ShoppingList}
import io.reactivecqrs.examples.e6.{ConcertScheduled, SeatBooked}
import io.reactivecqrs.examples.e7.{NoteAdded, TicketOpened}
import io.reactivecqrs.examples.e9.{ShopCartOpened, ShopCartSquashed, ShopItemAdded}
import io.reactivecqrs.testkit.SchemaReference
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

class StoredFormatsSpec extends AnyFeatureSpec with Matchers {

  // One JSON file per class, committed next to this spec; the tests run from the build's root.
  private val references = Paths.get("examples", "src", "test", "resources", "schema-reference")

  // A sample of every class the examples store: events, saga orders, and the states of snapshots.
  private val samples: Seq[AnyRef] = Seq(
    ShoppingCartCreated("Groceries"), ItemAdded("apples"), ItemRemoved(1),                            // E1
    StockOpened(10), ItemsReserved(2), ItemsReleased(2), PaymentOpened(10000), PaymentCharged(2500),  // E4
    Checkout(UserId(1), AggregateId(1), AggregateId(2), 2, 2500),
    Charge(UserId(1), AggregateId(1), AggregateId(2), 2, 2500),
    ListCreated("Camping"), ItemWritten("tent"), ChangesUndone(1),                                    // E5
    ListCopied(SpaceId(0), AggregateId(1), AggregateVersion(2)), ListSquashed(SpaceId(0), "Camping", Vector("tent")),
    ShoppingList("Camping", Vector("tent")),
    ConcertScheduled("Jazz night", 60), SeatBooked("customer 1"),                                     // E6
    TicketOpened("Printer jam"), NoteAdded("Paper reloaded"),                                         // E7
    ShopCartOpened(SpaceId(42), "Groceries"), ShopItemAdded("apples"),                                // E9
    ShopCartSquashed(SpaceId(42), "Groceries", Vector("apples")))

  Feature("Stored formats") {

    Scenario("every stored class reads its committed reference, and what it writes now reads back") {
      SchemaReference.verify(new MPJsons, references, samples) mustBe empty
    }
  }
}
```

Klasa przechodzi test, gdy jej plik referencyjny z repozytorium wciąż odczytuje się jako próbka (stare
dane pozostają czytelne), a próbka zapisana teraz odczytuje się jako ona sama (nowe dane są czytelne).
Brak pliku referencyjnego to błąd, chyba że uruchomisz testy z `-Dreactivecqrs.schema.record=true`:
wtedy plik zostanie zapisany (dla `ItemAdded("apples")` zawiera `{"name":"apples"}`). Zatwierdź te
pliki w repozytorium: od tej pory zmiana zapisywanej klasy, która psuje stare dane, zabarwi ten test
na czerwono.

Co musi być na liście próbek:

- **Każda klasa zdarzenia**, łącznie z klasami `V2`.
- **Każde zlecenie sagi**, także wewnętrzne: runner zapisuje zlecenie każdego kroku (rozdział 12).
  Wewnętrznego zlecenia, które saga trzyma jako `private`, nie da się utworzyć w teście; dlatego E4
  trzyma `Charge` jako prywatne w obrębie pakietu.
- **Korzeń każdego kontekstu z migawkami** (rozdział 13): migawka to JSON korzenia.
- **Dodatkowa informacja (info) w odpowiedzi każdego polecenia idempotentnego**, jeśli to twoja własna
  klasa; `SuccessResponse` i `FailureResponse` należą do biblioteki, która pilnuje ich sama.

> **Wskazówka:** dodaj próbkę tego samego dnia, w którym piszesz klasę. Pliki referencyjne to twoja
> umowa z danymi, które już są na produkcji.

## Komunikaty, które przechodzą między węzłami

Jeśli uruchamiasz klaster ([rozdział 17](17-cluster.md)), twoje polecenia, zdarzenia, stany i
odpowiedzi na zapytania podróżują przez serializator klastra. Przepuszczenie ich przez niego tam i z
powrotem to szybki test, czy dadzą radę. Potrzebny jest do tego system aktorów z providerem klastra,
który nie musi dołączać do żadnego klastra; `TestSystems.clusterConfig` to właśnie taka konfiguracja,
przygotowana tak, żeby dzielić JVM z innymi węzłami. Przykład wzoruje się na `ClusterSerializationSpec`
biblioteki:

<!-- snippet: e11/CrossNodeSpec.scala#nodes -->
```scala
import com.typesafe.config.ConfigFactory
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.examples.e1._
import io.reactivecqrs.testkit.TestSystems
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.serialization.{Serialization, SerializationExtension, Serializers}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class CrossNodeSpec extends AnyFeatureSpec with Matchers with BeforeAndAfterAll {

  // An actor system with the cluster provider serializes as between nodes; it need not join a cluster.
  private val node = ActorSystem("serialization", TestSystems.clusterConfig.withFallback(ConfigFactory.load()))
  private val serialization = SerializationExtension(node)

  override def afterAll(): Unit = Await.ready(node.terminate(), 30.seconds)

  private def roundTrip(value: AnyRef): AnyRef =
    Serialization.withTransportInformation(node.asInstanceOf[ExtendedActorSystem]) { () =>
      val serializer = serialization.findSerializerFor(value)
      serialization.deserialize(serializer.toBinary(value), serializer.identifier, Serializers.manifestFor(serializer, value)).get
    }

  Feature("Messages between nodes") {

    Scenario("the carts' commands, events, states and questions cross nodes") {
      Seq(CreateShoppingCart(UserId(1), "Groceries"), AddItem(UserId(1), AggregateId(7), AggregateVersion(3), "pears"),
        RemoveItem(UserId(1), AggregateId(7), AggregateVersion(4), 1), ItemAdded("pears"), ItemRemoved(1),
        ShoppingCart("Groceries", Vector(Item(1, "apples"))), GetCartSummary(AggregateId(7)),
        CartSummary("Groceries", 1, 2)).foreach(message => roundTrip(message) mustBe message)
    }
  }
}
```

Klasa z polem typu `Any`, typu jakiegoś traitu albo `ActorRef` wyłoży się tutaj, na twojej maszynie,
a nie między dwoma węzłami produkcyjnymi.

## Kilka węzłów w jednej JVM

Biblioteka testuje swoje zachowanie w klastrze, uruchamiając w jednym teście kilka systemów aktorów:
każdy to węzeł na lokalnym porcie, a wszystkie pracują na schemacie PostgreSQL danego uruchomienia.
`TestCluster` z test kitu robi to samo dla twoich testów. `ClusterSpec` z E2 uruchamia swoje trzy
węzły tak, z zaimportowanym `io.reactivecqrs.testkit.{TestCluster, TestDatabase}`:

<!-- snippet: e2/ClusterSpec.scala#threeNodes -->
```scala
TestDatabase.init()
private val systems = TestCluster.start(3, name = "shop")
private val nodes = systems.map(system => ClusterSetup.startNode(system)) // what the host starts on a node

override def afterAll(): Unit = TestCluster.stop(systems)
```

- `TestCluster.start(count, extra, appVersion, name)` uruchamia węzły, łączy je w klaster i czeka, aż
  wszystkie będą działać; pierwszy jest najstarszy. `extra` to dodatkowa konfiguracja na wierzchu, na
  przykład `"pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout = 3s"`;
  `name` nadaje nazwę systemom aktorów.
- `TestCluster.join(to, 2, appVersion = _ => "2.0.0")` dodaje później węzły w innej wersji, na
  potrzeby aktualizacji kroczących; `TestCluster.stop(systems)` je wyłącza.
- Konfiguracja każdego węzła to provider klastra na `127.0.0.1` z portem 0, split brain resolver z
  krótkim `stable-after` i ustawienia, dzięki którym kilka węzłów może dzielić jedną JVM
  (`pekko.cluster.jmx.multi-mbeans-in-same-jvm = on`, `pekko.coordinated-shutdown.exit-jvm = off`).

Własny `CartCluster` biblioteki (w źródłach testów modułu testdomain) buduje na każdym z nich
`TestNode`: magazyny, szynę zdarzeń i szynę poleceń koszyków. Dzięki niemu `ShardedOwnersSpec`,
`ClusterDeliverySpec`, `LocalOwnersSpec`, `ClusterMessageSizeSpec` i `RollingUpgradeSpec` sprawdzają
zachowanie opisane w [rozdziale 17](17-cluster.md).

Jest też tani trik: uruchom wszystkie pozostałe specy na **klastrze z jednego węzła**.
`TestSystems.newSystem()` tworzy taki klaster, gdy testy działają z
`-Dreactivecqrs.test.clusterOfOne=true`, a klaster z jednego węzła musi zachowywać się dokładnie tak
jak pojedynczy węzeł; biblioteka uruchamia cały swój zestaw testów na oba sposoby.

## Wzorce warte podkradzenia ze speców testdomain

- **Zatrzymaj polecenie w połowie drogi dzięki obietnicy (promise).** Polecenie `CountWhenReleased`
  z `AggregateOwnerSpec` ma handler, który zwraca `AsyncCommandResult` z `Future`, który test kończy,
  kiedy zechce. W międzyczasie test może wywołać pasywację właściciela, wysłać inne polecenia albo
  poczekać, aż minie limit czasu.
- **Sonda jako subskrybent.** W `DeliveriesSpec` z E11, tak jak w `OutboxPollerSpec` biblioteki,
  jedynym subskrybentem szyny jest `TestProbe`. Sonda sama odpowiada na pytanie szyny,

  <!-- snippet: e11/DeliveriesSpec.scala#probe -->
  ```scala
  val subscriber = TestProbe()
  val bus = system.actorOf(Props(new EventsBusActor()))
  bus ! RegisterSubscribers(Seq(subscriber.ref))
  subscriber.expectMsg(GetSubscriptionRequests(subscriber.ref))
  subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(AggregateType.of[ShoppingCart], subscriber.ref)), subscriber.ref))
  ```

  a po poleceniu na `cartId` wyławia dostarczenie tego koszyka, pomijając dostarczenia koszyków z
  innych speców, i je potwierdza:

  <!-- snippet: e11/DeliveriesSpec.scala#catch -->
  ```scala
  val versions = subscriber.fishForSpecificMessage(10.seconds) {
    case e: IdentifiableEvents[_] if e.aggregateId == cartId => e.events.map(_.version.asInt)
  }
  bus ! MessageAck(subscriber.ref, cartId, versions.map(AggregateVersion(_)), EventsDelivery)
  ```

- **Czekaj na efekt, nigdy nie śpij.** `awaitAssert` i `eventually` kończą się, gdy tylko warunek
  jest spełniony, a gdy nie spełni się nigdy, zawodzą z czytelnym komunikatem.
- **Własne identyfikatory testu.** `TestIds.aggregate()` z poleceniem, które samo wybiera
  identyfikator, oddziela scenariusze od siebie we wspólnym schemacie.
- **`Given`, `When`, `Then`.** Specy mówią, co przygotowują, co robią i czego oczekują, słowami, które
  ktoś z zespołu przeczyta w raporcie z testów.
- **Podczas pracy nad jednym specem uruchamiaj tylko jego:**
  `sbt "testdomain/testOnly *ShardedOwnersSpec"`.

## Co już umiesz

- Testujesz na trzech poziomach: handlery jako zwykłe funkcje, prawdziwych aktorów na dublerach w
  pamięci i całość na PostgreSQL, z dużo większą liczbą testów na pierwszym poziomie niż na ostatnim.
- Handlery poleceń i zdarzeń to funkcje kontekstu: wywołujesz je bezpośrednio, foldem po zdarzeniach
  odtwarzasz historię i sprawdzasz, czy twoje zdarzenie squasha oddaje stan.
- Dublery w pamięci z `core` dają szybkie testy całego systemu, ale nie piszą JSON-a i niczego nie
  wycofują, więc ważne ścieżki działają też na PostgreSQL, na osobnym schemacie dla każdego
  uruchomienia.
- `askFor[T](message)` pyta aktora i sprawdza klasę odpowiedzi; `TestProbe` w roli szyny zdarzeń
  testuje projekcję w pojedynkę; `RecordingMetrics` i `TestClock` pozwalają testować pomiary i
  zależności czasowe liczone w minutach.
- `SchemaReference` pilnuje zapisanych formatów, serializacja tam i z powrotem pilnuje komunikatów
  między węzłami, a `TestCluster` z test kitu uruchamia kilka węzłów w jednym teście.

Dalej: [rozdział 20](20-ephemeral-aggregates.md) opowiada o agregatach, które żyją tylko w
pamięci: o szkicach, które zachowujesz, kiedy okażą się ważne.
