# Rozdział 19. Testowanie domeny

Kod oparty na zdarzeniach jest przyjemny w testowaniu: reguły są prostymi funkcjami, historia jest prostymi danymi,
a biblioteka dostarcza zamienniki w pamięci dla swoich części bazy danych. Do końca tego rozdziału
przetestuje Twoje agregaty na trzech poziomach: ich obsługi jako zwykłych funkcji, całego systemu w
pamięć i prawdziwy problem w PostgreSQL. Zobaczysz również, jak chronić format przechowywanych danych.
zdarzenia, jak sprawdzać wiadomości przechodzące przez węzły i jak biblioteka uruchamia klaster w jednym węźle
test.

---

## Kuchnia testowa z trzema blatami

Dobry szef kuchni testuje nowe danie na trzy sposoby. Najpierw próbuje sosu na raz: szybko i potrafi
dwadzieścia razy na godzinę. Następnie gotuje całe danie na kuchence turystycznej, żeby sprawdzić, czy wszystkie części działają.
razem. Na koniec podaje ją raz w prawdziwej kuchni, z prawdziwymi piekarnikami, zanim trafi na
menu.

Testy domeny ReactiveCQRS mają te same trzy liczniki:

| Poziom | Co działa | Potrzeby | Łapie |
|---|---|---|---|
| 1. Proste funkcje | twoje polecenia i procedury obsługi zdarzeń, wywoływane bezpośrednio | nic | złe reguły, złe zmiany stanu |
| 2. W pamięci | prawdziwi aktorzy (szyna poleceń, właściciele, szyna zdarzeń, projekcje) w magazynach w pamięci | system aktorów | okablowanie, wersje, współbieżność, idempotentność, co tworzy projekcja |
| 3. PostgreSQL | wszystko, co w produkcji | lokalny PostgreSQL | formaty przechowywane, SQL, transakcje, dostarczanie przez skrzynkę nadawczą |

Napisz wiele testów na poziomie 1, odpowiednią liczbę na poziomie 2 i wystarczającą liczbę na poziomie 3, aby pokryć każdą ścieżkę, którą
dotyka bazy danych. Czwarty poziom, kilka węzłów w jednej wirtualnej maszynie Java, znajduje się na końcu rozdziału.

## Co biblioteka udostępnia do testowania

Dwa rodzaje pomocy:

- **W opublikowanym module `core`:** zastępcze moduły w pamięci (w tym rozdziale nazywamy je *podwójnymi*) i
  `RecordingMetrics`. Twoje testy mogą z nich korzystać bezpośrednio.
- **W module `testutils`:** własny zestaw testowy biblioteki: `CommonSpec`, `ActorAskSupport` (
  `??` helper), `TestDatabase`, `TestIds`, `TestClock` i `SchemaReference`. Jego klasy znajdują się w
  `src/test`, a kompilacja nie publikuje klas testowych (`Test / publishArtifact := false` w
  `project/Common.scala`), więc Twój projekt nie może na nich polegać. Są krótkie (żaden nie jest dużo dłuższy)
  Więcej niż 60 linii): skopiuj te, które Ci się podobają. Zestaw testowy dla aplikacji będzie dostępny w wersji 1.0.

Poniższe przykłady są napisane jako własne specyfikacje biblioteki: Specyfikacje funkcji ScalaTest 3.2 z
„obowiązkowe” narzędzia dopasowujące i zestaw testowy Pekko.

> **Pamiętaj:** każdy przykład w tym rozdziale pochodzi ze specyfikacji lub jest na niej wzorowany.
> `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/`. Aby zobaczyć więcej, otwórz
> specyfikację podaną obok.

## Poziom 1: obsługa poleceń jako zwykłe funkcje

Twój `AggregateContext` przechowuje Twoje reguły jako funkcje: `commandHandlers` przyjmuje bieżący stan i
Zwraca częściową funkcję z polecenia do wyniku. Można ją wywołać bez widocznego aktora:

```scala
importuj io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.testdomain.shoppingcart._
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

klasa CartRulesSpec rozszerza AnyFeatureSpec z Matchers {

  prywatny kontekst wartości = nowy ShoppingCartAggregateContext
  prywatna wartość alice = UserId(1)
  prywatna wartość cartId = AggregateId(1001)

  /** Co decyduje obiekt obsługujący dla `command` na `cart`; oczekiwana jest decyzja podjęta w przyszłości. */
  prywatna def decide(koszyk: Koszyk, polecenie: Any): GenericCommandResult[Any] =
    context.commandHandlers(koszyk)(polecenie) dopasuj {
      przypadek AsyncCommandResult(przyszłość) => Await.result(przyszłość, 5.sekund)
      sprawa rozstrzygnięta => rozstrzygnięta
    }

  prywatna def cartWith(items: String*): ShoppingCart =
    Koszyk("Artykuły spożywcze", items.zipWithIndex.map { case (name, i) => Item(i + 1, name) }.toVector)

  Funkcja("Dodawanie elementów") {

    Scenariusz ("element został dodany za pomocą ItemAdded") {
      zdecyduj(cartWith("mleko"), AddItem(alicja, cartId, AggregateVersion(2), "chleb")) musi być
        CommandSuccess(ItemAdded("bread"))
    }

    Scenariusz ("wózek z sześcioma przedmiotami odmawia przyjęcia siódmego")
      zdecyduj(koszykZ("a", "b", "c", "d", "e", "f"), DodajElement(alicja, identyfikator koszyka, wersja zbiorcza(7), "g")) musi być
        CommandFailure("Nie możesz mieć więcej niż 5 produktów w koszyku")
    }
  }
}
```

Kilka rzeczy, na które należy zwrócić uwagę:

- **Wersja podana w poleceniu nie ma tutaj znaczenia.** Sprawdzenie `expectedVersion` jest zadaniem właściciela,
  nie osoby obsługującej; to testujesz na poziomie 2.
- **Niektóre procedury obsługi odpowiadają później.** Procedura obsługi `AddItem` zwraca `AsyncCommandResult` (typ `Future`),
  `AddItemConcurrently``od razu zamienia się w `CommandSuccess`. `decide` traktuje oba tak samo.
- **Wyniki są porównywane według wartości.** `CommandSuccess(ItemAdded("bread"))` jest równe temu, co zbudował moduł obsługi, więc
  `mustBe` brzmi jak sama reguła.

Czy zauważyłeś niespodziankę? W odmowie napisano „więcej niż 5 przedmiotów”, a test wymaga wózka z *sześcioma*
przedmiotów, aby go zobaczyć. Obsługa odmawia, gdy wózek mieści już więcej niż pięć, więc szósty przedmiot wciąż
wchodzi. Czy to jest zasada, o której mówiłeś, to pytanie do właściciela sklepu; test dwuliniowy to
gdzie takie pytania pojawiają się, na długo zanim klient je znajdzie.

> **Wskazówka:** przetestuj krawędzie każdej reguły: ostatnią wartość, która spełnia warunek, i pierwszą, która nie spełnia warunku. Tutaj:
> pięć pozycji bierze szóstą, sześć pozycji odmawia siódmej.

## Poziom 1: obsługa zdarzeń i odtwarzanie historii

Obsługujące zdarzenia są również funkcjami: `eventHandlers(userId, timestamp, state)` daje częściową funkcję
ze zdarzenia do następnego stanu. Odtwarzanie historii to składanie, to samo, co robi właściciel, gdy
odczytuje agregat:

```scala
importuj java.time.Instant

prywatna def replay(wydarzenia: Wydarzenie[Koszyk]*): Koszyk =
  wydarzenia.foldLeft(kontekst.initialAggregateRoot) { (koszyk, zdarzenie) =>
    context.eventHandlers(alicja, Instant.now, koszyk)(zdarzenie)
  }

Scenariusz ("historia jest odtwarzana w koszyku") {
  odtwarzanie(ShoppingCartCreated("Artykuły spożywcze"), ItemAdded("mleko"), ItemAdded("chleb"), ItemRemoved(1)) musi być
    Koszyk("Artykuły spożywcze", Wektor(Pozycja(2, "chleb"))))
}
```

Ta sama fałda sprawdza twoje zdarzenie **squash** ([rozdział 14](14-squash.md)). Każde zdarzenie squash sprawdza, że
procedury obsługi zdarzeń, stosowane wyłącznie do zdarzenia squash, zwracają dokładnie taki sam stan, jaki zastępują; jeśli
Jeśli nie, dynia jest odrzucana. Zamiast tego sprawdź w teście:

```scala
Scenariusz („wydarzenie squashowe oddaje wózek”) {
  val cart = replay(ShoppingCartCreated("Artykuły spożywcze"), ItemAdded("mleko"), ItemAdded("chleb"))
  replay(context.squash.get.toEvent(cart)) mustBe cart
}
```

> **Ostrzeżenie:** obsługa zdarzeń nie wie nic o **cofnięciu** ani **duplikacji**. W magazynie
> cofnij oznacza wcześniejsze zdarzenia jako anulowane, a przywracanie je pomija, a duplikat odczytuje swoją bazę
> historia najpierw. Obsługa koszyka dla `ShoppingCartChangesUndone` po prostu zwraca koszyk bez zmian,
> więc proste złożenie nadal będzie stosować cofnięte zdarzenia. Przetestuj cofanie i duplikację za pomocą polecenia
> autobus, na poziomie 2 lub 3.

## Poziom 2: podwojenia w pamięci

Każda część bazy danych biblioteki ma w pamięci zmienną double w `core`:

| Podwójne | Zastępuje | Pamiętaj |
|---|---|---|
| `MemoryEventStoreState` (`core.eventstore`) | `PostgresEventStoreState` | przechowuje obiekty, a nie JSON; nie ma transakcji do wycofania; odczyt natychmiast kończy się niepowodzeniem |
| `MemoryCommandResponseState` (`core.commandhandler`) | `PostgresCommandResponseState` | nietransakcyjne: odpowiedź pozostaje zapisana, gdy później utrwalanie zawiedzie |
| `MemorySubscriptionsState` (`core.projection`) | `PostgresSubscriptionsState` | te same reguły porównywania i ustawiania dla wersji projekcji; brak transakcji |
| `MemoryDocumentStore[T]`, `MemoryDocumentStoreAutoId[T]` (`core.documentstore`) | `PostgresDocumentStore` | warunki zapytania odzwierciedlają warunki PostgreSQL; organizacje (spacje) są ignorowane |
| `MemoryUidGenerator` (`core.uid`) | `PostgresUidGenerator` | pule 100 identyfikatorów, od 1001 |
| `MemoryTypesNamesState` (`core.types`) | `PostgresTypesNamesState` | identyfikatory nazw klas, numerowane w miarę pojawiania się pytań |
| `MemoryProjectionRegistry` (`core.rebuild`) | `PostgresProjectionRegistry` | porzucenie kompilacji powoduje jedynie jej zapomnienie |

Brak stanu sagi w pamięci: sagi testowe są wykonywane w systemie PostgreSQL (`SagaRecoverySpec`).

> **Ostrzeżenie:** liczby zmiennoprzecinkowe służą do przyspieszenia, a nie do udowodnienia. Przechowują obiekty i nigdy nie zapisują JSON-a, więc
> klasa zdarzenia lub dokumentu, której mpjson nie może odczytać, przechodzi każdy test w pamięci i kończy się niepowodzeniem
> PostgreSQL. Nic nie jest wycofywane, a żadna procedura SQL nie chroni wersji. Uruchom ścieżki, które
> ma to również znaczenie na poziomie 3.

## Poziom 2: cały system wózków w pamięci

Oto kompletny system bez bazy danych: generator UID, magistrala zdarzeń bez subskrybentów i
magistrala poleceń koszyka, wszystko na liczbach podwójnych. Następuje po `AggregateOwnerSpec` i `SerialCommandsSpec`:

```scala
importuj io.reactivecqrs.api._
importuj io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.MemoryEventStoreState
import io.reactivecqrs.core.uid.{Generator UidPamięci, Aktor Generatora Uid}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.CommonSpec
import org.apache.pekko.actor.{ActorRef, Rekwizyty}

import scala.util.Try

klasa CartInMemorySpec rozszerza CommonSpec {

  prywatna wartość uidGenerator = system.actorOf(Props(nowy UidGeneratorActor(
    nowy MemoryUidGenerator, nowy MemoryUidGenerator, nowy MemoryUidGenerator)))
  prywatna wartość eventBus = system.actorOf(Props(nowy EventsBusActor()))
  eventBus! Zarejestruj subskrybentów (brak)
  prywatne koszyki wartości: ActorRef = system.actorOf(AggregateCommandBusActor(nowy ShoppingCartAggregateContext,
    uidGenerator, nowy MemoryEventStoreState, nowy MemoryCommandResponseState, eventBus))

  prywatna wartość alice = UserId(1)

  Funkcja("Dwie osoby zmieniają jeden wózek") {

    Scenariusz ("polecenie, które oczekuje starej wersji, zostało odrzucone")
      Podano("wózek z mlekiem w środku")
      val created: CustomCommandResponse[_] = carts ?? CreateShoppingCart(None, alice, "Groceries")
      val cartId = created.asInstanceOf[SuccessResponse].aggregateId
      val first: CustomCommandResponse[_] = carts ?? AddItem(alice, cartId, AggregateVersion(1), "milk")
      najpierw musi być SuccessResponse(cartId, AggregateVersion(2))

      Kiedy("ktoś, kto nadal patrzy na wersję 1, dodaje chleb")
      val late: CustomCommandResponse[_] = carts ?? AddItem(alice, cartId, AggregateVersion(1), "bread")

      Następnie („polecenie zostaje odrzucone, a wóz zostaje tylko z mlekiem”)
      późny musi być AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart], AggregateVersion(1), AggregateVersion(2))
      val cart: Try[Aggregate[ShoppingCart]] = carts ?? GetAggregate(cartId)
      cart.get.aggregateRoot.get.items.map(_.name) mustBe Vector("mleko")
    }
  }
}
```

Dwoje pomocników ułatwia sprawę:

- **`CommonSpec`** to zestaw testowy Pekko ze specyfikacją funkcji ScalaTest, `Given`/`When`/`Then`, `must`
  matchers i `ActorAskSupport` są wymieszane. `system` jest systemem aktorów TestKit.
- **`??`** (z `ActorAskSupport`) zadaje pytanie aktorowi i czeka na odpowiedź, do 10 sekund; typ
  oczekujesz, że (`CustomCommandResponse[_]`, `Try[Aggregate[ShoppingCart]]`) to typ, który otrzymasz.
  `askActor[R](message)(timeout)` wykonuje to samo z wybranym przez Ciebie limitem czasu.

Uwaga `eventBus ! RegisterSubscribers(Nil)`: magistrala nie publikuje niczego, dopóki nie zostanie poinformowana o swoim
abonentów, nawet jeśli ich nie ma.

Na tym poziomie możesz testować wszystko, co robi właściciel: sprawdzanie wersji, polecenia idempotentne, połączenia szeregowe
przetwarzanie (`SerialCommandsSpec` wysyła 1000 poleceń na raz do jednego koszyka i otrzymuje 1000 wersji w
wiersz), przekroczenia limitu czasu (`AggregateOwnerSpec`), cofanie i duplikowanie.

## Poziom 2: projekcja sama w sobie

Nie potrzebujesz strony zapisu, aby przetestować projekcję. Odtwórz magistralę zdarzeń samodzielnie za pomocą `TestProbe`:
przedstaw się jako autobus, przekaż projekcji przesyłkę i zobacz, co odpowie i co powie
Wykonano. Następuje po `ProjectionDeliverySpec`, z projekcją listy koszyków domeny testowej:

```scala
importuj io.reactivecqrs.api._
importuj io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.EventsBusActor.{AggregatesDelivery, GetSubscriptionRequests, MessageAck, SubscriptionRequests}
import io.reactivecqrs.core.projection.MemorySubscriptionsState
importuj io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames
importuj org.apache.pekko.actor.Props
zaimportuj org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

Scenario("nazwa nowego koszyka pojawia się na liście") { // w CommonSpec, takim jak powyżej
  projekcja val = system.actorOf(Props(nowy ShoppingCartsListProjectionAggregatesBased(
    nowy MemorySubscriptionsState, nowy MemoryDocumentStore[String])).withDispatcher(BlockingDispatcher.id))
  val bus = TestProbe()
  bus.send(projekcja, GetSubscriptionRequests(projekcja))
  bus.expectMsgType[SubscriptionRequests](3 sekundy)

  val CartId = AggregateId(1001)
  bus.send(projekcja, AggregateWithType[Koszyk](AggregateType.of[Koszyk], cartId,
    AggregateVersion(1), Seq(AggregateVersion(1)), Some(ShoppingCart("Spożywcze", Vector.empty))))
  bus.expectMsg(3.sekundy, MessageAck(projekcja, cartId, Seq(AggregateVersion(1)), AggregatesDelivery))

  val names: Vector[String] = projection ?? GetAllCartsNames()
  nazwy muszą być wektorem Vector("Artykuły spożywcze")
}
```

Co się stało: projekcja przekazała sondzie, do czego się ona stosuje, zastosowała wersję 1 karty 1001
(nowy wózek, więc jego odbiornik wstawił dokument), potwierdził to komunikatem `MessageAck` i odpowiedział
zapytanie z jego pamięci masowej dokumentów.

Ten sam trik sprawdza trudniejsze przypadki: wyślij wersję 3 przed wersją 2, a projekcja poprosi o
gap (`RequestRepublish`); wyślij wersję 1 dwa razy, a druga zostanie tylko potwierdzona. W przypadku projekcji
który przyjmuje zdarzenia zamiast stanów, wysyła `IdentifiableEvents` zamiast `AggregateWithType`.

## Poziom 3: przeciwko PostgreSQL

Prawdziwa baza danych wychwytuje to, czego nie potrafią zrobić podwójne: JSON, którego nie da się odczytać, SQL, transakcje,
Skrzynka nadawcza. Specyfikacja biblioteki wykorzystuje lokalny PostgreSQL z bazą danych `reactivecqrs`, której użytkownik i
hasła to `reactivecqrs` i `TestDatabase` z `testutils`:

- **`TestDatabase.init()`** konfiguruje pulę połączeń ScalikeJDBC raz na maszynę wirtualną Java na **schemacie swojej maszyny wirtualnej Java
  własny dla tego przebiegu testowego** (`test_run_<sekundy>_<losowy>`). Przebieg nigdy nie widzi tego, co pozostało po poprzednim przebiegu.
  z tyłu, a każdy przebieg tworzy schemat od podstaw; schematy przebiegów starszych niż sześć godzin są
  porzucony. Aby zachować stały schemat (aby móc go później obejrzeć), uruchom z
  `-Dreactivecqrs.test.schema=<nazwa>`.
- **Pakiety są uruchamiane pojedynczo**, ponieważ współdzielą bazę danych (`Test / parallelExecution := false` i
  jedno zadanie testowe na raz w `build.sbt`).
- **Specyfikacje jednego przebiegu współdzielą schemat**, więc każdy scenariusz ma własne identyfikatory: `TestIds.aggregate()`
  nadaje nowy identyfikator, znacznie wyższy od identyfikatorów nadawanych przez pule, dla poleceń, które wybierają swój identyfikator
  (`UtwórzKoszykZakupowyZIdentyfikatorem`).

Oto specyfikacja poziomu 3, oparta na „MetricsSpec” i „DeliveryEndToEndSpec”. Rejestruje ona również
metryki biblioteki z `RecordingMetrics`:

```scala
importuj io.mpjsons.MPJsons
importuj io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.core.types.PostgresTypesNamesState
importuj io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase, TestIds}
importuj org.apache.pekko.actor.Props

import scala.concurrent.duration._

Klasa CartsOnPostgresSpec rozszerza CommonSpec {

  TestDatabase.init()

  prywatna wartość mpjsons = nowe MPJsons
  prywatna val typesNames = new PostgresTypesNamesState().initSchema()
  prywatna wartość eventStore = nowy PostgresEventStoreState(mpjsons, typesNames).initSchema()
  prywatne odpowiedzi val = nowy PostgresCommandResponseState(mpjsons, typesNames).initSchema()
  prywatna wartość uidGenerator = system.actorOf(UidGeneratorActor.postgres(system))
  prywatna wartość eventBus = system.actorOf(Props(nowy EventsBusActor()))
  eventBus! Zarejestruj subskrybentów (brak)
  prywatne val carts = system.actorOf(AggregateCommandBusActor(nowy ShoppingCartAggregateContext,
    uidGenerator, eventStore, odpowiedzi, eventBus))

  prywatne metryki wartości = nowe RecordingMetrics
  ReactiveMetrics(system).use(metryki)

  Funkcja("Koszyki w PostgreSQL") {

    Scenariusz ("nowy koszyk jest przechowywany, publikowany i mierzony") {
      val cartId = AggregateId(TestIds.aggregate())
      val created: CustomCommandResponse[_] = carts ?? CreateShoppingCartWithId(None, UserId(1), cartId, "Groceries")
      utworzono mustBe SuccessResponse(cartId, AggregateVersion(1))

      eventStore.readEventsRange[ShoppingCart](cartId, AggregateVersion(1), AggregateVersion(1)).map(_.event) mustBe
        Seq(ShoppingCartCreated("Artykuły spożywcze"))
      waitAssert(eventStore.oldestVersionToPublish(cartId) mustBe None, 5.seconds, 50.millis)
      metrics.counted("reactivecqrs.events.stored", Map("type" -> "ShoppingCart")) musi być >= 1L
    }
  }
}
```

- `eventStore.oldestVersionToPublish(cartId)` ma wartość `None`, gdy w skrzynce nadawczej nie ma niczego, co czekałoby w koszyku:
  jego wydarzenia były dostępne dla wszystkich (w tym przypadku: dla nikogo) i potwierdzane.
- **`awaitAssert`** (z TestKit Pekko) powtarza asercję, aż zostanie ona spełniona lub upłynie czas.
  Dostawa, projekcje i skrzynka nadawcza są ostatecznie spójne: czekaj na nie, nie
  `Thread.sleep`. Funkcja `eventually` w ScalaTest robi to samo.
- **`RecordingMetrics`** przechowuje wszystkie pomiary w pamięci: `counted(nazwa, tagi)` sumuje liczby,
  `timed(nazwa, tagi)` wyświetla czasy, `gauged(nazwa, tagi)` zwraca ostatnie wartości wskaźników. Nazywasz
  tagi, które Cię interesują; miary z większą liczbą tagów również pasują. `MetricsSpec` sprawdza główne miary
  [rozdział 18](18-production.md) tędy.

### Testy trwające minuty, w milisekundach

Program sondujący skrzynkę nadawczą czeka kilka minut przed ponowną publikacją ([rozdział 18](18-production.md)).
`TestClock` to `java.time.Clock`, który przesuwa się tylko wtedy, gdy test go przesuwa; `OutboxPollerSpec` daje
jeden do osoby przeprowadzającej ankietę, przy czym sondaże są przeprowadzane w dużych odstępach czasu, tak aby sonda była przeprowadzana tylko wtedy, gdy zostanie jej wydane polecenie:

```scala
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions}
importuj io.reactivecqrs.testutils.TestClock

val clock = nowy TestClock()
val poller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
  OutboxPollerOptions(firstPollAfter = 1.hour, pollInterval = 1.hour), clock)))

przesunięcie zegara o 3 minuty + 1 sekundę
poller ! OutboxPollerActor.Poll // sonduje teraz, jakby minęły 3 minuty
```

## Ochrona zapisanych formatów

Przechowywane zdarzenia są przechowywane przez lata i każde z nich musi pozostać czytelne dzięki kodowi, który napiszesz w przyszłym roku (
powód [rozdział 6](06-events.md) mówi, że należy dodać klasę `V2` zamiast zmieniać klasę zapisaną).
`SchemaReference` zamienia to w test. Podajesz mu jedną próbkę każdej przechowywanej klasy; porównuje
każdy z plikiem JSON referencyjnym o nazwie odpowiadającej klasie i raportuje, czego już nie odczytuje. To jest
`StoredFormatsSpec`, w skrócie:

```scala
importuj java.nio.file.Paths
importuj io.mpjsons.MPJsons
import io.reactivecqrs.testutils.SchemaReference

klasa StoredFormatsSpec rozszerza AnyFeatureSpec z Matchers {

  prywatna wartość referenceDir = Paths.get("testdomain", "src", "test", "resources", "schema-reference")

  prywatne próbki wartości: Seq[AnyRef] = Seq(
    ShoppingCartCreated("Artykuły spożywcze"), ItemAdded("jabłko"), ItemRemoved(1), CartNameRewritten("Żywność"),
    Odpowiedź sukcesu (AggregateId (1001), AggregateVersion (2)))

  Funkcja("Zapisane formaty pozostają czytelne") {
    Scenariusz("wydarzenia i odpowiedzi poleceń pasują do zatwierdzonych odniesień") {
      SchemaReference.verify(new MPJsons, referenceDir, samples) musi być pusty
    }
  }
}
```

Klasa przechodzi, gdy jej zatwierdzone odniesienie jest nadal odczytywane jako próbka (stare dane pozostają
czytelny) i zapisana próbka teraz odczytuje się jako sama (nowe dane są czytelne). Brakuje odniesienia
jest błędem, chyba że uruchomisz `-Dreactivecqrs.schema.record=true`, który go zapisze (dla
`ItemAdded("apple")` plik zawiera `{"name":"apple"}`). Zatwierdź pliki: od tego momentu zmiana w
przechowywana klasa, która psuje stare dane, powoduje, że ten test staje się czerwony.

> **Wskazówka:** dodaj próbkę każdej nowej klasy zdarzeń, kolejności sagi i zapisanej klasy odpowiedzi w dniu, w którym
> napisz to. Pliki referencyjne to Twój kontrakt z danymi już w produkcji.

## Wiadomości, które przekraczają węzły

Jeśli uruchomisz klaster ([rozdział 17](17-cluster.md)), Twoje polecenia, zdarzenia, stany i odpowiedzi na zapytania
Przechodzą przez serializator klastra. Podróż w obie strony to szybki test, czy potrafią.
`ClusterSerializationSpec` działa w ten sposób, w systemie aktora z dostawcą klastra (potrzeba
nie dołączać do klastra):

```scala
importuj com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.serialization.{Serializacja, Rozszerzenie serializacji, Serializatory}

prywatny węzeł val = ActorSystem("serializacja", ConfigFactory.parseString(
  """pekko.actor.provider = klaster
    |pekko.remote.artery.canonical { nazwa_hosta = "127.0.0.1", port = 0 }""".stripMargin).withFallback(ConfigFactory.load()))
prywatna serializacja wartości = SerializationExtension(węzeł)

prywatna def roundTrip(wartość: AnyRef): AnyRef =
  Serializacja.zInformacjamiTransportowymi(węzeł.asInstanceOf[ExtendedActorSystem]) { () =>
    val serializer = serialization.findSerializerFor(wartość)
    serializacja.deserialize(serializer.toBinary(wartość), serializer.identyfikator, Serializery.manifestFor(serializer, wartość)).get
  }

Scenariusz ("wiadomości wózków przekraczają węzły") {
  Seq(AddItem(UserId(1), AggregateId(7), AggregateVersion(3), "gruszka"), ItemAdded("gruszka"),
    Koszyk("koszyk", Wektor(Przedmiot(1, "jabłko")))).foreach(wiadomość => roundTrip(wiadomość) musi być wiadomością)
}
```

Klasa z polem typu `Any`, cechą lub `ActorRef` tutaj nie działa na Twoim komputerze zamiast
między dwoma węzłami produkcyjnymi.

## Kilka węzłów w jednej maszynie wirtualnej Java

Biblioteka testuje zachowanie klastra, uruchamiając w ramach jednego testu kilka systemów aktorów, z których każdy jest
węzeł na porcie lokalnym, w całym schemacie PostgreSQL uruchomienia. `TestCluster` w domenie testowej
źródła, czy to:

- `TestCluster.start(3, extra)` uruchamia trzy węzły, łączy je i czeka, aż wszystkie będą aktywne; każdy
  `TestNode` zawiera system aktorów węzła, magazyny, magistralę zdarzeń i magistralę poleceń koszyka. `extra` to
  konfiguracja na górze, np. krótki czas bezczynności dla właścicieli partycjonowanych.
- `TestCluster.join(to, 2, appVersion = _ => "2.0.0", ...)` dodaje później węzły innej wersji,
  uaktualnienia ciągłe; `TestCluster.stop(nodes)` wyłącza je.
- Konfiguracja każdego węzła to dostawca klastra na `127.0.0.1` z portem 0, podzielony mózg
  resolver z krótkim `stable-after` i ustawieniami, które pozwalają kilku węzłom na współdzielenie jednej maszyny wirtualnej Java
  (`pekko.cluster.jmx.multi-mbeans-in-same-jvm = on`, `pekko.coordinated-shutdown.exit-jvm = off`).

Dzięki niemu `ShardedOwnersSpec`, `ClusterDeliverySpec`, `LocalOwnersSpec`, `ClusterMessageSizeSpec` i
`RollingUpgradeSpec` sprawdza zachowanie opisane w rozdziale 17 (17-cluster.md). `TestCluster` to test
kod biblioteki, takiej jak `testutils`; wielowęzłowy interfejs dla aplikacji pojawi się w wersji 1.0. Do
Następnie skopiuj pomysł: ma około 120 linijek.

Istnieje również tania sztuczka: uruchom wszystkie pozostałe specyfikacje na **grupie jednej**. Biblioteka
`CommonSpec` wykonuje to, gdy testy są uruchamiane z `-Dreactivecqrs.test.clusterOfOne=true` i klasterem
jeden musi zachowywać się dokładnie tak, jak pojedynczy węzeł.

## Wzorce warte ukradzenia ze specyfikacji domeny testowej

- **Wstrzymaj polecenie w trakcie lotu za pomocą obietnicy.** Polecenie `CountWhenReleased` klasy `AggregateOwnerSpec`
  ma procedurę obsługi zwracającą `AsyncCommandResult` dotyczącą przyszłości, w której test zostanie ukończony w wybranym przez niego momencie.
  W międzyczasie test może sparaliżować właściciela, wysłać inne polecenia lub poczekać na upływ czasu.
- **Sonda jako abonent.** W `OutboxPollerSpec` `TestProbe` jest jedynym abonentem magistrali:
  odpowiada na pytanie samego autobusu, a następnie przechwytuje dostawy, pomijając te z wózków innych specyfikacji:

  ```scala
  val subscriber = TestProbe()
  val bus = system.actorOf(Props(nowy EventsBusActor()))
  autobus ! Zarejestruj subskrybentów (Seq (subscriber.ref))
  subscriber.expectMsg(GetSubscriptionRequests(subscriber.ref))
  subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(AggregateType.of[ShoppingCart], subscriber.ref)), subscriber.ref))

  // ... po poleceniu na cartId:
  val versions = subscriber.fishForSpecificMessage(10.seconds) {
    przypadek e: IdentifiableEvents[_] jeśli e.aggregateId == cartId => e.events.map(_.version.asInt)
  }
  autobus ! MessageAck(subscriber.ref, cartId, versions.map(AggregateVersion(_)), EventsDelivery)
  ```

- **Czekaj na coś, nigdy nie śpij.** `awaitAssert` i `ewentualnie` kończą się, gdy tylko warunek zostanie spełniony
  trzyma i zawodzi z jasnym komunikatem, kiedy nigdy tego nie robi.
- **Identyfikatory własnego testu.** `TestIds.aggregate()` z poleceniem, które wybiera jego identyfikator, zachowuje scenariusze
  oddzielnie w ramach współdzielonego schematu.
- **Dane, Kiedy, Wtedy.** Specyfikacje określają, co konfigurują, co robią i czego oczekują, słowami
  kolega może przeczytać w raporcie z testu.
- **Uruchom jedną specyfikację na raz, podczas gdy nad nią pracujesz:**
  `sbt "testdomain/testOnly *ShardedOwnersSpec"`.

## Czego się nauczyłeś

- Testowanie na trzech poziomach: obsługa jako proste funkcje, prawdziwi aktorzy ponad duplikatami w pamięci i
  wszystko na bazie PostgreSQL, z dużo większą liczbą testów na pierwszym poziomie niż na ostatnim.
- Obsługujące polecenia i zdarzenia są funkcjami kontekstu: wywołuj je bezpośrednio, zwijaj zdarzenia w celu ich odtworzenia
  historię i sprawdź, czy twoje wydarzenie w squasha oddaje stan.
- Podwójne operacje w pamięci w `core` umożliwiają szybkie testy systemowe, ale nie zapisują żadnego JSON-a i nie generują niczego
  z powrotem, więc ważne ścieżki również są uruchamiane w PostgreSQL, według własnego schematu dla każdego uruchomienia.
- `TestProbe` odtwarzający magistralę zdarzeń testuje projekcję samodzielnie; `RecordingMetrics` i
  `TestClock` mierzy pomiary i czasy w minutach.
- `SchemaReference` chroni zapisane formaty, a podróż w obie strony przez serializator chroni wiadomości
  między węzłami, a `TestCluster` pokazuje, jak uruchomić kilka węzłów w jednym teście.

Dalej: [rozdział 20](20-ephemeral-aggregates.md), ostatni, dotyczy agregatów, które żyją tylko w
pamięć: szkice, które zachowujesz, gdy są istotne.