# Rozdział 4. Łączymy wszystko i wysyłamy pierwsze polecenie

Twój koszyk z [rozdziału 3](03-first-aggregate.md) to zestaw zwykłych funkcji, które czekają, aż ktoś
je wywoła. W tym rozdziale zbudujemy tego „kogoś”. Na końcu będziesz mieć mały program, który
uruchamia bibliotekę, tworzy koszyk o nazwie „Zakupy”, wkłada do niego jabłka i pomarańcze, odczytuje
koszyk z powrotem i go wypisuje. Po drodze poznasz aktorów, którzy wykonują całą pracę, i dowiesz się,
co znaczy każda odpowiedź biblioteki.

---

## Kto jest kim

Wyobraź sobie urząd w małym miasteczku. **Automat z numerkami** wydaje kolejne numery. Każdy wydział
ma swoje **okienko**, które przyjmuje sprawy jednego rodzaju. Za okienkiem każda teczka ze sprawą ma
swojego **urzędnika**, który zna ją na pamięć i robi w niej wpisy. **Tablica ogłoszeń** informuje
wszystkich zainteresowanych o tym, co postanowiono. A **nocny stróż** obchodzi budynek i sprawdza,
czy każde ogłoszenie zostało przeczytane.

ReactiveCQRS ma dokładnie te same postacie, tyle że jako aktorów:

| Aktor | W urzędzie | Jego zadanie | Ile ich jest |
|---|---|---|---|
| `UidGeneratorActor` | automat z numerkami | wydaje identyfikatory nowym agregatom, poleceniom i sagom | jeden na system |
| `AggregateCommandBusActor` | okienko | przyjmuje polecenia i pytania dotyczące jednego typu agregatu i przekazuje każde z nich właścicielowi agregatu | jeden na typ agregatu |
| `AggregateOwner` | urzędnik | opiekuje się jednym agregatem: trzyma jego stan, uruchamia twoje handlery, zapisuje zdarzenia | jeden na każdy używany agregat; tworzy ich za ciebie szyna poleceń |
| `EventsBusActor` | tablica ogłoszeń | ogłasza zapisane zdarzenia wszystkim subskrybentom i czeka, aż każdy z nich powie „mam” | jeden na system |
| `OutboxPollerActor` | nocny stróż | zleca ponowne opublikowanie zdarzeń, których subskrybent nie potwierdził | jeden na system |

Tworzysz ich wszystkich poza właścicielami, a każdego dopiero wtedy, gdy istnieje już to, czego on
potrzebuje. Ruszajmy, krok po kroku. („Jeden na system” zmieni się w „jeden na maszynę” albo „jeden w
całym klastrze” w [rozdziale 17](17-cluster.md); na razie mamy jedną maszynę i jeden system aktorów.)

Kod każdego kroku to fragment `FirstProgram` z przykładu E1
(`examples/src/main/scala/io/reactivecqrs/examples/e1/FirstProgram.scala`), który zobaczysz w
całości, razem z importami, na końcu rozdziału.

## Krok 1: baza danych

Dokładnie tak jak w [rozdziale 2](02-setting-up.md): najpierw pula połączeń,

<!-- snippet: e1/FirstProgram.scala#pool -->
```scala
Class.forName("org.postgresql.Driver")
ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
  ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
```

potem tabele:

<!-- snippet: e1/FirstProgram.scala#tables -->
```scala
val mpjsons = new MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
```

Koszyk bez projekcji i bez sag potrzebuje tylko tych trzech. Gdy dodasz projekcje i sagi, dopisz dwa
pozostałe wywołania `initSchema()` z rozdziału 2.

## Krok 2: system aktorów

<!-- snippet: e1/FirstProgram.scala#system -->
```scala
val system = ActorSystem("shop")
```

W tym systemie żyją wszyscy aktorzy biblioteki, a sam system przy starcie wczytuje twój plik
`application.conf` (rozdział 2).

## Krok 3: generator identyfikatorów

Nowe koszyki potrzebują identyfikatorów, a swój identyfikator dostaje też każde polecenie (wraca on
w odpowiedziach o błędach, które poznasz niżej). Rozdaje je generator identyfikatorów:

<!-- snippet: e1/FirstProgram.scala#uids -->
```scala
val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
```

`UidGeneratorActor.postgres(system)` daje ci `Props` generatora, który bierze numery z trzech
sekwencji PostgreSQL: `aggregates_uids_seq`, `commands_uids_seq` i `sagas_uids_seq`. Jeśli ich
brakuje, sam je tworzy, i dlatego w rozdziale 2 nie było dla nich żadnego `initSchema()`. Odczytuje
też `reactivecqrs.installation-number` i pilnuje, żeby identyfikatory agregatów mieściły się w
zakresie twojej instalacji. Wszystko to przygotowuje od razu, więc błędny numer instalacji zatrzyma
program już przy starcie, czyli dokładnie wtedy, kiedy chcesz się o tym dowiedzieć.

Rozpisany w całości, ten sam generator wygląda tak. Tę dłuższą postać zobaczysz w specach biblioteki:

```scala
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import org.apache.pekko.actor.Props

val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
  new PostgresUidGenerator("aggregates_uids_seq"),
  new PostgresUidGenerator("commands_uids_seq"),
  new PostgresUidGenerator("sagas_uids_seq"))), "uidGenerator")
```

Jedyna różnica: rozpisana postać pomija numer instalacji. W aplikacji lepiej używaj
`UidGeneratorActor.postgres(system)`.

Identyfikatory przychodzą w **pulach po 10 000**. Szyna poleceń prosi o pulę przy starcie, a o
następną wtedy, gdy z bieżącej zostaje już tylko jedna piąta, więc polecenie prawie nigdy nie czeka
na identyfikator.

## Krok 4: szyna zdarzeń

<!-- snippet: e1/FirstProgram.scala#eventBus -->
```scala
val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
eventBus ! EventsBusActor.RegisterSubscribers(Nil)
```

Druga linijka znaczy więcej, niż się wydaje. Szyna zdarzeń **niczego nie publikuje, dopóki nie
powiesz jej, kim są jej subskrybenci**, nawet jeśli odpowiedź brzmi „nikt”. Subskrybenci to twoje
projekcje, sagi i listenery, a na razie nie mamy żadnych, więc lista jest pusta: `Nil`. Od
[rozdziału 8](08-projections.md) właśnie tutaj będą trafiać twoje projekcje.

Skąd taka surowość? Zdarzenie, którego nikt nie subskrybuje, zostaje od razu potwierdzone i znika z
outboxu. Gdyby szyna zaczęła publikować, zanim twoje projekcje złożą subskrypcje, pierwsze zdarzenia
po każdym starcie nigdy by do nich nie dotarły. Dlatego szyna czeka, a to, co w tym czasie dostanie,
wstrzymuje u siebie.

> **Ostrzeżenie:** zapomnij o `RegisterSubscribers`, a nic nie wybuchnie z hukiem. Polecenia dalej
> kończą się sukcesem, a ich zdarzenia są bezpieczne w bazie danych, ale szyna wstrzymuje każde z
> nich i co pięć sekund zapisuje w logu ostrzeżenie:
> `Event bus waits for RegisterSubscribers from the host; nothing is published until then`. Żadna
> projekcja nie dowie się o niczym. Wyślij ten komunikat raz, gdy utworzysz już wszystkich
> subskrybentów.

## Krok 5: szyna poleceń

Teraz okienko dla koszyków. Dajesz mu swój kontekst z rozdziału 3 i elementy z kroków od 1 do 4:

<!-- snippet: e1/FirstProgram.scala#carts -->
```scala
val carts = system.actorOf(
  AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
```

`AggregateCommandBusActor(...)` zwraca `Props` szyny poleceń dla typu agregatu z kontekstu, a
`system.actorOf` ją uruchamia. Szyna ma jeszcze trzy parametry z wartościami domyślnymi, które
pasują prawie każdemu:

| Parametr | Domyślnie | Co robi |
|---|---|---|
| `maxInactivityMillis` | 43 200 000 (12 godzin) | Właściciel, który przez tyle czasu nie miał nic do roboty, zostaje zatrzymany. |
| `keepAliveLimit` | 200 | Gdy właścicieli w pamięci jest więcej niż ten limit, zatrzymywani są najdawniej używani spośród tych, którzy od ponad 2 minut nic nie robią. |
| `failedRestoreRetryAfter` | 5 sekund | Gdy agregatu nie da się odczytać z magazynu zdarzeń: jak długo jego właściciel odpowiada tym błędem, zanim kolejne żądanie spróbuje jeszcze raz. |

Szyna szuka właścicieli do zatrzymania najwyżej co 5 minut, przy okazji napływających komunikatów.
Zatrzymany właściciel to żadna strata: następne polecenie albo pytanie o ten koszyk uruchomi nowego,
który odtworzy zdarzenia koszyka (fold z rozdziału 3, tym razem naprawdę).

Drugi typ agregatu to jedna linijka więcej, a cała reszta pozostaje wspólna:

```scala
val invoices = system.actorOf(
  AggregateCommandBusActor(new InvoiceContext, uidGenerator, eventStore, responses, eventBus),
  "invoices")
```

(`InvoiceContext` to kontekst, który trzeba by napisać samodzielnie, tak jak kontekst koszyka.)

> **Pamiętaj:** jedna szyna poleceń na typ agregatu. Wszystkie korzystają z jednego generatora
> identyfikatorów, jednego magazynu zdarzeń, jednego magazynu odpowiedzi i jednej szyny zdarzeń.

## Krok 6: poller outboxu

<!-- snippet: e1/FirstProgram.scala#poller -->
```scala
system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
```

Każde zdarzenie trafia do **outboxu** (tabeli `events_to_publish`) w tej samej transakcji co samo
zdarzenie i znika z niego, gdy odpowie na nie każdy subskrybent. Poller outboxu to ten, który
zauważa, kiedy tak się nie stanie. Co 30 sekund przegląda outbox; dla zdarzenia, które wciąż czeka,
zleca właścicielowi agregatu ponowną publikację po 3 minutach i po 6; po 10 minutach zgłasza w logu,
że zdarzenie nie zostało dostarczone, i odtąd ponawia próby co 30 minut.

Uruchom dokładnie jeden na system i zrób to po szynach poleceń: pierwszy raz zagląda do outboxu
10 sekund po starcie i wtedy potrzebuje już szyn, którym przekaże swoje prośby. (W klastrze jest
jeden na cały klaster; [rozdział 17](17-cluster.md) pokazuje, jak to zrobić.) O tym, co robi, gdy coś
pójdzie nie tak, opowiada [rozdział 9](09-projection-failures.md).

## Wysyłanie polecenia

Wszystko jest na swoim miejscu. Polecenie wysyłasz przez `ask` (rozdział 1), który wkłada je do
skrzynki pocztowej szyny poleceń i daje ci `Future`, w którym pojawi się odpowiedź.

```scala
import com.example.shop.cart._
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(70.seconds)

val alice = UserId(1)

val answer = (carts ? CreateShoppingCart(alice, "Groceries")).mapTo[CustomCommandResponse[_]]
val response = Await.result(answer, timeout.duration)
// response: SuccessResponse(Id(1001),Version(1))
```

`mapTo` określa, jakiej odpowiedzi ma się spodziewać `Future`: na każde polecenie przychodzi
odpowiedź typu `CustomCommandResponse`. Potem `Await.result` blokuje wątek, dopóki odpowiedź nie
nadejdzie. W `main` małego programu to w porządku, ale nigdzie indziej: nigdy wewnątrz aktora, nigdy
w obsłudze żądania HTTP. Prawdziwy kod zostaje przy `Future` (więcej o tym niżej).

Dlaczego 70 sekund? Polecenie może trwać aż do upływu `commandTimeout` swojego kontekstu (domyślnie
60 sekund), zanim biblioteka sama odpowie błędem. Czekaj trochę dłużej, a zawsze dostaniesz
odpowiedź biblioteki, a nie przekroczenie własnego limitu czasu. Tę samą radę daje
[docs/SCALING.md](../SCALING.md) w sekcji 2.

Odpowiedź na `CreateShoppingCart` to `SuccessResponse(aggregateId, aggregateVersion)`: identyfikator,
który dostał nowy koszyk, i jego wersja, czyli 1. Identyfikator nowego koszyka poznasz tylko z tej
odpowiedzi, więc go zachowaj. A wersja to właśnie to, czego oczekuje twoje następne `AddItem`:

```scala
response match {
  case SuccessResponse(cartId, version) =>
    val next = Await.result((carts ? AddItem(alice, cartId, version, "apples")).mapTo[CustomCommandResponse[_]],
      timeout.duration)
    // next: SuccessResponse(Id(1001),Version(2))
  case other =>
    println("No cart: " + other)
}
```

## Co odpowiada biblioteka

Każde polecenie dostaje dokładnie jedną odpowiedź, zawsze jedną z tych (wszystkie są w
`io.reactivecqrs.api`):

| Odpowiedź | Co znaczy |
|---|---|
| `SuccessResponse(aggregateId, aggregateVersion)` | Zdarzenia są zapisane; agregat ma teraz wersję `aggregateVersion`. |
| `CustomSuccessResponse(aggregateId, aggregateVersion, info)` | To samo, a do tego `info`, które twój handler zwrócił w `CommandSuccess(event, info)`. |
| `FailureResponse(exceptions)` | Twój handler powiedział „nie” (`CommandFailure`) i podał powody; albo biblioteka odrzuciła polecenie, tak jak robi to z nowym agregatem, którego pierwsze zdarzenie nie wskazuje przestrzeni (space). |
| `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)` | Twoje `expectedVersion` nie było wersją agregatu: ktoś zmienił go przed tobą albo agregat nie istnieje (wtedy `was` to wersja 0). |
| `CommandRejected(aggregateId, reason)` | Polecenie w ogóle się nie wykonało, ale wysłane ponownie później może się udać. |
| `CommandHandlingError(commandName, errorId, commandId)` | Coś się zepsuło podczas działania twojego handlera: rzucił wyjątek, nie miał `case` dla tego polecenia albo działał dłużej niż limit czasu polecenia; albo nie dało się odczytać agregatu. |
| `EventHandlingError(eventName, errorId, commandId)` | Zapis się nie udał: handler zdarzenia rzucił wyjątek, zdarzenie przekroczyło jakiś limit albo zawiodła baza danych. |

Tylko przy dwóch sukcesach coś zostało zapisane. Przy dwóch ostatnich, czyli błędach, w logu
znajdziesz linię z tym samym `errorId` i pełnym opisem tego, co się stało.
[Rozdział 5](05-command-flavors.md) omawia każdą odpowiedź i to, co z nią zrobić, a
[dodatek B](appendix-b-cheat-sheet.md) zbiera je wszystkie.

Sam `Future` też może zakończyć się niepowodzeniem, i to bez żadnej odpowiedzi: z
`AskTimeoutException`, gdy w twoim limicie czasu nie przyszła odpowiedź, albo z błędem, gdy wyślesz
szynie coś, czego w ogóle nie rozumie, czyli coś, co nie jest poleceniem.

## Odczytanie koszyka

Żeby zobaczyć koszyk w obecnym stanie, zapytaj jego szynę poleceń komunikatem `GetAggregate`:

```scala
import scala.util.Try

// cartId: the id from the answer to CreateShoppingCart
val cart = Await.result((carts ? GetAggregate(cartId)).mapTo[Try[Aggregate[ShoppingCart]]], timeout.duration)
// cart: Success(Aggregate(Id(1001),Version(2),Some(ShoppingCart(Groceries,Vector(Item(1,apples))))))
```

Odpowiedź to `Try` w `Future`, który kończy się sukcesem: `Success` z agregatem albo `Failure`, gdy
nie da się odczytać zdarzeń koszyka (z `AggregateRestoreFailedException`). `Aggregate` ma trzy pola:

| Pole | Typ | Co zawiera |
|---|---|---|
| `id` | `AggregateId` | który to agregat |
| `version` | `AggregateVersion` | ile zdarzeń liczy jego historia |
| `aggregateRoot` | `Option[ShoppingCart]` | stan; `None`, gdy agregat nie istnieje (wersja 0) albo został usunięty |

Właściciel koszyka odpowiada z pamięci, a to ten sam właściciel, który zapisuje zdarzenia koszyka,
więc odpowiedź uwzględnia już twoje ostatnie polecenie: bez czekania i bez „za chwilę będzie”.
Używaj `GetAggregate`, gdy koszyka potrzebuje strona zapisu, na przykład żeby pokazać go przed
zmianą. Do list i wyszukiwania w wielu koszykach używaj projekcji ([rozdział 8](08-projections.md)).
A pytania o przeszłość (starszą wersję, stan w danej chwili, listę zdarzeń) opisuje
[rozdział 7](07-reading-the-past.md).

## Cały program

Oto wszystkie kroki w jednym miejscu: `FirstProgram` z przykładu E1. Tworzy koszyk, dodaje dwa
produkty i wypisuje koszyk. `main` otwiera zasoby programu, czyli pulę połączeń i system aktorów, i na
końcu je zamyka; resztą zajmuje się `run`, a test przykładu E1 uruchamia go na własnej testowej bazie
danych. W twoim projekcie, gdzie koszyk leży we własnym pakiecie, dodaj jego import
(`import com.example.shop.cart._`).

<!-- snippet: e1/FirstProgram.scala#program -->
```scala
import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

object FirstProgram {

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
      ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
    val system = ActorSystem("shop")
    try run(system)
    finally {
      Await.result(system.terminate(), 30.seconds)
      ConnectionPool.closeAll()
    }
  }

  def run(system: ActorSystem): Unit = {
    // 1. The tables.
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

    // 2. The actors.
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    val carts = system.actorOf(
      AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")

    // 3. Talking to the carts.
    implicit val timeout: Timeout = Timeout(70.seconds)
    val alice = UserId(1)

    def send(command: AnyRef): SuccessResponse =
      Await.result((carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
        case success: SuccessResponse => success
        case other => throw new IllegalStateException(command.getClass.getSimpleName + " failed: " + other)
      }

    val created = send(CreateShoppingCart(alice, "Groceries"))
    println("Created: " + created)

    val withApples = send(AddItem(alice, created.aggregateId, created.aggregateVersion, "apples"))
    val withOranges = send(AddItem(alice, created.aggregateId, withApples.aggregateVersion, "oranges"))
    println("Now at: " + withOranges.aggregateVersion)

    val cart = Await.result((carts ? GetAggregate(created.aggregateId)).mapTo[Try[Aggregate[ShoppingCart]]],
      timeout.duration).get
    println("The cart: " + cart)
  }
}
```

Zwróć uwagę, że każde `AddItem` niesie wersję z poprzedniej odpowiedzi. Tak działa blokada
optymistyczna: każde polecenie mówi „zmieniam koszyk taki, jaki był po mojej ostatniej zmianie”.

## Uruchomienie

Program uruchomisz poleceniem `sbt "examples/runMain io.reactivecqrs.examples.e1.FirstProgram"` (we
własnym projekcie: `sbt "runMain ..."` z pełną nazwą swojego obiektu). Wśród linii startowych Pekko
zobaczysz meldunek szyny zdarzeń `Event bus started with 0 subscriptions`, a potem, na świeżej bazie
danych:

```
Created: SuccessResponse(Id(1001),Version(1))
Now at: Version(3)
The cart: Aggregate(Id(1001),Version(3),Some(ShoppingCart(Groceries,Vector(Item(1,apples), Item(2,oranges)))))
```

Skąd 1001? Pierwsze 1 000 identyfikatorów agregatów zostawiono na identyfikatory systemowe, więc
sekwencja zaczyna się od 1001. Uruchom program ponownie, a nowy koszyk dostanie 11001: każdy start
szyny poleceń pobiera świeżą pulę 10 000 identyfikatorów, a resztka poprzedniej puli przepada.
Identyfikatory są unikalne, ale nie kolejne; nigdy nie doszukuj się w nich znaczenia.

A teraz zajrzyj do samego pamiętnika. W `psql`:

```sql
SELECT e.aggregate_id, e.version, t.name AS event_class, e.event
  FROM events e JOIN types_names t ON t.id = e.event_type_id
 ORDER BY e.id;
```

```
 aggregate_id | version |                   event_class                   |        event
--------------+---------+-------------------------------------------------+----------------------
         1001 |       1 | io.reactivecqrs.examples.e1.ShoppingCartCreated | {"name":"Groceries"}
         1001 |       2 | io.reactivecqrs.examples.e1.ItemAdded           | {"name":"apples"}
         1001 |       3 | io.reactivecqrs.examples.e1.ItemAdded           | {"name":"oranges"}
```

Trzy wiersze, trzy fakty. Sam koszyk, czyli wartość `ShoppingCart` wypisana przez program, nie jest
nigdzie przechowywany: ilekroć właściciel go potrzebuje, odtwarza go, przepuszczając te wiersze przez
twoje handlery zdarzeń.

> **Pod maską:** oto droga, którą przechodzi `AddItem`. Szyna poleceń nadaje mu identyfikator
> polecenia ze swojej puli i przekazuje je właścicielowi koszyka (a jeśli żadnego nie ma w pamięci,
> uruchamia nowego, który odtwarza koszyk). Właściciel sprawdza `expectedVersion`, uruchamia twój
> handler polecenia na dispatcherze handlerów, stosuje nowe zdarzenie do kopii stanu i zapisuje je w
> jednej transakcji: procedura składowana jeszcze raz sprawdza wersję i zapisuje razem wiersz w
> `events`, wiersz w `aggregates` i wiersz w outboxie. Potem właściciel publikuje zdarzenie na
> szynie zdarzeń i odpowiada ci. Koszyki nie mają subskrybentów, więc szyna od razu potwierdza
> zdarzenie, a wiersz w outboxie znika chwilę później. Jeśli program zatrzyma się wcześniej, wiersz
> poczeka i zostanie opublikowany ponownie przy następnym starcie, a subskrybenci po prostu zignorują
> go jako powtórkę. Całą historię opowiadają [ARCHITECTURE.md, sekcje 3 i 5](../ARCHITECTURE.md).

## Spróbuj: dwa sposoby, by usłyszeć „nie”

Wprowadź do programu dwie drobne zmiany i obserwuj odpowiedzi.

1. **Wyślij starą wersję.** Daj drugiemu `AddItem` wersję z pierwszej odpowiedzi,
   `created.aggregateVersion`, zamiast `withApples.aggregateVersion`. Koszyk ma wtedy już wersję 2,
   więc program zatrzyma się z komunikatem:

   ```
   AddItem failed: AggregateConcurrentModificationError(Id(...),AggregateType(io.reactivecqrs.examples.e1.ShoppingCart),Version(1),Version(2))
   ```

   Nic nie zostało zapisane, a twój handler nawet się nie uruchomił.

2. **Złam zasadę domową.** Dodaj sześć produktów pod rząd, każdy z wersją z poprzedniej odpowiedzi.
   Przy szóstym dostaniesz `FailureResponse(List(Cannot have more than 5 items in your cart))`: twój
   handler polecenia powiedział „nie”.

## Prawdziwy kod składa Future

`Await` był tylko skrótem na potrzeby małego programu. W prawdziwej aplikacji polecenie przychodzi w
żądaniu HTTP albo w obsłudze komunikatu, a blokowanie wątku na każde polecenie marnowałoby wątki,
których potrzebujesz. Zamiast tego zostań przy `Future` (`scala.concurrent.Future`) i łącz je w
łańcuch, tak jak robi to `ComposedCommands` z przykładu E1:

<!-- snippet: e1/ComposedCommands.scala#compose -->
```scala
import system.dispatcher   // an ExecutionContext for flatMap

def sendAsync(command: AnyRef): Future[SuccessResponse] =
  (carts ? command).mapTo[CustomCommandResponse[_]].flatMap {
    case success: SuccessResponse => Future.successful(success)
    case other => Future.failed(new IllegalStateException(command.getClass.getSimpleName + " failed: " + other))
  }

val groceries: Future[Aggregate[ShoppingCart]] = for {
  created <- sendAsync(CreateShoppingCart(alice, "Groceries"))
  apples  <- sendAsync(AddItem(alice, created.aggregateId, created.aggregateVersion, "apples"))
  _       <- sendAsync(AddItem(alice, created.aggregateId, apples.aggregateVersion, "oranges"))
  cart    <- (carts ? GetAggregate(created.aggregateId)).mapTo[Try[Aggregate[ShoppingCart]]].flatMap(Future.fromTry)
} yield cart
```

Tutaj nic nie czeka: każdy krok rusza, gdy poprzedni dostanie odpowiedź, a twój framework webowy
zamienia końcowy `Future` w odpowiedź na żądanie. Prawdziwa aplikacja zamieniłaby też każdy rodzaj
odpowiedzi w coś lepszego niż wyjątek: przy współbieżnej modyfikacji w prośbę „odśwież, proszę”, przy
`FailureResponse` w powody pokazane użytkownikowi. Jak to zrobić, pokazuje
[rozdział 5](05-command-flavors.md).

## Gdy coś jest nie tak

| Widzisz | Co to znaczy |
|---|---|
| `IllegalStateException: Connection pool is not yet initialized.(name:default)` | Kod biblioteki ruszył przed `ConnectionPool.singleton(...)`. Najpierw utwórz pulę. |
| `Event bus waits for RegisterSubscribers from the host; nothing is published until then`, co pięć sekund | Szyna zdarzeń nigdy nie dostała `RegisterSubscribers`. |
| `AggregateConcurrentModificationError(...)` | Twoje polecenie niosło nieaktualne `expectedVersion`. Odczytaj agregat jeszcze raz i zdecyduj od nowa. |
| `CommandHandlingError(...)`, a w logu `Error handling command ..., errorId: [...]` | Twój handler polecenia rzucił wyjątek albo nie ma `case` dla tego polecenia. Linia logu z tym samym `errorId` zawiera ślad stosu. |

Więcej objawów i sposobów na nie znajdziesz w [dodatku C](appendix-c-troubleshooting.md).

> **Pamiętaj:** w tym rozdziale używamy zwykłego, lokalnego systemu aktorów i z takim działa
> wszystko w części I. Na produkcji biblioteka z założenia uruchamia każde wdrożenie jako klaster
> Pekko, a gdy maszyna jest tylko jedna, jako klaster z jednym węzłem, który zachowuje się dokładnie
> tak jak to, co tu zbudowaliśmy. Jak to zrobić, pokazuje [rozdział 17](17-cluster.md).

## Od teraz: koszyk z domeny testowej

Koszyk zbudowany w rozdziale 3 to odchudzona kopia koszyka z domeny testowej
(`io.reactivecqrs.testdomain.shoppingcart`). Od [rozdziału 5](05-command-flavors.md) przykłady
używają już samego koszyka z domeny testowej, z jego dodatkowymi poleceniami i zdarzeniami. Jego
`CreateShoppingCart` ma z przodu jeszcze jedno pole, `idempotencyId`, więc zobaczysz je wysyłane jako
`CreateShoppingCart(None, alice, "Groceries")`; do czego służy to pole, wyjaśnia rozdział 5.

## Co już umiesz

- Działający system potrzebuje, w tej kolejności: puli połączeń i tabel, `ActorSystem`, generatora
  identyfikatorów (`UidGeneratorActor.postgres(system)`), `EventsBusActor`, jednego
  `AggregateCommandBusActor` na każdy typ agregatu i jednego `OutboxPollerActor`.
- Szyna zdarzeń nic nie publikuje, dopóki nie dostanie `RegisterSubscribers`, nawet z pustą listą.
- Polecenie wysyłasz przez `ask` i dostajesz dokładnie jedną odpowiedź, `CustomCommandResponse`:
  `SuccessResponse(aggregateId, aggregateVersion)`, gdy zdarzenia zostały zapisane, albo którąś z
  odmów i błędów.
- Każde `Command` niesie wersję z poprzedniej odpowiedzi; to właśnie jest blokada optymistyczna.
- `GetAggregate(id)` odpowiada, zwracając `Try[Aggregate[R]]`: identyfikator, wersję i stan, który
  uwzględnia już twoje ostatnie polecenie.
- W prawdziwym kodzie zostań przy `Future` i składaj je ze sobą; `Await` pasuje tylko do małych
  programów.

Dalej: [rozdział 5](05-command-flavors.md) podaje całe menu rodzajów poleceń.
