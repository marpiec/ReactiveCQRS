# Rozdział 4. Podłączanie i wysyłanie pierwszego polecenia

Twój koszyk z [rozdziału 3](03-first-aggregate.md) to zbiór prostych funkcji, czekających na kogoś, kto go użyje.
Nazwij ich. W tym rozdziale zbudujemy tę osobę. Na koniec będziesz mieć mały program, który
uruchamia bibliotekę, tworzy koszyk o nazwie „Zakupy”, wkłada do niego jabłka i pomarańcze, odczytuje koszyk
z powrotem i wydrukuje. Po drodze spotkasz aktorów, którzy wykonują pracę i dowiesz się, co każdy z nich
odpowiedź biblioteki oznacza.

---

## Kto jest kim

Wyobraź sobie mały ratusz. **Automat biletowy** wydaje numery. Każdy wydział ma **przedni
Biurko**, które przyjmuje prośby o rozpatrzenie swojej sprawy. Za biurkiem każda teczka ma swojego **urzędnika**
kto zna ten plik na pamięć i wpisuje do niego. **Tablica ogłoszeń** ogłasza, co zostało ustalone
Kogo to obchodzi. A **nocny portier** chodzi i sprawdza, czy wszystkie ogłoszenia zostały przeczytane.

ReactiveCQRS ma dokładnie takie osoby jako aktorów:

| Aktor | W ratuszu | Jego praca | Ile |
|---|---|---|---|
| `UidGeneratorActor` | automat biletowy | rozdaje identyfikatory dla nowych agregatów, poleceń i sag | jeden na system |
| `AggregateCommandBusActor` | recepcja | przyjmuje polecenia i pytania dla jednego typu agregatu i przekazuje każde z nich właścicielowi agregatu | po jednym na typ agregatu |
| `AggregateOwner` | urzędnik | opiekuje się jednym agregatem: utrzymuje jego stan, uruchamia procedury obsługi, przechowuje zdarzenia | po jednym na używany agregat; magistrala poleceń tworzy je za Ciebie |
| `EventsBusActor` | tablica ogłoszeń | ogłasza zapisane zdarzenia każdemu subskrybentowi i czeka, aż każdy z nich powie „mam” | jedno na system |
| `OutboxPollerActor` | portier nocny | zdarzenia są ponownie publikowane, gdy subskrybent ich nie potwierdził | jedno na system |

Tworzysz je wszystkie oprócz właścicieli, każdy po potrzebnych elementach. Chodźmy, krok po kroku.
raz. („Jeden na system” staje się „jednym na maszynę” lub „jednym w całym klastrze” w
[rozdział 17](17-cluster.md); na razie mamy jedną maszynę i jeden system aktora.)

## Krok 1: baza danych

Dokładnie tak jak w [rozdziale 2](02-setting-up.md): najpierw pula połączeń, potem tabele.

```scala
importuj io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import scalikejdbc.{PołączeniowyPula, UstawieniaPołączeniowegoPula}

Klasa.forName("org.postgresql.Driver")
ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
  Ustawienia puli połączeń (rozmiar początkowy = 5, rozmiar maksymalny = 32, limit czasu połączenia w milionach (connectionTimeoutMillis) = 3000L))

val mpjsons = nowy MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = nowy PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
```

Wózek bez projekcji i sag potrzebuje tylko tych trzech. Po dodaniu projekcji i sag,
dodaj dwa pozostałe wywołania `initSchema()` z rozdziału 2.

## Krok 2: system aktorów

```scala
zaimportuj org.apache.pekko.actor.ActorSystem

val system = ActorSystem("sklep")
```

Każdy aktor biblioteki znajduje się w tym systemie i odczytuje on Twój plik `application.conf` (rozdział 2)
kiedy się zacznie.

## Krok 3: generator uid

Nowe wózki potrzebują identyfikatorów, a każde polecenie również je otrzymuje (pojawiają się one w odpowiedziach na błędy, które napotkasz)
poniżej). Generator uid je rozdaje:

```scala
importuj io.reactivecqrs.core.uid.UidGeneratorActor

val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
```

`UidGeneratorActor.postgres(system)` podaje `Rekwizyty` generatora, który pobiera swoje liczby
z trzech sekwencji PostgreSQL: `aggregates_uids_seq`, `commands_uids_seq` i `sagas_uids_seq`.
Tworzy je, jeśli ich brakuje, dlatego w rozdziale 2 nie było dla nich funkcji `initSchema()`.
odczytuje `reactivecqrs.installation-number` i przechowuje zbiorcze identyfikatory w zakresie Twojej instalacji.
I od razu wszystko to konfiguruje, więc błędny numer instalacji zatrzymuje program na starcie,
a to jest dokładnie ten moment, kiedy chcesz o tym usłyszeć.

W zapisie ten sam generator wygląda tak. Zobaczysz tę dłuższą formę w bibliotece.
okular:

```scala
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
importuj org.apache.pekko.actor.Props

val uidGenerator = system.actorOf(Props(nowy UidGeneratorActor(
  nowy PostgresUidGenerator("aggregates_uids_seq"),
  nowy PostgresUidGenerator("commands_uids_seq"),
  nowy PostgresUidGenerator("sagas_uids_seq"))), "uidGenerator")
```

Jedyna różnica: formularz z literami ignoruje numer instalacji. W aplikacji,
preferuj `UidGeneratorActor.postgres(system)`.

Identyfikatory występują w **pulach po 10 000**. Magistrala poleceń prosi o pulę podczas uruchamiania i przy następnym
gdy pozostała tylko jedna piąta bieżącego puli, polecenie prawie nigdy nie czeka na identyfikator.

## Krok 4: magistrala zdarzeń

```scala
import io.reactivecqrs.core.eventbus.EventsBusActor
importuj org.apache.pekko.actor.Props

val eventBus = system.actorOf(Props(nowy EventBusActor()), "eventBus")

eventBus ! EventsBusActor.RegisterSubscribers(Nil)
```

Druga linijka ma większe znaczenie, niż wygląda. Szyna zdarzeń **nie publikuje niczego, dopóki nie powiesz jej, kto
Jego subskrybenci są**, nawet gdy odpowiedź brzmi „nikt”. Subskrybenci to twoje projekcje, sagi
i słuchaczy, a nie mamy jeszcze żadnego, więc lista jest pusta: „Nil”. Z [rozdziału 8](08-projections.md)
dalej, tutaj trafiają twoje projekcje.

Dlaczego tak restrykcyjnie? Wydarzenie, na które nikt się nie zapisał, jest od razu potwierdzane i opuszcza skrzynkę nadawczą.
Jeśli autobus zaczął publikować przed zapisaniem się Twoich prognoz, pierwsze wydarzenia po każdym
Start nigdy do nich nie dotrze. Dlatego autobus czeka i zatrzymuje to, co w międzyczasie otrzyma.

> **Ostrzeżenie:** zapomnij o `RegisterSubscribers` i nic nie zawiedzie głośno. Twoje polecenia i tak się powiodą,
> a ich zdarzenia są bezpieczne w bazie danych, ale magistrala przechowuje każde z nich i rejestruje ostrzeżenie
> co pięć sekund: `Magistrala zdarzeń czeka na RegisterSubscribers z hosta; nic nie jest publikowane
> do tego czasu. Ani jedna projekcja nie słyszy o niczym. Wyślij ją raz, po utworzeniu
> każdy subskrybent.

## Krok 5: magistrala poleceń

Teraz recepcja dla wózków. Podajesz kontekst z rozdziału 3 i fragmenty kroków od 1 do 4:

```scala
import com.example.shop.cart.ShoppingCartAggregateContext
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor

val carts = system.actorOf(
  AggregateCommandBusActor(nowy ShoppingCartAggregateContext, uidGenerator, eventStore, odpowiedzi, eventBus),
  „wózki”)
```

`AggregateCommandBusActor(...)` zwraca `Props` magistrali poleceń dla agregatu kontekstu
type; `system.actorOf` uruchamia go. Ma trzy dodatkowe parametry z wartościami domyślnymi, które pasują do prawie wszystkich
wszyscy:

| Parametr | Domyślny | Co robi |
|---|---|---|
| `maxInactivityMillis` | 43 200 000 (12 godzin) | Właściciel, który przez tak długi czas nic nie robił, został zatrzymany. |
| `keepAliveLimit` | 200 | W przypadku większej liczby właścicieli w pamięci, te używane najrzadziej i pozostające bezczynne przez ponad 2 minuty zostają zatrzymane. |
| `failedRestoreRetryAfter` | 5 sekund | Gdy agregatu nie można odczytać ze sklepu, jak długo jego właściciel odpowiada informacją o błędzie, zanim nastąpi kolejna próba żądania. |

Autobus co najwyżej co 5 minut sprawdza, czy właściciele się zatrzymują, w miarę jak napływają wiadomości. Właściciel, który się zatrzymał, nie jest
strata: następne polecenie lub pytanie dla danego wózka rozpoczyna nowe, które odtwarza zdarzenia wózka
(naprawdę, zagięcie rozdziału 3).

Drugim typem agregatu jest jeszcze jeden wiersz, dzielący wszystko inne:

```scala
faktury val = system.actorOf(
  AggregateCommandBusActor(nowy InvoiceAggregateContext, uidGenerator, eventStore, odpowiedzi, eventBus),
  „faktury”)
```

(`InvoiceAggregateContext` to kontekst, który możesz utworzyć samodzielnie, podobnie jak w przypadku koszyka.)

> **Pamiętaj:** jedna magistrala poleceń na typ agregatu. Wszystkie współdzielą jeden generator UID i jeden rejestr zdarzeń.
> sklep, jeden sklep odpowiedzi i jedna magistrala zdarzeń.

## Krok 6: sonda skrzynki nadawczej

```scala
import io.reactivecqrs.core.delivery.OutboxPollerActor

val outboxPoller = system.actorOf(Props(nowy OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
```

Każde zdarzenie jest zapisywane w **skrzynce nadawczej** (tabela `events_to_publish`) w tej samej transakcji co
samo zdarzenie i opuszcza je, gdy wszyscy subskrybenci odpowiedzą na nie. Ankieta skrzynki nadawczej to
który zauważa, kiedy tak się nie dzieje. Co 30 sekund odczytuje skrzynkę nadawczą; w przypadku zdarzenia, które
nadal czeka, właściciel agregatu musi opublikować go ponownie po 3 minutach i po 6; po
Po 10 minutach zdarzenie jest zgłaszane w dzienniku jako niedostarczone i od tego momentu próbuje ponownie co 30 minut
protokół.

Uruchom dokładnie jeden na system i uruchom go po poleceniu bus: pierwsze spojrzenie na skrzynkę nadawczą
pojawia się 10 sekund po uruchomieniu i do tego czasu potrzebuje autobusów, do których może przekazać swoje żądania. (Na
klaster istnieje jeden dla całego klastra; [rozdział 17](17-cluster.md) pokazuje jak.) Co on robi
Kiedy sprawy idą źle, to jest historia [rozdziału 9](09-projection-failures.md).

## Wysyłanie polecenia

Wszystko jest na swoim miejscu. Aby wysłać polecenie, użyj `ask` (rozdział 1): polecenie zostanie umieszczone w poleceniu.
skrzynkę pocztową autobusu i wręcza Ci „Przyszłość” odpowiedzi.

```scala
import com.example.shop.cart._
importuj io.reactivecqrs.api._
importuj io.reactivecqrs.api.id.UserId
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

niejawny limit czasu wartości: Limit czasu = Limit czasu (70 sekund)

val alice = UserId(1)

val answer = (koszyki ? CreateShoppingCart(alicja, "Spożywcze")).mapTo[CustomCommandResponse[_]]
val response = Await.result(odpowiedź, limit czasu.czas trwania)
// odpowiedź: SuccessResponse(Id(1001),Version(1))
```

`mapTo` informuje `Future`, jakiej odpowiedzi można się spodziewać: na każde polecenie odpowiada się
`CustomCommandResponse`. `Await.result` blokuje się do momentu otrzymania odpowiedzi. To jest w porządku w
„main” małego programu i nigdzie indziej: nigdy wewnątrz aktora, nigdy w żądaniu internetowym. Prawdziwy kod
zachowuje `Przyszłość` (więcej na ten temat poniżej).

Dlaczego 70 sekund? Polecenie może zająć tyle, ile wynosi „commandTimeout” kontekstu, czyli domyślnie 60 sekund.
zanim sama biblioteka odpowie błędem. Poczekaj trochę dłużej, a zawsze otrzymasz
odpowiedź biblioteki zamiast własnego limitu czasu. [docs/SCALING.md](../SCALING.md), sekcja 2,
udziela tej samej rady.

Odpowiedź na `CreateShoppingCart` to `SuccessResponse(aggregateId, aggregateVersion)`: identyfikator
nowy wózek dostał i jego wersję, 1. Odpowiedź jest jedynym miejscem, w którym można poznać identyfikator nowego wózka, więc trzymaj się
To jest wersja, której oczekuje Twój kolejny `AddItem`:

```scala
dopasowanie odpowiedzi {
  przypadek SuccessResponse(cartId, wersja) =>
    val next = Await.result((wózki ? AddItem(alicja, identyfikator wózka, wersja, "jabłka")).mapTo[CustomCommandResponse[_]],
      (czas trwania przekroczenia limitu czasu)
    // dalej: SuccessResponse(Id(1001),Version(2))
  przypadek inny =>
    println("Brak koszyka: " + inne)
}
```

## Na co odpowiada biblioteka

Każde polecenie otrzymuje dokładnie jedną odpowiedź, zawsze jedną z poniższych (wszystkie w `io.reactivecqrs.api`):

| Odpowiedź | To znaczy |
|---|---|
| `SuccessResponse(aggregateId, aggregateVersion)` | Zdarzenia zostały zapisane; agregat znajduje się teraz w `aggregateVersion`. |
| `CustomSuccessResponse(aggregateId, aggregateVersion, info)` | To samo, plus `info` zwrócone przez Twój program obsługi za pomocą `CommandSuccess(event, info)`. |
| `FailureResponse(exceptions)` | Twój program obsługi odpowiedział „nie” (`CommandFailure`), podając ku temu powody; albo biblioteka odrzuciła polecenie, jak to się dzieje w przypadku nowego agregatu, którego pierwsza nazwa zdarzenia nie zawiera spacji. |
| `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)` | Zmienna `expectedVersion` nie była wersją agregatu: ktoś zmienił ją pierwszy lub nie istnieje (zmienna `was` jest zatem wersją 0). |
| `CommandRejected(aggregateId, reason)` | Polecenie w ogóle nie zostało uruchomione, ale ponowne wysłanie go później może pomóc. |
| `CommandHandlingError(commandName, errorId, commandId)` | Wystąpił błąd podczas uruchamiania procedury obsługi błędów: wystąpił błąd, nie było `case` dla polecenia lub trwało to dłużej niż limit czasu polecenia; lub nie można było odczytać agregatu. |
| `EventHandlingError(eventName, errorId, commandId)` | Zapisywanie nie powiodło się: obsługa zdarzeń spowodowała błąd, zdarzenie przekroczyło limit lub baza danych uległa awarii. |

Tylko dwa sukcesy cokolwiek zapisały. Dla dwóch błędów w dzienniku znajduje się wiersz z tym samym
`errorId` i cała historia. [Rozdział 5](05-command-flavors.md) omawia każdą odpowiedź i to, co
co z tym zrobić, a [Dodatek B](appendix-b-cheat-sheet.md) zawiera ich wszystkie listę.

Sama `Future` może również zawieść, bez żadnej odpowiedzi: z wyjątkiem `AskTimeoutException`, gdy nie ma odpowiedzi
dotarłeś w czasie twojego limitu czasu lub wystąpił błąd podczas wysyłania autobusu, którego on nie rozumie
wcale, co nie jest rozkazem.

## Odczyt zawartości koszyka

Aby zobaczyć wózek takim, jaki jest teraz, należy wysłać do jego magistrali poleceń polecenie `GetAggregate`:

```scala
import scala.util.Try

// cartId: identyfikator z odpowiedzi na CreateShoppingCart
val cart = Await.result((carts ? GetAggregate(cartId)).mapTo[Try[Aggregate[ShoppingCart]]], timeout.duration)
// koszyk: Sukces(Agregat(Identyfikator(1001), Wersja(2), Niektóre(Koszyk(Artykuły spożywcze, Wektor(Przedmiot(1, jabłka))))))
```

Odpowiedzią jest „Próba” wewnątrz „Przyszłości”, która się powiedzie: „Sukces” w całości lub „Porażka”
gdy nie można odczytać zdarzeń z koszyka (wyjątek `AggregateRestoreFailedException`). Wyjątek `Aggregate` ma
trzy pola:

| Pole | Typ | Co zawiera |
|---|---|---|
| `id` | `AggregateId` | który agregat |
| `wersja` | `AggregateVersion` | ile zdarzeń zawiera jej historia |
| `aggregateRoot` | `Option[ShoppingCart]` | stan; `None`, gdy agregat nie istnieje (wersja 0) lub został usunięty |

Właściciel wózka odpowiada z pamięci i jest to ten sam właściciel, który przechowuje zdarzenia wózka, więc
Odpowiedź zawiera już Twoje ostatnie polecenie: bez czekania, bez „ostatecznie”. Użyj `GetAggregate`
gdy strona pisząca potrzebuje koszyka, na przykład, aby go pokazać przed zmianą. Do list i wyszukiwań
w wielu wózkach, użyj projekcji ([rozdział 8](08-projections.md)). A w przypadku pytań dotyczących
przeszłość (starsza wersja, stan w chwili obecnej, lista wydarzeń), patrz
[rozdział 7](07-reading-the-past.md).

## Cały program

Oto wszystkie kroki w jednym miejscu. Tworzy koszyk, dodaje dwa produkty i drukuje koszyk:

```scala
pakiet com.example.shop

import com.example.shop.cart._
importuj io.mpjsons.MPJsons
importuj io.reactivecqrs.api._
importuj io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
importuj io.reactivecqrs.core.uid.UidGeneratorActor
import org.apache.pekko.actor.{ActorSystem, Props}
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc.{PołączeniowyPula, UstawieniaPołączeniowegoPula}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

obiekt Główny {

  def main(args: Array[String]): Unit = {
    // 1. Baza danych: najpierw pula, potem tabele.
    Klasa.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
      Ustawienia puli połączeń (rozmiar początkowy = 5, rozmiar maksymalny = 32, limit czasu połączenia w milionach (connectionTimeoutMillis) = 3000L))

    val mpjsons = nowy MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = nowy PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

    // 2. Aktorzy.
    val system = ActorSystem("sklep")
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(nowy EventBusActor()), "eventBus")
    val carts = system.actorOf(
      AggregateCommandBusActor(nowy ShoppingCartAggregateContext, uidGenerator, eventStore, odpowiedzi, eventBus), "koszyki")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    system.actorOf(Props(nowy OutboxPollerActor(eventStore, eventBus)), "outboxPoller")

    // 3. Rozmowa z wózkami.
    niejawny limit czasu wartości: Limit czasu = Limit czasu (70 sekund)
    val alice = UserId(1)

    def send(polecenie: AnyRef): SuccessResponse =
      Await.result((carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
        sukces przypadku: SuccessResponse => sukces
        przypadek inny => wyrzuć nowy IllegalStateException(command.getClass.getSimpleName + " failed: " + inny)
      }

    próbować {
      val created = send(CreateShoppingCart(alicja, "Artykuły spożywcze"))
      println("Utworzono: " + utworzono)

      val withApples = send(AddItem(alicja, created.aggregateId, created.aggregateVersion, "jabłka"))
      val withOranges = send(AddItem(alicja, created.aggregateId, withApples.aggregateVersion, "pomarańcze"))
      println("Teraz w: " + withOranges.aggregateVersion)

      val cart = Await.result((carts ? GetAggregate(created.aggregateId)).mapTo[Try[Aggregate[ShoppingCart]]],
        timeout.duration).get
      println("Koszyk: " + koszyk)
    } Wreszcie {
      Await.result(system.terminate(), 30 sekund)
      ConnectionPool.closeAll()
    }
  }
}
```

Zwróć uwagę, że każdy element `AddItem` zawiera wersję z poprzedniej odpowiedzi. To optymistyczna wersja.
blokada w pracy: każde polecenie mówi „Zmieniam wózek na taki, jaki był po ostatniej zmianie”.

## Uruchomienie

Rozpocznij od `sbt "runMain com.example.shop.Main"`. Wśród linii startowych Pekko zobaczysz
raport magistrali zdarzeń „Magistrala zdarzeń została uruchomiona z 0 subskrypcjami”, a następnie w nowej bazie danych:

```
Utworzono: SuccessResponse(Id(1001),Version(1))
Teraz w: Wersja(3)
Koszyk: Aggregate(Id(1001),Version(3),Some(ShoppingCart(Groceries,Vector(Item(1,apples), Item(2,oranges)))))
```

Dlaczego 1001? Pierwsze 1000 identyfikatorów zbiorczych jest przechowywanych jako identyfikatory systemowe, więc sekwencja zaczyna się od 1001.
Uruchom program ponownie, a nowy wózek otrzyma 11001: każde uruchomienie magistrali poleceń powoduje utworzenie nowego puli
z 10 000 identyfikatorów, a to, co pozostało z poprzedniej puli, jest pomijane. Identyfikatory są unikalne, a nie następują po sobie;
Nigdy nie doszukuj się w nich sensu.

Teraz spójrz na sam dziennik. W `psql`:

```sql
WYBIERZ e.aggregate_id, e.version, t.name JAK event_class, e.event
  Z wydarzeń e DOŁĄCZ do types_names t NA t.id = e.event_type_id
 ZAMÓW PRZEZ e.id;
```

```
 aggregate_id | wersja | event_class | zdarzenie
-------------------------------- ...
         1001 | 1 | com.example.shop.cart.ShoppingCartCreated | {"name":"Artykuły spożywcze"}
         1001 | 2 | com.example.shop.cart.ItemAdded | {"name":"apples"}
         1001 | 3 | com.example.shop.cart.ItemAdded | {"name":"pomarańcze"}
```

Trzy linijki, trzy fakty. Sam koszyk, wartość „ShoppingCart”, którą wydrukowałeś, nie jest nigdzie przechowywana:
za każdym razem, gdy właściciel tego potrzebuje, odtwarza te wiersze za pomocą procedur obsługi zdarzeń.

> **Pod maską:** Oto ścieżka do `AddItem`. Magistrala poleceń nadaje mu identyfikator polecenia z
> swoją pulę i przekazuje ją właścicielowi wózka (właścicielowi rozpoczynającemu, który odtwarza wózek, jeśli żaden nie jest w
> pamięć). Właściciel sprawdza `expectedVersion`, uruchamia obsługę poleceń w dyspozytorze obsługi,
> stosuje nowe zdarzenie do kopii stanu i zapisuje je w jednej transakcji: zapisanej transakcji
> Procedura sprawdza wersję jeszcze raz i zapisuje wiersz `wydarzeń`, wiersz `agregatów` i
> wiersz skrzynki nadawczej. Następnie właściciel publikuje zdarzenie na magistrali zdarzeń i odpowiada Ci. Bez
> subskrybentów koszyków, magistrala potwierdza to natychmiast, a wiersz skrzynki nadawczej zostaje usunięty chwilę później.
> Jeśli program zatrzyma się wcześniej, wiersz czeka i jest ponownie publikowany przy następnym uruchomieniu, co
> subskrybenci po prostu ignorują jako powtórzenie. [ARCHITECTURE.md, sekcje 3 i 5](../ARCHITECTURE.md)
> opowiedz całą historię.

## Wypróbuj: dwa sposoby na usłyszenie „nie”

Wprowadź dwie małe zmiany do programu i obserwuj odpowiedzi.

1. **Wyślij starą wersję.** Podaj drugiemu elementowi `AddItem` wersję pierwszej odpowiedzi,
   `created.aggregateVersion` zamiast `withApples.aggregateVersion`. Koszyk jest w wersji 2 od
   następnie program zatrzymuje się na:

   ```
   Nie można dodać elementu: AggregateConcurrentModificationError(Id(...),AggregateType(com.example.shop.cart.ShoppingCart),Version(1),Version(2))
   ```

   Nic nie zostało zapisane, a twój opiekun nawet nie pobiegł.

2. **Złam zasadę domową.** Dodaj sześć elementów w rzędzie, poprzedzając każdy z nich wersją odpowiedzi.
   Szósty powraca jako `FailureResponse(List(Cannot have more than 5 items in your cart))`: Twój
   osoba obsługująca polecenia powiedziała nie.

## Prawdziwy kod tworzy przyszłość

„Await” było skrótem dla małego programu. W prawdziwej aplikacji polecenie pojawia się w sieci.
żądanie lub obsługę wiadomości, a blokowanie wątku za pomocą polecenia marnowałoby potrzebne wątki. Zachowaj
`Przyszłości` i zamiast tego połącz je łańcuchem:

```scala
import scala.concurrent.Future
import system.dispatcher // ExecutionContext dla flatMap

def sendAsync(polecenie: AnyRef): Future[SuccessResponse] =
  (wózki ? polecenie).mapTo[CustomCommandResponse[_]].flatMap {
    sukces przypadku: SuccessResponse => Future.successful(sukces)
    przypadek inny => Future.failed(nowy IllegalStateException(command.getClass.getSimpleName + " failed: " + inny))
  }

artykuły spożywcze val: Future[Aggregate[ShoppingCart]] = dla {
  utworzono <- sendAsync(CreateShoppingCart(alicja, "Spożywcze"))
  jabłka <- sendAsync(AddItem(alicja, created.aggregateId, created.aggregateVersion, "jabłka"))
  _ <- sendAsync(AddItem(alicja, created.aggregateId, jabłka.aggregateVersion, "pomarańcze"))
  koszyk <- (koszyki ? GetAggregate(created.aggregateId)).mapTo[Try[Aggregate[ShoppingCart]]].flatMap(Future.fromTry)
} oddaj koszyk
```

Tutaj nic nie czeka: każdy krok zaczyna się, gdy poprzedni uzyska odpowiedź, a Twoja struktura sieciowa
zamienia ostateczną „Przyszłość” w odpowiedź. Prawdziwa aplikacja również zamieniłaby każdy rodzaj odpowiedzi
w coś lepszego niż wyjątek: „proszę odświeżyć” w przypadku jednoczesnej modyfikacji,
Przyczyny wystąpienia błędu `FailureResponse` dla użytkownika. [Rozdział 5](05-command-flavors.md) pokazuje jak to zrobić.

## Kiedy coś jest nie tak

| Widzisz | To znaczy |
|---|---|
| `IllegalStateException: Pula połączeń nie została jeszcze zainicjowana.(name:default)` | Kod biblioteki został uruchomiony przed `ConnectionPool.singleton(...)`. Najpierw utwórz pulę. |
| `Magistrala zdarzeń oczekuje na RegisterSubscribers od hosta; do tego czasu nic nie jest publikowane`, co pięć sekund | Nigdy nie wysłałeś `RegisterSubscribers` do magistrali zdarzeń. |
| `AggregateConcurrentModificationError(...)` | Twoje polecenie zawierało starą wartość `expectedVersion`. Przeczytaj agregat jeszcze raz i podejmij decyzję ponownie. |
| `CommandHandlingError(...)` i `Polecenie obsługi błędów ..., errorId: [...]` w logu | Twój program obsługi poleceń zgłosił błąd lub nie ma `case` dla tego polecenia. Wiersz logu z tym samym `errorId` zawiera ślad stosu. |

Więcej objawów i sposobów leczenia znajdziesz w [Dodatku C](appendix-c-troubleshooting.md).

> **Pamiętaj:** w tym rozdziale wykorzystano prosty, lokalny system aktorów, a wszystko w części I działa z
> jeden. W środowisku produkcyjnym projekt biblioteki uruchamia każde wdrożenie jako klaster Pekko, czyli klaster
> pojedynczy węzeł, gdy istnieje tylko jedna maszyna, który zachowuje się dokładnie tak, jak to, co tutaj zbudowałeś.
> [Rozdział 17](17-cluster.md) pokazuje jak to zrobić.

## Od tego miejsca: koszyk domeny testowej

Koszyk, który zbudowałeś w rozdziale 3, jest odchudzoną kopią koszyka domeny testowej
(`io.reactivecqrs.testdomain.shoppingcart`). Od [rozdziału 5](05-command-flavors.md)
Przykłady wykorzystują sam koszyk domeny testowej, z dodatkowymi poleceniami i zdarzeniami.
`CreateShoppingCart` ma jeszcze jedno pole z przodu, `idempotencyId`, więc zobaczysz je wysłane jako
`CreateShoppingCart(None, alice, "Groceries"); rozdział 5 wyjaśnia do czego służy to pole.

## Czego się nauczyłeś

- Do działania systemu potrzebne są w tej kolejności: pula połączeń i tabele, `ActorSystem`,
  generator uid (`UidGeneratorActor.postgres(system)`), `EventsBusActor`, jeden
  `AggregateCommandBusActor` na każdy typ agregatu i jeden `OutboxPollerActor`.
- Szyna zdarzeń nie publikuje niczego, dopóki nie otrzyma `RegisterSubscribers`, nawet jeśli lista jest pusta.
- Wysyłasz polecenie za pomocą `ask` i otrzymujesz dokładnie jedną odpowiedź, `CustomCommandResponse`:
  `SuccessResponse(aggregateId, aggregateVersion)` gdy zdarzenia zostały zapisane lub jedno z
  awarie i błędy.
- Każde `Polecenie` zawiera wersję z poprzedniej odpowiedzi; jest to blokada optymistyczna.
- `GetAggregate(id)` odpowiada za pomocą `Try[Aggregate[R]]`: id, wersja i stan, już
  wliczając Twoje ostatnie polecenie.
- W prawdziwym kodzie zachowaj `Future` i skomponuj je; `Await` należy stosować tylko w małych programach.

Następnie: [rozdział 5](05-command-flavors.md) obsługuje całe menu rodzajów poleceń.