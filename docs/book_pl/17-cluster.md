# Rozdział 17. Przejście na rozproszoną dystrybucję: uruchamianie w klastrze

Do tej pory Twoja aplikacja działała na jednej maszynie. W tym rozdziale przenosi się do współdzielonego domu:
kilka maszyn, zwanych **węzłami**, które działają jak jedna. Na koniec będziesz wiedział, co się zmienia, gdy
uruchamiasz na kilku węzłach, które części uruchamiasz na każdym węźle, a które uruchamiasz dokładnie raz, jak zdecydować
gdzie mieszkają właściciele każdego typu agregatu, co może podróżować między węzłami (i jak duże to może być)
być) i jak uaktualniać węzły pojedynczo, nie przerywając działania aplikacji.

---

## Dlaczego więcej niż jeden węzeł?

Wyobraź sobie ruchliwą restaurację z jednym kucharzem. Kiedy kucharz robi sobie przerwę, kuchnia się zatrzymuje. Zatrudnij
drugi kucharz i kuchnia kontynuują pracę w przerwach i obsługują więcej stolików w porze kolacji.
dwóch kucharzy dzieli się jedną książką zamówień i obowiązuje jedna zasada: karta zamówienia jest w rękach jednego
gotować na raz.

Uruchomienie ReactiveCQRS na kilku węzłach daje te same trzy efekty:

- **Podtrzymywanie aktywności.** Gdy jeden węzeł uruchamia się ponownie lub maszyna przestaje działać, pozostałe nadal świadczą usługi.
- **Więcej rąk.** Obsługujący polecenia, właściciele agregatów i projekcje współdzielą procesory kilku
  maszyny.
- **Uaktualnienia bez znaku „zamknięte”.** Wymieniasz węzły po jednym na raz (*aktualizacja ciągła*, w
  koniec tego rozdziału).

I wiele pozostaje dokładnie takie samo:

- **Jedna baza danych PostgreSQL** dla wszystkich węzłów, z tymi samymi tabelami co poprzednio.
- **Twój kod**: agregaty, polecenia, zdarzenia, projekcje i sagi z poprzednich rozdziałów.
- **Sprawdzanie wersji** (optymistyczna blokada [rozdziału 1](01-cqrs-without-the-headache.md)).
  baza danych nadal pozwala tylko jednemu autorowi przechowywać każdą wersję agregatu, niezależnie od tego, co się dzieje pomiędzy
  węzły.

> **Pamiętaj:** wszystkie węzły klastra współdzielą jedną bazę danych. Klaster rozdziela *pracę*;
> baza danych przechowuje *prawdę*.

## Każda instalacja jest klastrem, nawet klastrem jednej instalacji

ReactiveCQRS działa na **klastrze Apache Pekko**. Włącza się go za pomocą konfiguracji, a nie kodu. Oto
plik `application.conf` dla węzła instalacji trzywęzłowej, którego system aktorów nazywa się `shop`
(`ActorSystem("sklep", konfiguracja)`):

```hocon
punk {
  aktor.dostawca = klaster
  zdalna.tętnica.kanoniczna {
    hostname = "10.0.0.11" # adres tego węzła
    port = 17355 # Wartość domyślna Pekko
  }
  klaster {
    węzły-siewne = ["pekko://shop@10.0.0.11:17355", "pekko://shop@10.0.0.12:17355"]
    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
    split-brain-resolver.active-strategy = keep-majority
    app-version = "1.4.0" # wersja Twojego wydania; zobacz poniżej „Aktualizacje ciągłe”
  }
}
```

Trzy z tych wersów zasługują na osobne słowo:

- **`węzły-seed`** to adresy, do których węzeł początkowy ma się dołączyć. (Pekko Management's Cluster
  Bootstrap może je znaleźć na Kubernetesie; własna dokumentacja Pekko opisuje oba sposoby.)
- **Rozwiązywacz podzielonego mózgu** decyduje, co się stanie, gdy sieć podzieli klaster na dwa
  grupy, które nie mogą się widzieć: jedna strona zostaje, druga się zamyka. „zachowaj większość” utrzymuje
  większa strona. Przy dwóch węzłach nie ma większości, więc zastosuj strategię „większości dzierżawy” Pekko z
  dzierżawa (w Kubernetes, dzierżawa Kubernetes od Pekko Management): strona, która posiada dzierżawę
  gorset.
- **`app-version`** nadaje nazwę Twojemu wydaniu. Ma to znaczenie podczas aktualizacji kroczących.

Na pojedynczej maszynie używasz dokładnie tych samych ustawień, a sam węzeł jest jego jedynym zalążkiem. To jest
**grupa jednego** i zachowuje się jak pojedynczy węzeł, który miałeś wcześniej. Własny test biblioteki
zestawy zaliczą test, gdy każdy test zostanie wykonany w klastrze składającym się z jednej osoby.

> **Wskazówka:** korzystaj z ustawień klastra od pierwszego dnia, nawet na jednej maszynie. Przejście z jednego węzła do trzech
> później następuje zmiana konfiguracji, a nie kodu.

> **Pod maską:** „każde wdrożenie jest klastrem: jeden węzeł lokalny, kilka w chmurze” to
> decyzja D42; rozdzielony moduł rozdzielający mózg i dzierżawa to D46, sprawdzane przez `ClusterBehaviourSpec`.
> Pełną historię można znaleźć w [ARCHITECTURE.md, sekcja 13](../ARCHITECTURE.md).

## Co działa na każdym węźle i co działa raz

W domu wieloosobowym każdy ma własną szczoteczkę do zębów, ale tylko jedna osoba robi to co tydzień
zakupy, albo lodówka zapełnia się pięcioma workami ziemniaków. Grono to to samo:

| Na każdym węźle | Raz w całym klastrze |
|---|---|
| generator uid (`UidGeneratorActor.postgres(system)`) | każda zarządzana projekcja (`Projections.start`, [rozdział 10](10-rebuilding-projections.md)) |
| szyna zdarzeń (`EventsBusActor`) | sonda skrzynki nadawczej (`OutboxPollerActor`) |
| magistrala poleceń według typu agregatu (`AggregateCommandBusActor`) | harmonogram zadań squash (`SquashScheduler`, [rozdział 14](14-squash.md)) |
| | każdy biegacz sagi ([rozdział 12](12-sagas.md)) |

Należy pamiętać o jednym fakcie: **na każdy węzeł istnieje magistrala zdarzeń.** Właściciel zbiorczy publikuje swoje
zdarzenia do magistrali węzła, na którym działa. Abonent zarejestrowany na magistrali jednego węzła widzi zatem
tylko wydarzenia właścicieli tego węzła. Więc cokolwiek musi zobaczyć *każde* wydarzenie (projekcję, sagę, która
listens) uruchamia się raz w klastrze i jest rejestrowany na magistrali *każdego* węzła.

### Uruchamianie tego, co działa raz: `Singletons.start`

`Singletons.start(system, nazwa, właściwości)` to sposób uruchomienia czegoś, co musi zostać uruchomione raz. Każdy węzeł
nazywa się tak samo. Następnie Pekko uruchamia aktora na **najstarszym** węźle klastra i
`start` przydziela każdemu węzłowi **proxy**: `ActorRef`, który przekazuje wszystko, co do niego wysyłasz, gdziekolwiek
Aktor działa, zachowując nadawcę, więc odpowiedzi wracają do Ciebie. Kiedy najstarszy węzeł odchodzi, aktor
przechodzi do następnego najstarszego, raz; podczas przenoszenia serwer proxy przechowuje wiadomości do niego wysłane (do
Bufor Pekko wynoszący 1000).

W systemie aktorów, który nie jest klastrem, `Singletons.start` po prostu uruchamia samego aktora, więc
ten sam kod działa w obu przypadkach.

`Projections.start` ([rozdział 10](10-rebuilding-projections.md)) wykonuje to za Ciebie: w klastrze,
Zwracany `ActorRef` jest proxy węzła dla nadzorcy projekcji.

Oto, co każdy węzeł aplikacji koszyka zakupowego uruchamia na początku. Używa
`ClusterCartsProjection`, mała projekcja `ClusterDeliverySpec`, która przechowuje elementy każdego koszyka
nazwy i `ShardedCartContext` z następnej sekcji:

```scala
importuj io.mpjsons.MPJsons
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projekcje, Ustawienia projekcji}
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState
importuj io.reactivecqrs.core.uid.UidGeneratorActor
import org.apache.pekko.actor.{ActorRef, Rekwizyty}

// Ten sam kod na każdym węźle (system: ActorSystem tego węzła)
val mpjsons = nowy MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = nowy PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()

// Po jednym z każdego na każdym węźle
val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
val eventBus = system.actorOf(Props(nowy EventBusActor()), "eventBus")
val carts = system.actorOf(AggregateCommandBusActor(new ShardedCartContext, uidGenerator, eventStore, responses, eventBus), "carts")

// Po dostaniu się do klastra: każdy węzeł je uruchamia, jeden węzeł je uruchamia
projekcje val = nowe projekcje(system,
  Ustawienia projekcji (eventStore, subskrypcje, nowy PostgresProjectionRegistry().initSchema(), Seq (nowy ShardedCartContext)))
val cartItems: ActorRef = projections.start[ClusterCartsProjection]("cluster-carts", 1, "clusterCarts") { build =>
  nowy ClusterCartsProjection(subskrypcje,
    nowy PostgresDocumentStore[ClusterCartItems](build.documentStore("cluster_carts"), mpjsons, nowy NoopDocumentStoreCache))
}
Singletons.start(system, "outboxPoller", Props(nowy OutboxPollerActor(eventStore, eventBus)))
val sagaState = new PostgresSagaState(mpjsons, typesNames)
sagaState.initSchema()
val cartsSaga: ActorRef = Singletons.start(system, "cartsSaga", Props(new MultipleCartCreatorSaga(sagaState, uidGenerator, carts)))

// Każdy węzeł rejestruje serwer proxy projekcji na własnej magistrali
eventBus! Zarejestruj subskrybentów (Seq (elementy koszyka))
```

Co się stanie, gdy to zostanie uruchomione na trzech węzłach:

- Każdy węzeł ma własny generator UID, magistralę zdarzeń i magistralę poleceń wózka. Polecenie wysyłane do wózka
  magistrala poleceń *dowolnego* węzła działa.
- Jeden nadzorca projekcji „cluster-carts” działa na najstarszym węźle. Szyna każdego węzła zna to
  poprzez serwer proxy tego węzła, więc projekcja otrzymuje zdarzenia właścicieli każdego węzła.
- Działa jeden moduł sondujący skrzynkę nadawczą i jeden moduł uruchamiający Sagę, również na najstarszym węźle. Wysyłasz polecenia Sagi do
  `cartsSaga`, serwer proxy, niezależnie od węzła, na którym się znajdujesz.

`ClusterDeliverySpec` uruchamia dokładnie to na trzech węzłach: projekcji dostarczanej przez właścicieli każdego węzła
odpowiada na zapytania na dowolnym węźle, z jedną instancją w całym klastrze; a gdy najstarszy węzeł opuszcza klaster,
projekcja, sondaż i biegacz sagi poruszają się, a to, co zostało opublikowane w międzyczasie, wciąż dociera
projekcja.

> **Ostrzeżenie:** nigdy nie uruchamiaj programu do pobierania skrzynek nadawczych ani harmonogramu squasha za pomocą zwykłego `system.actorOf` na
> każdy węzeł. Przy trzech węzłach każde zdarzenie oczekujące w skrzynce nadawczej byłoby ponownie publikowane trzy razy,
> i każdy squash byłby testowany trzy razy. Uruchom je poleceniem `Singletons.start`.

> **Pamiętaj:** zarejestruj proxy projekcji na magistrali *każdego* węzła. Projekcja zarejestrowana na jednym
> węzeł widziałby zdarzenia tylko jednego węzła i po prostu pomijałby resztę.

Kilka rzeczy działa nieco inaczej, gdy coś zostanie przeniesione do innego węzła:

- **Odpowiedzi na sagę pojawiają się co najmniej raz.** Twórca sagi przechowuje informacje o pytającym wraz z węzłem pytającego
  adres, aby odpowiedź dotarła do osoby dzwoniącej na dowolnym węźle. Jeśli biegacz przemieszcza się między wysłaniem odpowiedzi
  i nagrywając ją, następny uczestnik może odpowiedzieć ponownie: osoba dzwoniąca z długim stażem powinna traktować drugą odpowiedź
  dla tej samej sagi jako powtórzenia.
- **Subskrypcje push** projekcji podlegających subskrypcji
  ([rozdział 11](11-subscribable-projections.md)) są tracone, gdy pojedynczy obiekt projekcji ulega przesunięciu.
  W odpowiedzi na prośbę o odnowienie pojawia się `ProjectionSubscriptionsCancelled`: subskrybuj ponownie.

> **Pod maską:** autobus pyta każdego zarejestrowanego abonenta o swoje subskrypcje
> `GetSubscriptionRequests(registered)`, a nadzorca stojący za proxy odpowiada za to. Kiedy
> singleton się porusza, jego instancje zatrzymują się; magistrala ponownie pyta serwer proxy, a to, co zostało opublikowane w międzyczasie
> pozostaje w skrzynce nadawczej, dopóki moduł pobierający nie opublikuje jej ponownie. Zobacz
> [ARCHITEKTURA.md, sekcja 13.4](../ARCHITEKTURA.md) (D44, D46, D53, D55, F110, F129).

## Miejsce zamieszkania właścicieli: Lokalnie lub w Shard

Każdy agregat ma **właściciela**, aktora, który przechowuje jego stan i uruchamia jego polecenia
([rozdział 1](01-cqrs-without-the-headache.md)). W klastrze wybierasz, według typu agregatu, gdzie
Ci właściciele żyją. Pomyśl jeszcze raz o biletach na zamówienie:

- **Lokalnie:** każdy kucharz, którego zapytano o stolik nr 7, zabiera kopię jego paragonu. Gdy dwóch kucharzy pisze
  od razu, księga zamówień (baza danych) decyduje, kto był pierwszy.
- **Sharded:** każdy bilet ma jedno biurko w kuchni. Ten, kto przyjmuje zamówienie na stolik nr 7, odchodzi.
  do tego biurka.

### Lokalny, domyślny

Dzięki `Placement.Local` każdy węzeł zachowuje własnych właścicieli agregatów, o które jest pytany.
Wózek 42 może mieć właściciela na węźle A *i* na węźle B. To jest bezpieczne: sprawdzanie wersji bazy danych
pozwala, aby tylko jeden z nich przechowywał każdą wersję.

Kosztuje to jednak trochę. Na węźle, który nie jest jedyny w swoim klastrze, właściciel nie wie, co
pozostałe węzły napisały, więc zanim odpowie na stan lub uruchomi polecenie, porównuje swoją wersję
z zapisanym jednym (jedno wyszukiwanie klucza) i odczytuje agregat ponownie, gdy inny węzeł go zmieni.

Właściciele bezczynnych węzłów lokalnych są usypiani przez magistralę poleceń swojego węzła. Nie częściej niż co 5 minut wykonuje ona następujące operacje:
powyżej 200 właścicieli, te używane najrzadziej, które nie były używane przez 2 minuty, są zatrzymywane, a wszystkie używane przez właściciela, które nie były używane przez
12 godzin jest zatrzymane. Obie liczby są parametrami `AggregateCommandBusActor(...)`:
`keepAliveLimit` (200) i `maxInactivityMillis` (12 godzin).

### Sharded: jeden właściciel na agregat w całym klastrze

Kontekst jest włączany poprzez nadpisanie parametru `placement`. To jest `ShardedCartContext` ze specyfikacji klastra:

```scala
importuj io.reactivecqrs.api._
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartAggregateContext

klasa ShardedCartContext rozszerza ShoppingCartAggregateContext {
  nadpisanie val placement: Placement = Placement.Sharded(numberOfShards = 10)
}
```

To cała zmiana w kodzie. Teraz:

- **W całym klastrze na jeden wózek przypada jeden właściciel**, umieszczony przez Pekko Cluster Sharding. Każdy
  Szyna poleceń węzła uruchamia region fragmentu typu i wysyła przez niego wszystko do właścicieli.
  Polecenie lub zapytanie wysłane do magistrali poleceń wózka dowolnego węzła dociera do jednego właściciela, gdziekolwiek się znajduje.
  biegnie.
- **Odłamek agregatu** pochodzi z jego identyfikatora (identyfikator modulo `liczbaOdłamków`). Pekko rozprzestrzenia
  odłamki nad węzłami i przesuwa je, gdy węzły pojawiają się i znikają. Domyślnie jest to 100 odłamków; dąż do
  około dziesięciu fragmentów na węzeł największego klastra, jaki planujesz.
- **Bezczynni właściciele są pasywowani przez swój odłamek** po 120 sekundach bez wiadomości (Pekko's
  `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout`).
- **System aktorów musi być klastrem** (`pekko.actor.provider = cluster`) składającym się z jednego lub większej liczby węzłów.
- **Polecenia są przesyłane** do węzła właściciela, więc muszą zawierać wiadomość między węzłami (patrz „Wiadomości
  między węzłami" poniżej).

`PassivateAggregate(id)`, wysłane do magistrali poleceń dowolnego węzła, powoduje uśpienie właściciela agregatu
oba umiejscowienia.

Oto co kupuje od „jednego właściciela” w ramach „ShardedOwnersSpec”: trzy węzły wysyłają 15 współbieżnych
Polecenia `AddItemConcurrently` odnoszą się do każdego z koszyków. Wszystkie 45 kończą się sukcesem, koszyk kończy się na wersji 46, a
dokładnie jeden węzeł jest zarządzany przez jego właściciela.

### Jak wybrać

| | Lokalny | Podzielony |
|---|---|---|
| Właściciele jednego agregatu | jeden na każdym węźle o to pytał | jeden w całym klastrze |
| Dwóch pisarzy jednocześnie | możliwe; decyduje sprawdzenie wersji | nie w normalnym trybie |
| Dodatkowy koszt odczytu lub polecenia | jedno wyszukiwanie klucza w węźle, który nie jest sam | konieczność przejścia do węzła właściciela, gdy działa gdzie indziej |
| Bezczynni właściciele | przeszukani przez magistralę poleceń | pasywowani przez odłamek po 120 s |
| Polecenia muszą mieścić wiadomość między węzłami | nie | tak |

Prosta zasada:

- **Jeden węzeł:** pozostaje przy `Lokalnym`. Właściciel, będąc sam w swoim klastrze, nie potrzebuje żadnych wyszukiwań.
- **Kilka węzłów, typ, który jest często odczytywany lub zapisywany:** ustaw go jako `Sharded`.
- **Kilka węzłów, typ, który jest rzadko zmieniany:** `Lokalny` jest w porządku.

> **Ostrzeżenie:** zmiana położenia typu lub jego `numberOfShards` wymaga kropki w całym tekście
> klaster, nigdy nie jest to aktualizacja ciągła. Podczas gdy stare i nowe węzły działają obok siebie, agregat miałby
> właściciel lokalny *i* właściciel podzielony, a właściciele podzieleni nie sprawdzają sklepu przed udzieleniem odpowiedzi.
> Typy można przenosić do kategorii „Sharded” pojedynczo, stawiając za każdym razem kropkę.

> **Pod maską:** strona właścicieli miejsca umieszczenia to `Owners.scala` (`LocalOwners`,
> `ShardedOwners`); wiadomości do właścicieli sharded są przesyłane jako `ToOwner(aggregateId, message)`. Zobacz
> [ARCHITEKTURA.md, sekcja 13.3](../ARCHITEKTURA.md) (D43, D72, F131, F145).

## Kiedy węzeł opuszcza

Kucharz, który wraca do domu, najpierw kończy danie na patelni, a potem wręcza kupony. Właściciel, który został pocięty na kawałki
robi to samo, gdy jego węzeł opuszcza klaster (lub gdy Pekko przenosi swój fragment, aby zrównoważyć obciążenie):

1. Kończy wykonywane polecenia (w przypadku przetwarzania szeregowego kolejka zostaje opróżniona).
2. Odpowiada na zapytania, które na niego czekają, i zapisuje migawkę, jeśli typ wykonuje migawki.
   pasywacja ([rozdział 13](13-snapshots.md)).
3. Zatrzymuje się. Wiadomości dla jego fragmentu czekają w regionie fragmentu węzła wysyłającego (Pekko buforuje do
   100 000 wiadomości na region) i dotrzeć do nowego właściciela na innym węźle.

Zakończenie procesu zajmuje trochę czasu, więc plik `reference.conf` biblioteki przydziela mu czas:

```hocon
pekko.cluster.sharding.handoff-timeout = 90 s
pekko.coordinated-shutdown.phases.cluster-sharding-shutdown-region.timeout = 90 s
```

Oba przekraczają domyślny limit czasu polecenia wynoszący 60 sekund (`AggregateContext.commandTimeout`). Pekko
zatrzymuje właściciela na 5 sekund przed upływem limitu czasu przełączenia.

> **Ostrzeżenie:** Twój menedżer procesów musi odczekać *dłużej* niż te 90 sekund, zanim zamknie węzeł
> to jest wyjście (na Kubernetes: `terminationGracePeriodSeconds`). Jeśli wywołasz kontekst
> `commandTimeout`, ustaw oba ustawienia wyżej, a także okres karencji wyżej od nich.

Polecenie, które czekało w buforze podczas przekazania, jest wykonywane później niż zostało wysłane, a jego 60 sekund
rozpocząć, gdy dotrze do nowego właściciela. Osoba dzwoniąca z krótkim czasem oczekiwania może zatem zrezygnować z polecenia.
który nadal jest stosowany. Lekarstwo pochodzi z [rozdziału 5](05-command-flavors.md): **spróbuj ponownie z
klucz idempotentności**, a polecenie zostanie wykonane jednorazowo, bez względu na to, ile razy zostało wysłane.

`ShardedOwnersSpec` sprawdza to w trudny sposób: węzeł odchodzi, gdy jeden z jego właścicieli uruchamia polecenie
trwa to dwanaście sekund, a dziesięć wózków, których właściciele tam jeżdżą, otrzymuje po pięć przedmiotów, po jednym na każde 150 sekund
milisekundy, przez inne węzły. Każde polecenie kończy się sukcesem, łącznie z tym wolniejszym, i każdy element
jest w koszyku raz.

A co, jeśli węzeł nie odejdzie grzecznie, lecz zniknie? Rozdzielony mózg to zauważy i usunie.
z klastra, a jego fragmenty zaczynają się od nowa na pozostałych węzłach. Przez krótką chwilę dwóch właścicieli
może istnieć jeden agregat; kontrola wersji bazy danych nadal pozwala tylko jednemu z nich na zapisanie każdego
wersja.

> **Pamiętaj:** własne testy wielowęzłowe biblioteki uruchamiają kilka węzłów w jednej wirtualnej maszynie Java i obejmują węzły
> które odchodzą z gracją. Testy, które wyłączają węzeł lub przerywają sieć, są planowane przed wersją 1.0
> wydanie. Do tego czasu testuj takie awarie we własnym środowisku.

## Wiadomości między węzłami

W jednym węźle wiadomość jest przekazywana jak notatka przez biurko. Między węzłami musi być
Napisane, włożone do koperty i wysłane: **numer seryjny**. A koperta ma limit rozmiaru.

### Jeden serializator do wszystkiego

Plik `reference.conf` biblioteki wiąże jeden serializator, `ClusterSerializer`, z `java.io.Serializable`,
więc klasy przypadków i kolekcje Scala, które przechodzą przez nie przez węzły (Pekko zachowuje kilka swoich
własne, takie jak „Some” i „None”). Serializacja w Javie pozostaje wyłączona (domyślne ustawienie w Pekko). Konfigurujesz
Nic.

- Wiadomości biblioteki są zapisywane pole po polu.
- **Twoje wartości** (polecenia, zdarzenia, stany agregatów, komunikaty zapytań i ich odpowiedzi, kolejność sagi
  i odpowiedzi) są zapisywane w formacie JSON ich klasy przez mpjsons, tak samo jak przechowywane są zdarzenia.
- Wyjątki biblioteki, takie jak `AggregateInIncorrectVersionException`, zachowują swoją klasę.
  inny wyjątek pojawia się jako `RemoteFailure` z oryginalną nazwą klasy i komunikatem.

Daje to Twoim klasom cztery zasady:

1. **pliki mpjson muszą być w stanie je zapisywać i odczytywać według swojej klasy:** brak pól typu `Dowolny`, cecha,
   `ActorRef` lub `Throwable`.
2. **Definiuj komunikaty na najwyższym poziomie lub w obiekcie, nigdy wewnątrz klasy aktora.** Klasa
   zdefiniowany wewnątrz innego węzła niesie ze sobą otaczający go obiekt, który nie może zostać przeniesiony do innego węzła.
3. **Pole dodane później musi być `Opcją`.** Podczas aktualizacji ciągłej węzeł starszej wersji
   wersja wysyła wiadomość bez nowego pola, a czytelnik wypełnia brakujące pole
   „Brak”, zero lub „null”, nigdy z domyślną wartością pola w Scali. Odczytaj „{"id":1}” jako
   `Item(id: Int, name: String)` i otrzymasz `Item(1, null)`.
4. **Pole, które zmienia swój typ, wymaga nowej klasy.**

> **Wskazówka:** test przesyłania wiadomości przez serializator w obie strony zajmuje kilka linijek;
> [rozdział 19](19-testing.md) pokazuje jeden. Jeśli zarejestrowałeś własne konwertery mpjsons, podaj
> serializator klastra, instancja skonfigurowana w ten sam sposób, poprzez
> `ClusterSerialization(system).mpjsons`, przed wymianą komunikatów między węzłami.

### Jak duża może być wiadomość

Transport Pekko (Artery) wysyła wiadomość w jednej ramce o rozmiarze 256 KiB (ćwierć megabajta)
Domyślnie, ustawione przez `pekko.remote.artery.advanced.maximum-frame-size`. Pekko upuszcza większą wiadomość i
rejestruje tylko „Odrzucanie ponadgabarytowego ładunku”; nadawca nigdy nie zostaje o tym poinformowany.

Limit biblioteki `MessageSize(system).limit` to 8 KiB (bez ramki) przeznaczone na nagłówek Pekko:
Domyślnie 248 KiB. Oto, co dzieje się z każdym rodzajem wiadomości, która może być zbyt duża:

| Wiadomość | Gdy przekracza węzły | Za duża? |
|---|---|---|
| polecenie typu `Sharded` | jego właściciel uruchamia je na innym węźle | odrzucane od razu za pomocą `FailureResponse` ("zbyt duże, aby wysłać je między węzłami"), niezależnie od tego, gdzie uruchamia je jego właściciel |
| odpowiedź polecenia, odpowiedź `GetAggregate` (stan) | właściciel działa na innym węźle | pytający od razu zgłasza błąd `MessageTooLargeException` |
| odpowiedź na zapytanie projekcji | projekcja działa na innym węźle | pytający od razu kończy zapytanie błędem `MessageTooLargeException` |
| dostawa kilku zdarzeń | abonent działa na innym węźle | podzielony na części o maksymalnej długości połowy limitu |
| pojedyncze zdarzenie lub stan w dostawie | subskrybent działa na innym węźle | awaria u subskrybenta |
| `GetEventsForAggregate` (cała historia) | nigdy | odczytaj na węźle pytającym, ze sklepu |

Każda wiadomość, która jest zbyt duża, jest rejestrowana na węźle wysyłającym wraz z jej klasą i rozmiarem (maksymalnie raz)
na klasę co 10 sekund) i liczone w metryce `reactivecqrs.message.too.large`
([rozdział 18](18-production.md)).

Aby uzyskać wyobrażenie o rozmiarach zmierzonych 17.10.2026 r. (raport `load-M5` w `docs/perf/`): jedno zdarzenie
dostarczenie zajęło 561 bajtów; ponowna publikacja części 1000 wydarzeń z notatkami o długości 200 znaków zajęła
470 KiB, dlatego takie części są dzielone.

Co z tym zrobić:

- **Utrzymuj polecenia, odpowiedzi, stany i odpowiedzi na zapytania znacznie poniżej 248 KiB.** Stan, który subskrybenci
  zabieraj ze sobą każde wydarzenie podróżuje z każdym wydarzeniem.
- **Odczytaj duży stan na węźle pytającym** za pomocą `GetAggregateAtInstant(id, Instant.now)`:
  magistrala poleceń przywraca dane bezpośrednio ze sklepu, zamiast aby właściciel je wysyłał.
- **Jeśli nie da się uniknąć większych wiadomości**, podnieś `pekko.remote.artery.advanced.maximum-frame-size`.
  Pekko przechowuje w pamięci bezpośredniej do 128 buforów rozmiaru ramki, więc ramki o rozmiarze 1 MiB mogą zająć do
  128 MiB (szacowane na podstawie tej arytmetyki). Albo skieruj adresy docelowe dużych wiadomości przez Pekko
  `large-message-destinations` (ramki 2 MiB, domyślnie 32 bufory).

[Rozdział 18](18-production.md) zawiera więcej informacji na temat dużych wydarzeń i dokumentów.

> **Pod maską:** serializator, jego tolerancyjny odczyt i ochrona rozmiaru są opisane w
> [ARCHITECTURE.md, sekcja 13.1](../ARCHITECTURE.md) (D45, D68, F121, F135); zmierzone rozmiary
> a zalecenia znajdziesz w [SCALING.md, sekcja 6](../SCALING.md).

## Czy ten węzeł jest gotowy?

Węzeł, który właśnie został uruchomiony, nie powinien przyjmować ruchu, dopóki jego magistrala zdarzeń nie pozna wszystkich jego danych.
subskrybenci: dopóki każdy zarejestrowany subskrybent nie odpowie (projekcje za ich serwerami proxy)
(wliczając w to), autobus nie publikuje niczego.

`Readiness(system)` śledzi to:

```scala
import io.reactivecqrs.core.cluster.Readiness

Readiness(system).ready // fałsz, podczas gdy magistrala zdarzeń nadal czeka na swoich subskrybentów
Readiness(system).waitingFor // magistrale zdarzeń, które nadal czekają
```

Jeśli korzystasz z kontroli stanu zdrowia Pekko Management (dodaj Pekko Management do swojej aplikacji; biblioteka
nie przynosi), jeden wiersz sprawia, że ​​jest to sprawdzenie gotowości, więc moduły równoważenia obciążenia wysyłają ruch tylko do gotowych
węzły:

```hocon
pekko.management.health-checks.readiness-checks {
  reactivecqrs = "io.reactivecqrs.core.cluster.ReadinessCheck"
}
```

`Members(system)` informuje, co dany węzeł wie o członkach klastra:

```scala
import io.reactivecqrs.core.cluster.Members

val members = Członkowie(system)
members.oldestAppVersion // najniższa wersja aplikacji wśród członków, np. Some(Version("1.0.0"))
members.olderVersionsRun // prawda, gdy członek uruchamia starszą wersję niż ten węzeł
members.alone // prawda poza klastrem i w klastrze jednego
```

## Ulepszenia ciągłe

Zmiana kucharzy podczas serwowania obiadu jest dopuszczalna, pod warunkiem, że nikt nie przygotuje nowego dania
na kwitach zamówień, zanim każdy kucharz zdąży je przeczytać. Na tym właśnie polega idea stopniowego uaktualniania.

Kroki:

1. **Nadaj każdej wersji osobną wersję aplikacji** na każdym węźle tej wersji:
   `pekko.cluster.app-version = "2.0.0"`.
2. **Uruchom węzły nowej wersji.** Dołączają one do klastra. Nowe fragmenty trafiają tylko do węzłów
   najnowsza wersja, więc praca przenosi się ze starych węzłów do nowych i nigdy z powrotem.
3. **Zatrzymuj stare węzły jeden po drugim, najstarszy na końcu.** Elementy, które działały kiedyś, działają na najstarszym węźle.
   węzła, więc poruszają się tylko raz.
4. **Klienci muszą ponawiać próby.** Klient wysyła do działających węzłów polecenie, na które nie otrzymuje odpowiedzi
   po pewnym czasie zostaje ponownie przesłany przez inny węzeł, z tym samym kluczem idempotencji.

### Nowa klasa zdarzeń: zapisuj ją tylko wtedy, gdy każdy węzeł może ją odczytać

Załóżmy, że wersja 2.0.0 rejestruje, skąd pochodzi każdy element, z nową klasą zdarzeń. Węzły
1.0.0 nigdy o tym nie słyszałem. Jeśli węzeł 2.0.0 zapisał jeden, podczas gdy węzeł 1.0.0 nadal działa, stary węzeł
właściciele i prognozy nie mogły tego odczytać.

Podajesz więc kontekst, z którego każdy węzeł może odczytać nową klasę, a program obsługi zapisuje
Stare zdarzenie do tej pory. To jest `ShardedCartContextV2` ze specyfikacji klastra, lekko przycięte:

```scala
importuj io.reactivecqrs.api._
import io.reactivecqrs.testdomain.shoppingcart._

klasa przypadku ItemAddedV2 (nazwa: String, źródło: String) rozszerza Event[ShoppingCart]

klasa ShardedCartContextV2 rozszerza ShardedCartContext {
  nadpisz val writtenFrom: Mapa[Klasa[_], String] = Mapa(klasa[DodanyElementV2] -> "2.0.0")

  nadpisz def commandHandlers = cart => {
    przypadek c: AddItemConcurrently jeśli everyNodeRuns("2.0.0") => CommandSuccess(ItemAddedV2(c.name, "wersja 2"))
    przypadek inny => super.commandHandlers(cart)(inne) // Dodano element, jak w 1.0.0
  }

  nadpisz def eventHandlers = (userId, timestamp, cart) => {
    przypadek ItemAddedV2(nazwa, _) => EventsHandlers.itemAdded(koszyk, ItemAdded(nazwa))
    przypadek inny => super.eventHandlers(userId, timestamp, cart)(inne)
  }
}
```

Co się dzieje podczas aktualizacji:

- Podczas gdy węzeł 1.0.0 jest członkiem, `everyNodeRuns("2.0.0")` jest fałszem, a obiekt obsługi zapisuje
  `ItemAdded`, jak poprzednio.
- Jeśli coś mimo wszystko próbuje zapisać `ItemAddedV2`, właściciel odpowiada na polecenie za pomocą
  `CommandRejected` (wyślij ponownie później); odpowiedź na squash, którego zdarzenie squash jest takiej klasy
  z `SquashRefused`.
- Po opuszczeniu ostatniego węzła 1.0.0 zapisywany jest `ItemAddedV2`.

Dostarcz obsługę zdarzeń *i* obsługę projekcji nowej klasy w tej samej wersji, w której
umieszcza go w `writtenFrom`.

`RollingUpgradeSpec` wykonuje to wszystko pod obciążeniem: dwa węzły 1.0.0, dwa węzły 2.0.0 dołączają się później,
polecenia z każdego węzła. Nie jest zapisywany `ItemAddedV2`, dopóki stary węzeł jest członkiem; jest zapisywany raz
oba stare węzły zniknęły; żadne polecenie nie zostało utracone ani zastosowane dwa razy, a projekcja zawiera każdy element tylko raz.

Jeszcze kilka zasad ruchu drogowego:

- **Ustaw wersje aplikacji przed użyciem `writtenFrom`.** Gdy żaden członek nie ustawił żadnej (wszystko u Pekko)
  `0.0.0`), każda klasa jest zapisywana od razu; członek bez niej jest uważany za najniższą wersję, więc
  nic nowego nie zostało napisane, gdy film jest emitowany.
- **Węzeł, który nie zna swojego klastra, nie zapisuje niczego nowego.**
- **Nigdy nie cofaj się do wersji, w której napisano nowe klasy zdarzeń.** Starszej wersji nie można
  przeczytaj je.
- **Nowa klasa poleceń:** wysyłaj ją tylko wtedy, gdy każdy węzeł uruchomi swoją wersję, jako właściciel starego węzła
  nie wiedziałbym o tym.
- **Projekcja usunięta w nowej wersji:** `Projections.retire(name)` czeka, aż każdy członek
  uruchamia tę wersję przed przerwaniem projekcji (sprawdza ją ponownie co 10 sekund).
- **Zmiany umiejscowienia** nadal wymagają kropki (patrz powyżej).

> **Pod maską:** wersje aplikacji, przełącznik zapisu, gotowość i test aktualizacji ciągłej
> opisano w [ARCHITECTURE.md, sekcja 13.6](../ARCHITECTURE.md) (D65–D73, F132, F138).

> **Pamiętaj:** przykład kompletnego klastra (trzy węzły, manifesty Kubernetes z dzierżawą) to
> w wersji 1.0. Do tego czasu `TestCluster`, `ShardedOwnersSpec`, `ClusterDeliverySpec` i
> `RollingUpgradeSpec` w `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/` pokazuje okablowanie
> każdego węzła.

## Czego się nauczyłeś

- Każda instalacja działa jako klaster Pekko, składający się z jednego lub wielu węzłów, a wszystkie węzły współdzielą jeden węzeł.
  Baza danych PostgreSQL, której kontrola wersji chroni każdy zapis.
- Na każdy węzeł przypada magistrala zdarzeń. Każde zdarzenie musi zostać uruchomione raz (`Singletons.start`,
  `Projections.start`), a jego serwer proxy jest zarejestrowany na magistrali każdego węzła.
- Właścicielami typu mogą być „Lokalni” (jeden na węzeł, decyduje baza danych) lub „Sharded” (jeden w klastrze);
  zrób zajęte typy `Sharded` w klastrze składającym się z kilku węzłów i zmieniaj ich rozmieszczenie tylko za pomocą kropki.
- Węzeł opuszczający czeka do 90 sekund na polecenia w pracy; menedżer procesów musi czekać
  dłużej, a klienci próbują ponownie, używając kluczy idempotentnych.
- Wiadomości między węzłami przechodzą przez jeden serializator: zachowaj zgodność swoich klas z mpjsons, dodaj pola jako
  `Opcja` i utrzymuj wiadomości w rozmiarze znacznie poniżej 248 KiB.
- Uaktualnienia ciągłe: wersja aplikacji na wydanie, najstarszy węzeł na końcu i `writtenFrom` z
  `everyNodeRuns` dla nowych klas zdarzeń.

Dalej: [rozdział 18](18-production.md) przenosi to wszystko do produkcji: lista kontrolna uruchomienia,
obserwując działanie systemu i co robić przy dużym obciążeniu.