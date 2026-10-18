# Rozdział 17. Na wielu węzłach: praca w klastrze

Do tej pory twoja aplikacja mieszkała na jednej maszynie. W tym rozdziale przeprowadza się do wspólnego
mieszkania, czyli na kilka maszyn, zwanych **węzłami**, które pracują jak jedna. Pod koniec będziesz
wiedzieć, co się zmienia, gdy aplikacja działa na kilku węzłach, które jej części działają na każdym
węźle, a które dokładnie raz, jak zdecydować, gdzie mieszkają właściciele agregatów każdego typu, co może
podróżować między węzłami (i jak duże może być) oraz jak aktualizować węzły po jednym, nie przerywając
pracy aplikacji.

---

## Po co więcej niż jeden węzeł?

Wyobraź sobie ruchliwą restaurację z jednym kucharzem. Gdy kucharz robi przerwę, kuchnia staje. Zatrudnij
drugiego kucharza, a kuchnia będzie działać także w czasie przerw i w porze kolacji obsłuży więcej
stolików. Obaj kucharze korzystają z jednego zeszytu zamówień i obowiązuje ich jedna zasada: kartka
z zamówieniem jest w danej chwili w rękach tylko jednego kucharza.

Uruchomienie ReactiveCQRS na kilku węzłach daje ci te same trzy rzeczy:

- **Ciągłość działania.** Gdy jeden węzeł się restartuje albo maszyna pada, pozostałe obsługują dalej.
- **Więcej rąk do pracy.** Handlery poleceń, właściciele agregatów i projekcje dzielą się procesorami
  kilku maszyn.
- **Aktualizacje bez wywieszki „nieczynne”.** Węzły wymieniasz po jednym (to *aktualizacja krocząca*, pod
  koniec tego rozdziału).

A wiele zostaje dokładnie tak, jak było:

- **Jedna baza PostgreSQL** dla wszystkich węzłów, z tymi samymi tabelami co wcześniej.
- **Twój kod**: agregaty, polecenia, zdarzenia, projekcje i sagi z wcześniejszych rozdziałów.
- **Sprawdzanie wersji** (blokowanie optymistyczne z [rozdziału 1](01-cqrs-without-the-headache.md)).
  Baza danych nadal pozwala zapisać każdą wersję agregatu tylko jednemu piszącemu, cokolwiek dzieje się
  między węzłami.

> **Pamiętaj:** wszystkie węzły klastra dzielą jedną bazę danych. Klaster rozkłada *pracę*, a baza danych
> przechowuje *prawdę*.

## Każda instalacja jest klastrem, nawet jednowęzłowym

ReactiveCQRS działa na **Apache Pekko Cluster**. Klaster włączasz konfiguracją, a nie kodem. Oto
`application.conf` węzła instalacji złożonej z trzech węzłów, której system aktorów nazywa się `shop`
(`ActorSystem("shop", config)`):

```hocon
pekko {
  actor.provider = cluster
  actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }
  remote.artery.canonical {
    hostname = "10.0.0.11"     # this node's address
    port = 17355               # Pekko's default
  }
  cluster {
    seed-nodes = ["pekko://shop@10.0.0.11:17355", "pekko://shop@10.0.0.12:17355"]
    downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
    split-brain-resolver.active-strategy = keep-majority
    app-version = "1.4.0"      # your release's version; see "Rolling upgrades" below
  }
}
```

Cztery z tych linii zasługują na słowo wyjaśnienia:

- **`serialization-bindings`** kieruje każdy komunikat między węzłami przez serializator biblioteki (zob.
  „Komunikaty między węzłami” niżej). `reference.conf` biblioteki też ustawia to wiązanie, ale
  `reference.conf` Pekko wiąże `java.io.Serializable` z serializacją Javy, która jest wyłączona, a z dwóch
  plików `reference.conf` wygrywa ten, który jest wcześniej na ścieżce klas (classpath). Ścieżka klas
  spakowanej aplikacji może mieć dowolną kolejność, dlatego to wiązanie ustawia twoja konfiguracja. Węzeł,
  który może rozmawiać z innymi, ale serializowałby komunikaty inaczej, zapisuje w logu błąd, który o tym
  mówi, i pozostaje niegotowy.

- **`seed-nodes`** to adresy, z którymi kontaktuje się startujący węzeł, żeby dołączyć do klastra.
  Pasują do maszyn, których adresy się nie zmieniają. Na Kubernetesie dołączaj zamiast tego przez
  **Cluster Bootstrap** z Pekko Management, tak jak robi to E2 pod koniec tego rozdziału: pod po restarcie ma
  starą nazwę, ale nowy adres IP, a pierwszy węzeł seed, który tworzy nowy klaster, gdy żaden inny
  seed nie odpowie w ciągu pięciu sekund, może wtedy podzielić klaster podczas restartu kroczącego.
- **Split brain resolver** decyduje, co się dzieje, gdy sieć podzieli klaster na dwie grupy, które się
  nawzajem nie widzą: jedna strona zostaje, druga się wyłącza. `keep-majority` zostawia większą stronę.
  Przy dwóch węzłach nie ma większości, więc użyj strategii `lease-majority` z Pekko razem z dzierżawą,
  czyli lease (na Kubernetesie: dzierżawą Kubernetesa z Pekko Management): zostaje ta strona, która trzyma
  dzierżawę.
- **`app-version`** nazywa twoje wydanie. Ma znaczenie podczas aktualizacji kroczących.

Na jednej maszynie używasz dokładnie tych samych ustawień, a jedynym węzłem seed jest sam ten węzeł. To
**klaster jednowęzłowy**, który zachowuje się jak pojedynczy węzeł z wcześniejszych rozdziałów. Testy
samej biblioteki przechodzą, gdy każdy z nich działa na klastrze jednowęzłowym.

> **Wskazówka:** używaj ustawień klastra od pierwszego dnia, nawet na jednej maszynie. Późniejsze
> przejście z jednego węzła na trzy będzie wtedy zmianą konfiguracji, a nie kodu.

> **Pod maską:** „każde wdrożenie jest klastrem: jeden węzeł on-premise, kilka w chmurze” to decyzja D42;
> split brain resolver i dzierżawa to decyzja D46, którą sprawdza `ClusterBehaviourSpec`. Całą historię
> opisuje [ARCHITECTURE.md, sekcja 13](../ARCHITECTURE.md).

## Co działa na każdym węźle, a co tylko raz

We wspólnym mieszkaniu każdy ma własną szczoteczkę do zębów, ale cotygodniowe zakupy robi jedna osoba,
bo inaczej lodówka zapełni się pięcioma workami ziemniaków. W klastrze jest tak samo:

| Na każdym węźle | Raz w całym klastrze |
|---|---|
| generator identyfikatorów (`UidGeneratorActor.postgres(system)`) | każda projekcja zarządzana (`Projections.start`, [rozdział 10](10-rebuilding-projections.md)) |
| szyna zdarzeń (`EventsBusActor`) | poller outboxu (`OutboxPollerActor`) |
| szyna poleceń dla każdego typu agregatu (`AggregateCommandBusActor`) | scheduler squasha (`SquashScheduler`, [rozdział 14](14-squash.md)) |
| | każdy runner sagi ([rozdział 12](12-sagas.md)) |

Jedyny fakt, który trzeba zapamiętać: **każdy węzeł ma własną szynę zdarzeń.** Właściciel agregatu
publikuje zdarzenia na szynie węzła, na którym działa. Subskrybent zarejestrowany na szynie jednego węzła
widzi więc tylko zdarzenia właścicieli z tego węzła. Dlatego wszystko, co musi widzieć *każde* zdarzenie
(projekcja, saga, która nasłuchuje), działa w klastrze raz i jest zarejestrowane na szynie *każdego*
węzła.

### Jak uruchomić to, co działa raz: `Singletons.start`

`Singletons.start(system, name, props)` służy do uruchamiania czegoś, co musi działać raz. Wywołuje go
każdy węzeł, z tą samą nazwą. Pekko uruchamia wtedy aktora na **najstarszym** węźle klastra, a `start`
daje każdemu węzłowi **proxy**: `ActorRef`, który przekazuje wszystko, co do niego wyślesz, tam, gdzie
akurat działa aktor, i zachowuje nadawcę, więc odpowiedzi wracają do ciebie. Gdy najstarszy węzeł
odchodzi, aktor przenosi się na kolejny najstarszy, jeden raz; w czasie przeprowadzki proxy przetrzymuje
wysłane do niego komunikaty (do 1 000, tyle mieści bufor Pekko).

W systemie aktorów, który nie jest klastrem, `Singletons.start` po prostu sam uruchamia aktora, więc ten
sam kod działa w obu przypadkach.

`Projections.start` ([rozdział 10](10-rebuilding-projections.md)) robi to za ciebie: w klastrze
`ActorRef`, który zwraca, jest proxy tego węzła do nadzorcy projekcji.

Oto co na starcie uruchamia każdy węzeł E2, czyli przykładu z klastrem: koszyki z E1 (w shardach, zob.
następną sekcję), listę tych koszyków jako projekcję zarządzaną i poller outboxu:

<!-- snippet: e2/ClusterSetup.scala#node -->
```scala
// The same code on every node (system: this node's ActorSystem; cartContext: E1's carts, sharded, see below)
val mpjsons = new MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()

// One of each on every node
val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
val carts = system.actorOf(
  AggregateCommandBusActor(cartContext, uidGenerator, eventStore, responses, eventBus), "carts")

// Once in the cluster: every node starts them, one node runs them
val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions,
  new PostgresProjectionRegistry().initSchema(), contexts = Seq(cartContext)))
val cartsList: ActorRef = projections.start[CartsListProjection]("e2-carts-list", 1, "cartsList") { build =>
  new CartsListProjection(subscriptions,
    new PostgresDocumentStore[CartSummary](build.documentStore("e2_carts_list"), mpjsons, new NoopDocumentStoreCache))
}
Singletons.start(system, "outboxPoller", Props(new OutboxPollerActor(eventStore, eventBus)))

// Every node registers the projection's proxy on its own bus
eventBus ! RegisterSubscribers(Seq(cartsList))
```

Importy są na początku `ClusterSetup.scala`, w `examples/src/main/scala/io/reactivecqrs/examples/e2/`.

Co się dzieje, gdy to działa na trzech węzłach:

- Każdy węzeł ma własny generator identyfikatorów, szynę zdarzeń i szynę poleceń koszyków. Polecenie
  wysłane do szyny poleceń koszyków *dowolnego* węzła działa.
- Działa jeden nadzorca projekcji `e2-carts-list`, na najstarszym węźle. Szyna każdego węzła zna go przez
  proxy swojego węzła, więc projekcja dostaje zdarzenia właścicieli ze wszystkich węzłów.
- Działa jeden poller outboxu, także na najstarszym węźle.
- Runner sagi też działa raz: każdy węzeł wywołuje `SagaRunner.start(system, ...)`
  ([rozdział 12](12-sagas.md)) i wysyła zlecenia sagi do zwróconego proxy, z dowolnego węzła.

`ClusterSpec` z E2 uruchamia to na trzech węzłach w jednej JVM: koszyk zapełniany przez wszystkie węzły
ma jednego właściciela, a lista koszyków odpowiada na każdym węźle; gdy najstarszy węzeł odchodzi, lista
koszyków przenosi się i nadal dostaje zdarzenia opublikowane w międzyczasie. `ClusterDeliverySpec`
biblioteki sprawdza to samo, także z runnerem sagi.

> **Ostrzeżenie:** nigdy nie uruchamiaj pollera outboxu ani schedulera squasha zwykłym
> `system.actorOf` na każdym węźle. Przy trzech węzłach każde zdarzenie czekające w outboxie byłoby
> publikowane ponownie trzy razy, a każdy squash byłby próbowany trzy razy. Uruchamiaj je przez
> `Singletons.start`.

> **Pamiętaj:** rejestruj proxy projekcji na szynie *każdego* węzła. Projekcja zarejestrowana tylko na
> jednym węźle widziałaby zdarzenia jednego węzła i po cichu gubiłaby resztę.

Kilka rzeczy działa trochę inaczej, gdy coś przeniesie się na inny węzeł:

- **Odpowiedzi sag przychodzą co najmniej raz.** Runner sagi zapisuje, kto pytał, razem z adresem
  węzła pytającego, więc odpowiedź dociera do wywołującego na dowolnym węźle. Jeśli runner przeniesie
  się między wysłaniem odpowiedzi a jej odnotowaniem, następny runner może odpowiedzieć jeszcze raz:
  długo żyjący wywołujący powinien traktować drugą odpowiedź dla tej samej sagi jako powtórkę.
- **Subskrypcje push** projekcji subskrybowalnych ([rozdział 11](11-subscribable-projections.md))
  przepadają, gdy singleton projekcji się przenosi. Na odnowienie przychodzi wtedy odpowiedź
  `ProjectionSubscriptionsCancelled`: zasubskrybuj ponownie.

> **Pod maską:** szyna pyta każdego zarejestrowanego subskrybenta o jego subskrypcje komunikatem
> `GetSubscriptionRequests(registered)`, a w imieniu proxy odpowiada stojący za nim nadzorca. Gdy
> singleton się przenosi, jego instancje się zatrzymują; szyna znów pyta proxy, a to, co opublikowano
> w międzyczasie, zostaje w outboxie, aż poller outboxu opublikuje to ponownie. Zob.
> [ARCHITECTURE.md, sekcja 13.4](../ARCHITECTURE.md) (D44, D46, D53, D55, F110, F129).

## Gdzie mieszkają właściciele: Local czy Sharded

Każdy agregat ma **właściciela**, czyli aktora, który przechowuje jego stan i wykonuje jego polecenia
([rozdział 1](01-cqrs-without-the-headache.md)). W klastrze dla każdego typu agregatu wybierasz, gdzie ci
właściciele mieszkają. Wróćmy do kartek z zamówieniami:

- **Local:** każdy kucharz, którego ktoś zapyta o stolik 7, bierze kopię jego kartki. Gdy dwóch kucharzy
  pisze na niej naraz, zeszyt zamówień (baza danych) rozstrzyga, kto był pierwszy.
- **Sharded:** każda kartka ma w kuchni jedno stanowisko. Kto przyjmie zamówienie dla stolika 7, zanosi
  je na to stanowisko.

### Local, czyli ustawienie domyślne

Przy `Placement.Local` każdy węzeł trzyma własnych właścicieli, dla agregatów, o które go pytano.
Koszyk 42 może mieć właściciela na węźle A *i* drugiego na węźle B. To bezpieczne: sprawdzanie wersji
w bazie danych pozwala zapisać każdą wersję tylko jednemu z nich.

Ma to jednak niewielki koszt. Na węźle, który nie jest w klastrze sam, właściciel nie wie, co zapisały
inne węzły, więc zanim poda stan albo wykona polecenie, porównuje swoją wersję z zapisaną (jedno
wyszukanie po kluczu), a jeśli zmienił go inny węzeł, czyta tylko nowe zdarzenia (albo, po cofnięciu lub
przepisaniu historii, cały agregat).

Bezczynnych właścicieli Local usypia szyna poleceń ich węzła. Nie częściej niż co 5 minut robi porządki:
gdy właścicieli jest ponad 200, zatrzymuje najdawniej używanych spośród bezczynnych od 2 minut, a każdy
właściciel bezczynny od 12 godzin zostaje zatrzymany; najwyżej 200 naraz, reszta sekundę później. Obie liczby to parametry
`AggregateCommandBusActor(...)`: `keepAliveLimit` (200) i `maxInactivityMillis` (12 godzin).

### Sharded: jeden właściciel agregatu w całym klastrze

Kontekst wybiera tę opcję, nadpisując `placement`. E2 umieszcza koszyki z E1 w shardach:

<!-- snippet: e2/ShardedCarts.scala#sharded -->
```scala
class ShardedCartContext extends ShoppingCartContext {
  override val placement: Placement = Placement.Sharded(numberOfShards = 30) // about ten per node of three
}
```

To cała zmiana w twoim kodzie. Teraz:

- **Każdy koszyk ma w całym klastrze jednego właściciela**, rozmieszczanego przez Pekko Cluster
  Sharding. Szyna poleceń każdego węzła uruchamia region shardów tego typu i przez niego wysyła
  właścicielom wszystko. Polecenie albo zapytanie wysłane do szyny poleceń koszyków dowolnego węzła trafia
  do tego jednego właściciela, gdziekolwiek on działa.
- **Shard agregatu** wynika z jego identyfikatora (identyfikator modulo `numberOfShards`). Pekko rozkłada
  shardy na węzły i przenosi je, gdy węzły dochodzą i odchodzą. Domyślnie jest 100 shardów; celuj
  w mniej więcej dziesięć shardów na węzeł największego klastra, jaki planujesz.
- **Bezczynnych właścicieli pasywuje ich shard** po 120 sekundach bez komunikatu (ustawienie Pekko
  `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout`).
- **System aktorów musi być klastrem** (`pekko.actor.provider = cluster`), jedno- lub wielowęzłowym.
- **Polecenia podróżują** do węzła właściciela, więc muszą zmieścić się w komunikacie między węzłami
  (zob. „Komunikaty między węzłami” niżej).

`PassivateAggregate(id)`, wysłany do szyny poleceń dowolnego węzła, usypia właściciela agregatu przy
obu rozmieszczeniach.

Oto co daje „jeden właściciel”, na przykładzie `ShardedOwnersSpec`: trzy węzły wysyłają do jednego
koszyka po 15 współbieżnych poleceń `AddItemConcurrently`. Wszystkie 45 kończą się sukcesem, koszyk
kończy na wersji 46, a jego właściciel działa na dokładnie jednym węźle.

### Jak wybrać

| | Local | Sharded |
|---|---|---|
| Właściciele jednego agregatu | po jednym na każdym węźle, którego o niego pytano | jeden w całym klastrze |
| Dwóch piszących naraz | możliwe; rozstrzyga sprawdzanie wersji | nie przy normalnej pracy |
| Dodatkowy koszt odczytu lub polecenia | jedno wyszukanie po kluczu, na węźle, który nie jest sam | droga do węzła właściciela, gdy ten działa gdzie indziej |
| Bezczynni właściciele | usuwani przy porządkach szyny poleceń | pasywowani przez shard po 120 s |
| Polecenia muszą zmieścić się w komunikacie między węzłami | nie | tak |

Prosta reguła:

- **Jeden węzeł:** zostań przy `Local`. Właściciel na węźle, który jest w klastrze sam, nie potrzebuje
  wyszukań.
- **Kilka węzłów, typ często czytany lub zapisywany:** ustaw `Sharded`.
- **Kilka węzłów, typ rzadko używany:** `Local` wystarczy.

> **Ostrzeżenie:** zmiana rozmieszczenia typu albo jego `numberOfShards` wymaga zatrzymania całego
> klastra, nigdy aktualizacji kroczącej. Gdy stare i nowe węzły działają obok siebie, agregat miałby
> właściciela Local *i* właściciela w shardzie, a właściciele w shardach nie sprawdzają magazynu zdarzeń,
> zanim odpowiedzą. Typy możesz przenosić do `Sharded` po jednym, za każdym razem z pełnym zatrzymaniem.

> **Pod maską:** rozmieszczenie od strony właścicieli to `Owners.scala` (`LocalOwners`,
> `ShardedOwners`); komunikaty do właścicieli w shardach podróżują jako `ToOwner(aggregateId, message)`.
> Zob. [ARCHITECTURE.md, sekcja 13.3](../ARCHITECTURE.md) (D43, D72, F131, F145).

## Gdy węzeł odchodzi

Kucharz, który idzie do domu, najpierw kończy danie na patelni, a dopiero potem przekazuje kartki.
Właściciel w shardzie robi to samo, gdy jego węzeł opuszcza klaster (albo gdy Pekko przenosi jego shard,
żeby wyrównać obciążenie):

1. Kończy polecenia, które właśnie wykonuje (przy przetwarzaniu szeregowym kolejka się opróżnia).
2. Odpowiada na zapytania, które na niego czekają, i zapisuje migawkę, jeśli typ robi migawki przy
   pasywacji ([rozdział 13](13-snapshots.md)).
3. Zatrzymuje się. Komunikaty dla jego sharda czekają w regionie shardów węzła, który je wysyła (Pekko
   buforuje do 100 000 komunikatów na region), i trafiają do nowego właściciela na innym węźle.

Kończenie trwa, więc `reference.conf` biblioteki daje na nie czas:

```hocon
pekko.cluster.sharding.handoff-timeout = 90 s
pekko.coordinated-shutdown.phases.cluster-sharding-shutdown-region.timeout = 90 s
```

Oba ustawienia są wyższe niż domyślny limit czasu polecenia, 60 sekund
(`AggregateContext.commandTimeout`). Pekko zatrzymuje właściciela siłą 5 sekund przed upływem
`handoff-timeout`.

> **Ostrzeżenie:** twój menedżer procesów musi czekać *dłużej* niż te 90 sekund, zanim zabije
> odchodzący węzeł (na Kubernetesie: `terminationGracePeriodSeconds`). Jeśli podniesiesz `commandTimeout`
> kontekstu, podnieś oba ustawienia powyżej niego, a okres karencji (grace period) powyżej nich.

Polecenie, które podczas przekazania czekało w buforze, wykonuje się później, niż je wysłano, a jego
60 sekund zaczyna biec, gdy dotrze do nowego właściciela. Wywołujący z krótkim limitem czasu może więc
zrezygnować z polecenia, które mimo to zostanie zastosowane. Lekarstwo znamy z
[rozdziału 5](05-command-flavors.md): **ponawiaj z kluczem idempotencji**, a polecenie zadziała raz,
niezależnie od tego, ile razy je wysłano.

`ShardedOwnersSpec` sprawdza to w trudnych warunkach: węzeł odchodzi, gdy jeden z jego właścicieli
wykonuje polecenie trwające dwanaście sekund, a dziesięć koszyków, których właściciele tam działają,
dostaje przez pozostałe węzły po pięć produktów, jeden co 150 milisekund. Każde polecenie się udaje, także
to wolne, a każdy produkt jest w swoim koszyku raz.

A co, jeśli węzeł nie odejdzie grzecznie, tylko zniknie? Split brain resolver to zauważy i usunie go
z klastra, a jego shardy wystartują od nowa na pozostałych węzłach. Przez krótką chwilę mogą istnieć
dwaj właściciele jednego agregatu; sprawdzanie wersji w bazie danych nadal pozwala zapisać każdą wersję
tylko jednemu z nich.

### Co pokazują testy awarii E2

Do E2 dołączone są testy awarii, które robią to wszystko prawdziwym procesom; leżą
w `examples/deploy/e2/faults`. Trzy węzły działają w Dockerze, każdy z klientem, który tworzy koszyki
i dodaje produkty do koszyków wspólnych dla wszystkich węzłów. Każde polecenie niesie klucz idempotencji;
klient prowadzi rejestr tego, co wysłał i jaką dostał odpowiedź, a gdy odpowiedź zaginie, wysyła
polecenie jeszcze raz, z tym samym kluczem. Pod takim obciążeniem skrypt:

1. zabija węzeł (`docker kill`) i minutę później uruchamia go ponownie;
2. zatrzymuje węzeł w uporządkowany sposób i uruchamia go ponownie, więc shardy przenoszą się gdzie
   indziej i wracają;
3. odcina najstarszy węzeł, na którym działają lista koszyków i poller outboxu, od dwóch pozostałych,
   choć nadal ma on dostęp do bazy danych;
4. zabija najstarszy węzeł.

Potem porównuje bazę danych z rejestrami. Z katalogu głównego repozytorium:

```bash
sbt examples/stageE2
```

```bash
examples/deploy/e2/faults/run.sh
```

Dwa przebiegi na jednej maszynie (18/10/2026, Docker Desktop na Windowsie; pomiar, a nie benchmark)
wysłały po około 100 000 poleceń w dziewięć minut i każde sprawdzenie wypadło pomyślnie. Każde
polecenie, na które przyszła odpowiedź o sukcesie, jest zapisane raz, w swoim koszyku; żadne z 18 poleceń
wysłanych ponownie po zagubionej odpowiedzi nie zadziałało dwa razy; żadna wersja agregatu nie została
zapisana dwa razy; żaden koszyk nie dostał szóstego produktu; a lista koszyków nadążyła za każdym
koszykiem. Czasy pokazują, czego się spodziewać:

- **Agregaty węzła, który zniknął, czekają niecałe 50 sekund.** Pozostałe węzły zauważają to w ciągu
  6–8 sekund, split brain resolver decyduje 20 sekund później (jego `stable-after`), a shardy
  i singletony węzła startują gdzie indziej po kolejnych 20 sekundach. Do tego czasu polecenia dla jego
  agregatów czekają albo przekraczają limit czasu u nadawców i są wysyłane ponownie.
- **Odcięty węzeł sam się wyłącza.** Najstarszy węzeł, odcięty, w tej samej chwili co dwa pozostałe
  uznał, że jest po mniejszej stronie, zatrzymał swoje singletony i zakończył proces; singletony
  wystartowały na kolejnym najstarszym węźle 21 sekund później. Ani przez chwilę nie działały dwa naraz.
- **Outbox pokrywa to, czego zniknięty węzeł nie dostarczył.** Węzeł zabity tuż po zatwierdzeniu swoich
  transakcji zostawił dwa lub trzy zdarzenia zapisane, ale niedostarczone. Poller outboxu opublikował je
  ponownie mniej więcej trzy i pół minuty później (czeka 3 minuty, a outbox sprawdza co 30 sekund). Gdy
  zabity węzeł był najstarszy, poller przeniósł się razem z innymi singletonami, a nowy poller przy
  pierwszym sprawdzeniu publikuje ponownie to, co zastanie w outboxie: mniej więcej minutę po zabiciu.

> **Pamiętaj:** przy ustawieniach domyślnych klient, który przez minutę ponawia polecenie z jego kluczem,
> przetrwa utratę węzła. Jeśli klient podda się wcześniej, dostaniesz błędy dla agregatów, które tylko się
> przenoszą.

## Komunikaty między węzłami

Wewnątrz jednego węzła komunikat podaje się jak karteczkę przez biurko. Między węzłami trzeba go spisać,
włożyć do koperty i wysłać pocztą, czyli **zserializować**. A koperta ma limit rozmiaru.

### Jeden serializator do wszystkiego

Jeden serializator, `ClusterSerializer`, jest związany z `java.io.Serializable`, więc przechodzą przez
niego case classy i kolekcje Scali, które wędrują między węzłami (kilka z nich Pekko obsługuje własnymi
serializatorami, na przykład `Some` i `None`). Wiąże go `reference.conf` biblioteki, a także twoja
konfiguracja (jej pierwszy punkt, powyżej): to, który `reference.conf` wygra, zależy od kolejności na
ścieżce klas. Serializacja Javy pozostaje wyłączona (to domyślne ustawienie Pekko). Poza tą jedną linijką
niczego nie konfigurujesz.

- Komunikaty samej biblioteki są zapisywane pole po polu; zdarzenia w dostarczeniach zajmują po około
  33 bajty poza swoim JSON-em.
- **Twoje wartości** (polecenia, zdarzenia, stany agregatów, komunikaty zapytań i odpowiedzi na nie,
  zlecenia i odpowiedzi sag) są zapisywane przez mpjsons jako JSON swojej klasy, tak samo jak zapisuje się
  zdarzenia.
- Wyjątki biblioteki, takie jak `AggregateInIncorrectVersionException`, zachowują swoją klasę. Każdy inny
  wyjątek dociera jako `RemoteFailure`, z oryginalną nazwą klasy i komunikatem.

Z tego wynikają cztery reguły dla twoich klas:

1. **mpjsons musi umieć je zapisać i odczytać na podstawie ich klasy:** żadnych pól, których typem jest
   `Any`, trait, `ActorRef` albo `Throwable`.
2. **Definiuj komunikaty na najwyższym poziomie albo w obiekcie, nigdy wewnątrz klasy aktora.** Klasa
   zdefiniowana wewnątrz innej niesie ze sobą instancję klasy, która ją otacza, a tej nie da się przenieść
   na inny węzeł.
3. **Pole dodane później musi być typu `Option`.** Podczas aktualizacji kroczącej węzeł starszej wersji
   wysyła komunikat bez nowego pola, a odbiorca wypełnia brakujące pole wartością `None`, zerem albo
   `null`, nigdy domyślną wartością pola ze Scali. Odczytaj `{"id":1}` jako `Item(id: Int, name: String)`,
   a dostaniesz `Item(1, null)`.
4. **Pole, które zmienia typ, wymaga nowej klasy.**

> **Wskazówka:** test, który przepuszcza twoje komunikaty przez serializator tam i z powrotem, to kilka
> linijek; [rozdział 19](19-testing.md) pokazuje taki test. Jeśli masz zarejestrowane własne konwertery
> mpjsons, przekaż serializatorowi klastra instancję skonfigurowaną tak samo, przez
> `ClusterSerialization(system).mpjsons`, zanim węzły zaczną wymieniać komunikaty.

### Jak duży może być komunikat

Transport Pekko (Artery) wysyła komunikat w jednej ramce, domyślnie o rozmiarze 256 KiB (ćwierć
megabajta), ustawianym przez `pekko.remote.artery.advanced.maximum-frame-size`. Większy komunikat Pekko
odrzuca i tylko zapisuje w logu „Discarding oversized payload”; nadawca nigdy się o tym nie dowiaduje.

Limit biblioteki, `MessageSize(system).limit`, to ta ramka minus 8 KiB na własny nagłówek Pekko:
domyślnie 248 KiB. Oto co się dzieje z każdym rodzajem komunikatu, który mógłby być za duży:

| Komunikat | Kiedy przechodzi między węzłami | Za duży? |
|---|---|---|
| polecenie typu `Sharded` | jego właściciel działa na innym węźle | od razu odrzucone z `FailureResponse` („too large to be sent between nodes”), gdziekolwiek działa jego właściciel |
| odpowiedź na polecenie, odpowiedź na `GetAggregate` (stan) | właściciel działa na innym węźle | pytający od razu dostaje błąd `MessageTooLargeException` |
| odpowiedź projekcji na zapytanie | projekcja działa na innym węźle | pytający od razu dostaje błąd `MessageTooLargeException` |
| dostarczenie kilku zdarzeń | subskrybent działa na innym węźle | dzielone na części o rozmiarze najwyżej połowy limitu |
| pojedyncze zdarzenie albo sam stan w dostarczeniu | subskrybent działa na innym węźle | błąd u subskrybenta |
| `GetEventsForAggregate` (cała historia) | nigdy | czytane na pytającym węźle, z magazynu zdarzeń |

Każdy za duży komunikat trafia do logu węzła, który go wysyła, z klasą i rozmiarem (najwyżej raz na
10 sekund dla każdej klasy), i jest liczony w metryce `reactivecqrs.message.too.large`
([rozdział 18](18-production.md)).

Żeby wyczuć rozmiary, oto pomiary z 17/10/2026 (raport `load-M5` w `docs/perf/`): dostarczenie jednego
zdarzenia zajęło 561 bajtów; część ponownej publikacji z 1 000 zdarzeń z notatkami po 200 znaków zajęła
470 KiB i właśnie dlatego takie części są dzielone.

Co z tym zrobić:

- **Utrzymuj polecenia, odpowiedzi, stany i odpowiedzi na zapytania wyraźnie poniżej 248 KiB.** Stan,
  który subskrybenci dostają z każdym zdarzeniem, podróżuje z każdym zdarzeniem.
- **Duży stan czytaj na pytającym węźle**, przez `GetAggregateAtInstant(id, Instant.now)`: szyna
  poleceń odtwarza go z magazynu zdarzeń na miejscu, zamiast żeby właściciel przesyłał go z innego węzła.
- **Jeśli większych komunikatów nie da się uniknąć**, podnieś
  `pekko.remote.artery.advanced.maximum-frame-size`. Pekko trzyma w pamięci bezpośredniej (direct memory)
  do 128 buforów o rozmiarze ramki, więc ramki 1 MiB mogą zająć do 128 MiB (to szacunek z tego
  rachunku). Możesz też poprowadzić ruch do adresatów dużych komunikatów przez
  `large-message-destinations` z Pekko (domyślnie ramki 2 MiB, 32 bufory).

Więcej o dużych zdarzeniach i dokumentach znajdziesz w [rozdziale 18](18-production.md).

> **Pod maską:** serializator, jego tolerancyjny odczyt i strażnika rozmiaru opisuje
> [ARCHITECTURE.md, sekcja 13.1](../ARCHITECTURE.md) (D45, D68, F121, F135); zmierzone rozmiary
> i zalecenia są w [SCALING.md, sekcja 6](../SCALING.md).

## Czy ten węzeł jest gotowy?

Węzeł, który dopiero wystartował, nie powinien przyjmować ruchu, zanim jego szyna zdarzeń nie pozna
wszystkich swoich subskrybentów: dopóki każdy zarejestrowany subskrybent nie odpowie (łącznie
z projekcjami za ich proxy), szyna niczego nie publikuje.

Śledzi to `Readiness(system)`:

<!-- snippet: cart/NodeVersionsSpec.scala#readiness -->
```scala
import io.reactivecqrs.core.cluster.Readiness

Readiness(system).ready        // false while an event bus still waits for its subscribers
Readiness(system).waitingFor   // the event buses that still wait
```

Węzeł nie zostaje niegotowy na zawsze. Subskrybent, który nie odpowiedział w czasie
`registrationTimeout` szyny (2 minuty, w `DeliveryOptions`), zostaje pominięty: szyna startuje
i loguje błąd, który go wymienia. Dostarcza do pozostałych, ale żadne zdarzenie nie opuszcza outboxu,
dopóki brakujący subskrybent nie odpowie; wtedy właściciele publikują ponownie to, co zachowali.
Subskrybent, który zatrzyma się, zanim odpowie, zostaje pominięty tak samo, od razu.

Jeśli używasz health checków z Pekko Management (dodaj Pekko Management do swojej aplikacji; biblioteka
go nie dołącza), jedna linia zamienia tę informację w sprawdzenie gotowości (readiness check), więc load
balancery kierują ruch tylko do gotowych węzłów:

```hocon
pekko.management.health-checks.readiness-checks {
  reactivecqrs = "io.reactivecqrs.core.cluster.ReadinessCheck"
}
```

`Members(system)` mówi, co ten węzeł wie o członkach klastra:

<!-- snippet: cart/NodeVersionsSpec.scala#members -->
```scala
import io.reactivecqrs.core.cluster.Members

val members = Members(system)
members.oldestAppVersion   // the lowest app-version among the members, e.g. Some(Version("1.0.0"))
members.olderVersionsRun   // true while a member runs an older version than this node
members.alone              // true outside a cluster, and in a cluster of one
```

## Aktualizacje kroczące

Wymiana kucharzy po jednym w trakcie kolacji to nic złego, pod warunkiem że nikt nie wpisze na kartkę
z zamówieniem nowego dania, zanim każdy kucharz nie będzie umiał go przeczytać. Na tym polega cała idea
aktualizacji kroczącej.

Kroki:

1. **Nadaj każdemu wydaniu własną wersję aplikacji (app-version)**, na każdym węźle tego wydania:
   `pekko.cluster.app-version = "2.0.0"`.
2. **Uruchom węzły nowej wersji.** Dołączają do klastra. Nowe shardy trafiają tylko do węzłów najnowszej
   wersji, więc praca przechodzi ze starych węzłów na nowe, nigdy z powrotem.
3. **Zatrzymuj stare węzły po jednym, najstarszy na końcu.** To, co działa raz, mieszka na najstarszym
   węźle, więc przenosi się tylko raz.
4. **Niech klienci ponawiają.** Klient wysyła do węzłów, które działają; polecenie, na które w porę nie
   przyjdzie odpowiedź, wysyła ponownie przez inny węzeł, z tym samym kluczem idempotencji.

### Nowa klasa zdarzenia: zapisuj ją dopiero, gdy każdy węzeł umie ją przeczytać

Załóżmy, że wersja 2.0.0 koszyków zapisuje, skąd pochodzi każdy produkt, w nowej klasie zdarzenia.
Węzły 1.0.0 nigdy o niej nie słyszały. Gdyby węzeł 2.0.0 zapisał takie zdarzenie, gdy węzeł 1.0.0
jeszcze działa, właściciele i projekcje starego węzła nie umieliby go odczytać.

Dlatego mówisz kontekstowi, od której wersji każdy węzeł umie przeczytać nową klasę, a do tego czasu
handler zapisuje stare zdarzenie. Oto wersja 2.0.0 koszyków z domeny testowej (`CartsV2.scala`,
w `examples/src/test/scala/io/reactivecqrs/examples/cart/`; `NodeVersionsSpec` uruchamia ją na węzłach
obu wersji, a `ShardedCartContextV2` ze speców klastra robi to samo dla koszyków w shardach):

<!-- snippet: cart/CartsV2.scala#contextV2 -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.testdomain.shoppingcart._

case class ItemAddedV2(name: String, source: String) extends Event[ShoppingCart]

class ShoppingCartContextV2 extends ShoppingCartAggregateContext {
  override val writtenFrom: Map[Class[_], String] = Map(classOf[ItemAddedV2] -> "2.0.0")

  override def commandHandlers = cart => {
    case c: AddItemConcurrently if everyNodeRuns("2.0.0") => CommandSuccess(ItemAddedV2(c.name, "version 2"))
    case other => super.commandHandlers(cart)(other) // ItemAdded, as in 1.0.0
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case ItemAddedV2(name, _) => EventsHandlers.itemAdded(cart, ItemAdded(name))
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }
}
```

Co się dzieje podczas aktualizacji:

- Dopóki członkiem klastra jest węzeł 1.0.0, `everyNodeRuns("2.0.0")` zwraca false, a handler zapisuje
  `ItemAdded`, jak wcześniej.
- Jeśli coś mimo to spróbuje zapisać `ItemAddedV2`, właściciel odpowie na polecenie `CommandRejected`
  (wyślij je ponownie później); na squash, którego zdarzenie squasha jest takiej klasy, przyjdzie
  odpowiedź `SquashRefused`.
- Gdy ostatni węzeł 1.0.0 odejdzie, zapisywane jest `ItemAddedV2`.

Handlery zdarzeń *i* obsługę nowej klasy w projekcjach wypuszczaj w tym samym wydaniu, które wpisuje ją
do `writtenFrom`.

`RollingUpgradeSpec` robi to wszystko pod obciążeniem: dwa węzły 1.0.0, dwa węzły 2.0.0 dołączające
później, polecenia z każdego węzła. Dopóki stary węzeł jest członkiem klastra, nie zostaje zapisane
żadne `ItemAddedV2`; zapisuje się je, gdy oba stare węzły odejdą; żadne polecenie nie ginie ani nie
zostaje zastosowane dwa razy, a projekcja ma każdy produkt raz.

Jeszcze kilka przepisów ruchu drogowego:

- **Ustaw wersje aplikacji, zanim zaczniesz polegać na `writtenFrom`.** Gdy żaden członek klastra nie
  ma ustawionej wersji (wszyscy mają domyślne `0.0.0` z Pekko), każda klasa jest zapisywana od razu;
  członek bez wersji liczy się jako najniższa wersja, więc dopóki działa, nic nowego nie jest zapisywane.
- **Węzeł, który jeszcze nie zna swojego klastra, nie zapisuje niczego nowego.**
- **Nigdy nie cofaj się poniżej wersji, której nowe klasy zdarzeń zostały zapisane.** Starsza wersja nie
  umie ich przeczytać.
- **Nowa klasa polecenia:** wysyłaj ją dopiero, gdy każdy węzeł działa w jej wersji, bo właściciel na
  starym węźle by jej nie znał.
- **Projekcja albo saga dodana w nowej wersji:** jej singleton działa na najstarszym węźle, który
  działa w starej wersji, dopóki nie zostanie wymieniony jako ostatni. Szyny nowych węzłów czekają na
  nią `registrationTimeout`, potem startują bez niej, więc aktualizacja idzie dalej. Ich zdarzenia
  zostają w outboxie, dopóki singleton nie wystartuje na nowym węźle i nie odpowie; wtedy są
  publikowane ponownie. Projekcja zarządzana i tak buduje się z magazynu.
- **Projekcja usunięta w nowej wersji:** `Projections.retire(name)` czeka, aż każdy członek klastra
  będzie działał w tej wersji, zanim usunie projekcję (sprawdza ponownie co 10 sekund).
- **Zmiany rozmieszczenia** nadal wymagają pełnego zatrzymania (zob. wyżej).
- **Klaster, który działa na wersji milestone** (1.0.0-M5 lub -M6), przechodzi na 1.0.0 z pełnym
  zatrzymaniem: 1.0 zapisuje zdarzenia dostarczenia w postaci, której tamte wersje nie umieją odczytać.

> **Pod maską:** wersje aplikacji, przełącznik zapisu, gotowość i test aktualizacji kroczącej opisuje
> [ARCHITECTURE.md, sekcja 13.6](../ARCHITECTURE.md) (D65–D73, F132, F138).

## Trzy węzły na serio: Docker Compose i Kubernetes

E2 ma w `examples/deploy/e2/` wszystko, czego trzeba, żeby uruchomić jego węzły jako osobne procesy:

| Plik | Co to jest |
|---|---|
| `Dockerfile` | obraz ze środowiskiem uruchomieniowym Javy 21 i jarami przykładów, który uruchamia `ClusterSetup` z E2 |
| `docker-compose.yml` | trzy węzły i PostgreSQL na jednej maszynie |
| `kubernetes.yaml` | trzy węzły na Kubernetesie, z dzierżawą dla split brain resolvera |

Każdy węzeł uruchamia ten sam `ClusterSetup.main`, a to, czym węzły się różnią, bierze ze swojego
środowiska: `NODE_HOST` (swój adres), `CONTACT_POINTS` (węzły, które pyta najpierw, przy Docker
Compose), `REQUIRED_CONTACT_POINTS` (ile z nich musi odpowiedzieć, zanim powstanie nowy klaster),
`APP_VERSION`, bazę danych (`DB_URL`, `DB_USER`, `DB_PASSWORD`) i `SHOP_CONFIG`, czyli konfigurację
wyjściową:

<!-- snippet: e2/ClusterSetup.scala#config -->
```scala
/** A node's configuration: `e2.conf` (or `e2-kubernetes.conf`, as SHOP_CONFIG says), with this node's address, its
  * contact points for Cluster Bootstrap, how many of them must answer before a new cluster forms, and the release's
  * version, from the environment. Alone, a node is its own contact point: a cluster of one. */
def nodeConfig(setting: String => Option[String]): Config = {
  val host = setting("NODE_HOST").getOrElse("127.0.0.1")
  val contactPoints = setting("CONTACT_POINTS").fold(Seq(host))(_.split(",").map(_.trim).toSeq)
  val required = setting("REQUIRED_CONTACT_POINTS").getOrElse(contactPoints.size.toString)
  ConfigFactory.parseString(
    s"""pekko.remote.artery.canonical.hostname = "$host"
       |pekko.management.http.hostname = "$host"
       |pekko.discovery.config.services.shop.endpoints = [${contactPoints.map(p => s"""{host = "$p", port = 8558}""").mkString(", ")}]
       |pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr = $required
       |pekko.cluster.app-version = "${setting("APP_VERSION").getOrElse("1.0.0")}"
       |""".stripMargin)
    .withFallback(ConfigFactory.load(setting("SHOP_CONFIG").getOrElse("e2")))
}
```

Obie konfiguracje są w `examples/src/main/resources`:

- **`e2.conf`**, dla Docker Compose: provider klastra na porcie 17355, split brain resolver
  z `keep-majority` (trzy węzły zawsze mają większość), `coordinated-shutdown.exit-jvm = on` (węzeł,
  który opuszcza klaster albo zostaje z niego usunięty, kończy swój proces, a kontener startuje
  ponownie), Pekko Management na porcie 8558 z health checkami (wśród nich `ReadinessCheck` biblioteki)
  i Cluster Bootstrap ze stałą listą punktów kontaktowych.
- **`e2-kubernetes.conf`**, dla Kubernetesa: to samo, ale punkty kontaktowe są znajdowane przez API
  Kubernetesa (pody z etykietą `app=shop`), a strategią jest `lease-majority`: przy podziale zostaje ta
  strona, która trzyma Kubernetes Lease, także przy dwóch węzłach, gdzie nie ma większości. Dzierżawa to
  natywny obiekt Kubernetesa `Lease`, nazwany od systemu aktorów (`shop-pekko-sbr`), pochodzący
  z `pekko-lease-kubernetes` z Pekko Management.

`main` uruchamia Pekko Management, a potem `ClusterBootstrap(system).start()`; węzłów seed nie ma
w konfiguracji, bo gdy są, Bootstrap nie działa. Startujący węzeł pyta każdy znaleziony punkt
kontaktowy, do jakiego klastra należy, i dołącza do tego, który mu wskażą. Dopiero gdy wszystkie wymagane
punkty kontaktowe zgłoszą, że nie należą do żadnego, tworzą nowy klaster, zaczynając od tego
z najniższym adresem.

Manifest Kubernetesa ma cztery części, które warto znać:

1. **Konto usługi (service account) z rolą**: `get`, `watch` i `list` na `pods`, żeby Bootstrap znalazł
   punkty kontaktowe, oraz `get`, `create`, `update` i `list` na `leases` z `coordination.k8s.io`, żeby
   split brain resolver mógł wziąć dzierżawę. Bez dzierżawy przy podziale nie zostaje żadna strona.
2. **IP poda jako adres węzła** (`NODE_HOST` z `status.podIP`): pod po restarcie to nowy węzeł z nowym
   adresem, a żaden inny węzeł nie trzyma starego adresu w pamięci podręcznej.
3. **StatefulSet z trzema podami uruchamianymi równolegle**: utworzenie pierwszego klastra wymaga trzech
   punktów kontaktowych. Aktualizacja krocząca i tak wymienia pody po jednym, od najnowszego, każdy
   dopiero wtedy, gdy poprzedni jest gotowy.
4. **Sondy (probes) na Pekko Management**: `/ready` trzyma ruch z dala, dopóki szyna zdarzeń węzła nie
   pozna swoich subskrybentów; `terminationGracePeriodSeconds: 120` daje odchodzącemu węzłowi czas na
   przekazanie właścicieli (90 sekund, jak wyżej).

Na Kubernetesie z Docker Desktop trzy pody E2 utworzyły w ten sposób jeden klaster i utrzymały go przez
restart kroczący, a gdy jeden węzeł przestał odpowiadać (jego proces zamrożono, a nie zatrzymano), split
brain resolver wziął dzierżawę i usunął go z klastra; węzeł po odmrożeniu odkrył, że został usunięty,
zakończył działanie i dołączył ponownie, gdy jego kontener wystartował od nowa.

> **Ostrzeżenie:** lokalny klaster zachowuje obraz, który raz pobrał. Po zbudowaniu nowego obrazu nadaj
> mu nowy tag, inaczej pody dalej będą uruchamiać stary.

Żeby wypróbować to na jednej maszynie, uruchom w katalogu głównym repozytorium:

```bash
sbt examples/stageE2
docker compose -f examples/deploy/e2/docker-compose.yml up --build
```

`stageE2` kopiuje jary przykładów, razem ze wszystkimi bibliotekami, których potrzebują, do
`examples/target/e2/lib`, a obraz uruchamia je przez `java -cp "/app/lib/*"`: w obrazie nie ma sbt i nie
trzeba sięgać do żadnego repozytorium. Każdy węzeł wypisuje koszyk, który utworzył, i to, co mówi o nim
lista koszyków, gdziekolwiek ona działa. Zatrzymaj `node1`, najstarszy, a lista koszyków i poller
outboxu przeniosą się na `node2`; zrestartuj `node3`, a znów utworzy koszyk, odczytany z `node2`. Plik
Compose daje zatrzymywanemu węzłowi dwie minuty (`stop_grace_period: 2m`), więcej, niż może trwać
przekazanie; domyślne 10 sekund Dockera zabiłoby go w połowie.

> **Pamiętaj:** `java -cp "lib/*"` układa jary na ścieżce klas w przypadkowej kolejności i właśnie
> dlatego `e2.conf` sam wiąże serializator (pierwszy punkt konfiguracji omówionej w tym rozdziale).

## Co już umiesz

- Każda instalacja działa jako klaster Pekko, z jednym węzłem albo wieloma, a wszystkie węzły dzielą
  jedną bazę PostgreSQL, której sprawdzanie wersji pilnuje każdego zapisu.
- Każdy węzeł ma własną szynę zdarzeń. To, co musi widzieć każde zdarzenie, działa raz
  (`Singletons.start`, `Projections.start`), a jego proxy jest zarejestrowane na szynie każdego węzła.
- Właściciele typu są `Local` (po jednym na węźle, rozstrzyga baza danych) albo `Sharded` (jeden
  w klastrze); w klastrze z kilkoma węzłami ustawiaj zapracowane typy jako `Sharded`, a rozmieszczenie
  zmieniaj tylko przy pełnym zatrzymaniu.
- Odchodzący węzeł czeka do 90 sekund na polecenia w toku; twój menedżer procesów musi czekać dłużej,
  a klienci ponawiają z kluczami idempotencji.
- Węzeł, który zniknie albo zostanie odcięty, split brain resolver usuwa z klastra; jego agregaty
  i singletony startują gdzie indziej w ciągu minuty, a outbox dostarcza to, czego węzeł nie zdążył.
  Testy awarii E2 pokazują to pod obciążeniem: każde potwierdzone polecenie zapisane raz, nic nie
  zginęło.
- Komunikaty między węzłami przechodzą przez jeden serializator: dbaj, by twoje klasy były przyjazne dla
  mpjsons, nowe pola dodawaj jako `Option`, a komunikaty utrzymuj wyraźnie poniżej 248 KiB.
- Aktualizacje kroczące: osobna wersja aplikacji (app-version) dla każdego wydania, najstarszy węzeł na
  końcu, a dla nowych klas zdarzeń `writtenFrom` z `everyNodeRuns`.

Dalej: [rozdział 18](18-production.md) zabiera to wszystko na produkcję: lista kontrolna na start,
obserwowanie działającego systemu i co robić pod dużym obciążeniem.
