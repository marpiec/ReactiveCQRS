# Rozdział 9. Kiedy w projekcjach coś pójdzie nie tak

Projekcje to kod, a kod się psuje: błąd w listenerze, baza danych, która na moment się zatnie,
projekcja wolniejsza od poleceń, za którymi podąża, maszyna, która pada milisekundę po zatwierdzeniu
transakcji. W tym rozdziale zobaczysz, co ReactiveCQRS obiecuje w sprawie dostarczania zdarzeń, co
robi samo, gdy coś pójdzie nie tak, i jakiej garści komunikatów używasz, żeby obejrzeć szkody i je
naprawić. Na koniec będziesz umieć czytać linie logu, zapytać projekcję, co jej dolega, i ruszyć z
miejsca koszyk, który utknął.

---

## Obietnica w prostych słowach

Wyobraź sobie listy polecone. Listonosz dzwoni do drzwi, aż ktoś pokwituje odbiór, a jeśli nikt tego
nie zrobi, przychodzi innego dnia. Każdy list od danego nadawcy ma numer. A domownicy prowadzą
notatnik: „z banku, ostatni otwarty list: numer 7”. List numer 7, który przyjdzie drugi raz, zostaje
rozpoznany i odłożony na bok. Gdy przychodzi list 9, a 8 nigdy nie dotarł, domownicy dzwonią do banku:
„prosimy jeszcze raz wysłać numer 8”.

Dokładnie to ReactiveCQRS obiecuje projekcji:

1. **Co najmniej raz.** Każde zdarzenie dociera do każdej projekcji, która słucha jego typu agregatu,
   nawet po awariach i restartach. Czasem więcej niż raz.
2. **Po kolei, w obrębie agregatu.** Zdarzenia koszyka 42 są stosowane w kolejności wersji. Między
   różnymi koszykami nie ma żadnej obietnicy: wersja 5 koszyka 7 może zostać zastosowana przed wersją
   2 koszyka 42, nawet jeśli ta druga została zatwierdzona wcześniej.
3. **Zastosowane raz.** Notatnik to tabela `subscriptions`: dla każdej projekcji, rodzaju listenera i
   agregatu ostatnia zastosowana wersja. Zapisuje się ją w tej samej transakcji co twój model odczytu.
   Dostarczenie z tą wersją albo niższą to duplikat: zostaje potwierdzone, ale nie jest stosowane
   ponownie.
4. **Bez dziur.** Wersja wyższa niż następna oczekiwana oznacza, że jakieś wersje zaginęły. Projekcja
   prosi właściciela agregatu, żeby opublikował je ponownie, i gdy dotrą, stosuje wszystko po kolei.

> **Pamiętaj:** kolejność obowiązuje tylko w obrębie jednego agregatu. Model odczytu, który łączy kilka
> koszyków (powiedzmy, sumę po wszystkich koszykach), nie może zakładać, że zdarzenia różnych koszyków
> przychodzą w kolejności, w jakiej zostały zatwierdzone.

## Dlaczego listener musi być odporny na powtórki

„Zastosowane raz” dotyczy tego, co twój listener zapisuje przez swoją sesję: te zapisy są zatwierdzane
razem z notatnikiem albo razem z nim wycofywane. Wszystko inne, co robi listener, nie jest tym objęte
i może się wydarzyć więcej niż raz:

- aktualizacje nawet 100 koszyków dzielą jedną transakcję; gdy jeden koszyk zawiedzie, wszystkie
  zostają wycofane i każdy jest uruchamiany ponownie osobno;
- listener, który zawodzi, jest uruchamiany do dziewięciu razy (następna sekcja);
- listener `noDBSession` działa, zanim zostanie zapisana jego wersja, więc jeśli ten zapis się nie
  uda, zobaczy te same zdarzenia jeszcze raz;
- przebudowa (rozdział 10) odtwarza historię przez twoje listenery.

Dlatego e-maile, wywołania HTTP i wiadomości do innych aktorów trzymaj z dala od zwykłych listenerów.
Ich miejsce jest w sadze (rozdział 12) albo w osobnym listenerze, który używa `noDBSession` (bez
transakcji, nigdy w grupie z innymi koszykami), nic nie robi, dopóki `rebuilding` ma wartość true, i
jest idempotentny: zrobienie tego samego dwa razy musi być nieszkodliwe.

<!-- snippet: cart/CartDeletedMailer.scala#mailer -->
```scala
import io.reactivecqrs.core.projection.{AggregateListenerActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart.{ShoppingCart, ShoppingCartDeleted}

trait Mailer { def sendOnce(key: String, text: String): Unit } // your own service: one mail per key

class CartDeletedMailer(val subscriptionsState: SubscriptionsState, mailer: Mailer) extends AggregateListenerActor {
  override protected val projectionName: String = "CartDeletedMailer"

  protected val listeners = List(
    EventsListener.noDBSession[ShoppingCart] { (cartId, events) =>
      if (!rebuilding && events.exists(_.event == ShoppingCartDeleted()))
        mailer.sendOnce("cart-deleted-" + cartId.asLong, "Your cart " + cartId.asLong + " was deleted.")
    })
}
```

`AggregateListenerActor` to projekcja, której `receiveQuery` ignoruje każdą wiadomość: listener,
którego nie ma o co pytać. Uruchamiasz go i rejestrujesz jak każdą projekcję (rozdział 8).

## Gdy listener rzuca wyjątek

Zepsujmy coś celowo. Przyjmijmy, że nazwy koszyków na naszej liście muszą być unikalne, i dajmy jej
magazyn taki jak w rozdziale 8, ale z unikalnym indeksem (tutaj we własnej tabeli):

<!-- snippet: cart/FailuresSpec.scala#uniqueStore -->
```scala
val cartsStore = new PostgresDocumentStore[CartSummary]("unique_carts", mpjsons, new NoopDocumentStoreCache,
  Seq(UniqueTextIndex(1, Seq("name"))))
```

Bloki kodu z tego rozdziału działają w `FailuresSpec`
(`examples/src/test/scala/io/reactivecqrs/examples/cart/`), który opowiada tę historię z trzema
szybkimi ponowieniami zamiast dziewięciu powolnych. Alicja ma koszyk o nazwie „Zakupy”. Potem Bob
tworzy koszyk o tej samej nazwie. Stronie zapisu to nie przeszkadza, bo tam nazwy koszyków nie muszą
być unikalne, ale `insertDocument` naszego listenera dla koszyka Boba (koszyka 43) narusza indeks. Oto,
co dzieje się dalej:

1. Wstawienie rzuca wyjątek. Transakcja zostaje wycofana: nie ma dokumentu, a notatnik dla koszyka 43
   wciąż mówi „nic nie zastosowano”. W logu pojawia się ostrzeżenie:

   ```
   Projection CartsList failed on ShoppingCart 43 versions 1-1 (attempt 1), retrying: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint ...
   ```

2. Projekcja próbuje ponownie po 1, 2, 4, 8, 16, 30, 30 i 30 sekundach: dziewięć prób w mniej więcej
   dwie minuty. Późniejsze zdarzenia koszyka 43 czekają za tym, które zawodzi. Wszystkie inne koszyki
   idą dalej jak zwykle.
3. Jeśli któraś próba się powiedzie (na przykład po chwilowej czkawce bazy danych), na tym koniec:
   żadnego wpisu, nic do naprawiania.
4. Po dziewiątej nieudanej próbie projekcja daje za wygraną w sprawie koszyka 43. Dodaje wiersz do
   `subscription_failures` (wersję, której nie mogła zastosować, błąd i od kiedy) i zapisuje w logu
   błąd:

   ```
   Projection CartsList gave up on ShoppingCart 43 at version 1 (PSQLException: ERROR: duplicate key value ...); deliveries of it fail until it is retried or skipped
   ```

5. Od teraz na każde dostarczenie koszyka 43 odpowiada od razu `MessageFailed`, nawet nie próbując.
   Szyna zdarzeń liczy to jako odpowiedź (i za każdym razem zapisuje w logu ostrzeżenie), więc wiersze
   outboxu koszyka 43 są usuwane jak zwykle. Teraz to wiersz w `subscription_failures` przypomina o
   tym, co zostało do zrobienia.

Jeszcze jeden przypadek: jeśli nie da się zapisać nawet samej awarii, bo nie działa baza danych,
projekcja nic nie odpowiada i co 30 sekund próbuje jeszcze raz z tą samą paczką, aż albo ją zastosuje,
albo zapisze awarię. A błąd, który jest winą bazy danych, a nie listenera (utracone albo odrzucone
połączenie, transakcja wycofana przez serwer, serwerowi brakuje zasobów), w ogóle nie liczy się jako
próba: paczka jest ponawiana co 30 sekund, dopóki bazy nie ma, z jednym ostrzeżeniem na minutę, i
restart bazy danych nie zostawia żadnego zapisu awarii.

> **Pamiętaj:** awaria dotyczy jednej projekcji, jednego rodzaju listenera i jednego agregatu. Koszyk
> 43 utknął w `CartsList`; ten sam koszyk w innych projekcjach i wszystkie inne koszyki w tej projekcji
> mają się dobrze.

> **Wskazówka:** model odczytu to złe miejsce na reguły biznesowe. O zmianie dowiaduje się dopiero
> wtedy, gdy jest już zapisana, więc jedyne, co może zrobić, to zawieść. Reguły należą do strony
> zapisu, gdzie polecenie wciąż można odrzucić.

## Oględziny szkód: GetProjectionStatus

Zapytaj samą projekcję:

<!-- snippet: cart/FailuresSpec.scala#status -->
```scala
import io.reactivecqrs.core.projection.{GetProjectionStatus, ProjectionStatus}

val status = Await.result((cartsList ? GetProjectionStatus).mapTo[ProjectionStatus], 5.seconds)
status.failures.foreach { f =>
  println(f.aggregateType.simpleName + " " + f.aggregateId.asLong + " stuck at version " + f.version.asInt + ": " + f.error)
}
// ShoppingCart 43 stuck at version 1: PSQLException: ERROR: duplicate key value violates unique constraint ...
```

Odpowiedź to `ProjectionStatus(projectionName, failures, retrying, waitingForGap)`:

- **`failures`**: po jednym `SubscriptionFailure` na każdą subskrypcję, która utknęła, z jej
  `subscriptionType` (który rodzaj listenera), `aggregateType`, `aggregateId`, `version` (pierwsza
  niezastosowana wersja), `error` i `since`;
- **`retrying`**: koszyki, których listener zawiódł i jest właśnie ponawiany (krok 2 powyżej);
- **`waitingForGap`**: koszyki czekające na brakujące wersje (dalej w tym rozdziale).

Trzy puste listy znaczą, że wszystko w porządku. Awarie są przechowywane w tabeli
`subscription_failures`, więc przetrwają restart: koszyk, który utknął, stoi w miejscu, dopóki ktoś
czegoś z tym nie zrobi. Żeby odpowiedź zawsze mogła do ciebie dotrzeć, także z innego węzła klastra,
zawiera 100 najstarszych awarii, z błędem każdej przyciętym do 500 znaków, a `failuresTotal` mówi, ile
ich jest; dwie pozostałe listy mają najwyżej po 1 000 koszyków.

> **Wskazówka:** podepnij `GetProjectionStatus` każdej projekcji pod panel administracyjny albo health
> check. Więcej o obserwowaniu działającego systemu znajdziesz w rozdziale 18.

## Naprawa: ponowić czy pominąć

Najpierw usuń przyczynę. Tutaj zamieniamy unikalny indeks na zwykły, pod nowym numerem (przy starcie
magazyn usunie indeks 1 i utworzy indeks 2), i robimy restart:

<!-- snippet: cart/FailuresSpec.scala#indexSwap -->
```scala
Seq(MultipleTextIndex(2, Seq("name")))
```

Być może nic więcej nie trzeba robić: gdy projekcja startuje, zaraz po tym, jak szyna ją zarejestruje,
sama ponawia próby dla awarii zapisanych wcześniej, 20 na sekundę. Po tym restarcie koszyk 43 dostanie
więc swój dokument, a ty nie musisz nawet kiwnąć palcem. Gdy usuwasz przyczynę bez restartu albo
ponowienie przy starcie nie pomogło, to samo zrobisz ręcznie trzema komunikatami wysłanymi do
projekcji:

| Komunikat | Odpowiedź | Co robi |
|---|---|---|
| `RetryFailedSubscription(cartId)` | `FailedSubscriptionsHandled(cartId, count)` | prosi właściciela koszyka o wersję, na której projekcja utknęła, i wszystkie po niej, raz za razem, aż przyjdą; gdy ta wersja zostanie zastosowana, wpis o awarii jest usuwany |
| `RetryAllFailedSubscriptions` | `FailedSubscriptionsRetried(count)` | to samo dla każdej awarii projekcji, 20 na sekundę |
| `SkipFailedVersion(cartId)` | `FailedSubscriptionsHandled(cartId, count)` | oznacza wersję, na której projekcja utknęła, jako zastosowaną, choć jej nie stosuje, usuwa wpis i raz prosi o wersje po niej; przy awarii „brakuje wersji” niczego nie oznacza (tych wersji może jeszcze nie być) i prosi o nie ponownie |

`count` mówi, ile nieudanych subskrypcji obsłużono; 0 znaczy, że nie było nic do zrobienia.

<!-- snippet: cart/FailuresSpec.scala#retry -->
```scala
import io.reactivecqrs.core.projection.{FailedSubscriptionsHandled, RetryFailedSubscription}

val handled = Await.result((cartsList ? RetryFailedSubscription(cartId)).mapTo[FailedSubscriptionsHandled], 5.seconds)
// FailedSubscriptionsHandled(Id(43),1)
```

Gdy się uda, w logu pojawi się `Projection CartsList recovered from its failure on ShoppingCart 43`.
Jeśli listener nadal zawodzi, wracasz do kroku 1: ponowienia, a potem nowy wpis.

> **Ostrzeżenie:** pominięcie oznacza utratę czegoś, a czego, zależy od listenera. Listener stanu
> dostaje z następną wersją cały stan, więc jego dokument znów jest poprawny, gdy tylko koszyk się
> zmieni (o ile kiedykolwiek to nastąpi). Listener zdarzeń już nigdy nie zobaczy pominiętego zdarzenia:
> jego skutek przepada na dobre. Jest też pułapka na listenery stanu, które polegają na `created`:
> jeśli pominięta wersja to ta, która tworzyła dokument, następna wersja spróbuje wykonać
> `overwriteDocument` na dokumencie, który nie istnieje, i zawiedzie. Listener napisany z
> `updateDocument(0, cartId.asLong, _ => Document(summary))`, które wstawia dokument, gdy jeszcze nic
> nie ma, w nią nie wpadnie.

## Brakujące wersje

Czasem dostarczenie nigdy nie dociera: wiadomość ginie, gdy pada węzeł, albo projekcja dołącza dopiero
po pierwszych zdarzeniach koszyka (rozdział 8). Projekcja zauważa to, gdy przychodzi następne
dostarczenie: „jestem na wersji 3, a to jest wersja 5”. Wtedy:

1. Prosi właściciela koszyka, przez szynę, żeby ponownie opublikował wszystko od wersji 4, tylko dla
   tej projekcji. W logu widać `Projection CartsList asks for versions from 4 of ShoppingCart 42`.
   Wersja 5 czeka.
2. Gdy wersje dotrą, stosuje po kolei 4 i 5 i potwierdza dostarczenie, które czekało.
3. Gdy nie ma odpowiedzi, pyta znowu po 10 sekundach, potem po 20, 40 i tak dalej, aż do jednego razu
   na 5 minut.
4. Jeśli po 10 minutach wersje wciąż nie przyszły, zapisuje awarię na wersji 4 („versions from 4 are
   missing: asked … times in 10 minutes without an answer”), którą obsługujesz jak każdą inną awarię.

`ProjectionStatus.waitingForGap` wymienia koszyki, które są na etapie kroków od 1 do 3. Gdy właściciel
nie ma nic po tym, o co prosi projekcja (koszyk jest w wersji 3, a nieaktualny wpis poprosił o 4),
mówi o tym, a projekcja przestaje czekać; wpis awarii wersji, których nigdy nie było, zostaje wtedy
usunięty. Wpis awarii, której wersję projekcja już zastosowała (jego usunięcie nie powiodło się po
ponowieniu, które ją zastosowało), znika od razu po odczytaniu, zamiast stać się luką.

Gdy projekcja nie ma jeszcze nic z koszyka (jej przebudowa celowana go wyzerowała albo dodano ją
później), a przychodzi zdarzenie na żywo, właściciel najpierw daje jej historię koszyka. Ta historia
jest stosowana tak, jak stosuje ją przebudowa: `rebuilding` jest prawdą, więc projekcja subskrybowalna
niczego za nią nie wypycha (rozdział 11). Zdarzenie na żywo, które przyszło razem z nią, jest
stosowane tak samo; zdarzenia po nim są już na żywo.

## Opcje na wypadek awarii

Wszystko powyższe kształtują te opcje `ProjectionActorOptions` z rozdziału 8:

| Opcja (domyślnie) | Na co wpływa |
|---|---|
| `retries` (`Backoff(1.second, 30.seconds, maxAttempts = 9)`) | jak ponawiany jest zawodzący listener, zanim awaria zostanie zapisana: czekanie zaczyna się od pierwszego czasu i podwaja się aż do drugiego, łącznie przez tyle prób |
| `retryFailuresOnStart` (true) | czy przy starcie ponawiać próby dla awarii zapisanych przed nim |
| `failureRetryInterval` (50 milisekund) | tempo ponawiania prób, gdy awarii jest wiele: przy starcie i dla `RetryAllFailedSubscriptions` |
| `gapRequestRepeat` (10 sekund) | ile prośba o brakujące wersje czeka, zanim zostanie wysłana ponownie; ten czas podwaja się aż do 5 minut |
| `gapGiveUpAfter` (10 minut) | jak długo projekcja prosi o brakujące wersje, zanim luka zostanie zapisana jako awaria |

<!-- snippet: cart/FailuresSpec.scala#retries -->
```scala
import io.reactivecqrs.core.projection.ProjectionActorOptions
import io.reactivecqrs.core.util.Backoff

val options = ProjectionActorOptions.DEFAULT
  .withRetries(Backoff(1.second, 10.seconds, maxAttempts = 4)) // four attempts, about 7 seconds of waiting
  .copy(gapGiveUpAfter = 30.minutes)
```

## Tymczasem w szynie zdarzeń

Szyna zdarzeń prowadzi małą notatkę o każdym dostarczanym zdarzeniu, osobno dla każdego subskrybenta
i rodzaju listenera, dopóki subskrybent nie odpowie `MessageAck` (zastosowane, teraz albo wcześniej)
albo `MessageFailed` (poddał się). Wtedy:

- Gdy odpowiedzą wszyscy subskrybenci, szyna daje znać szynie poleceń koszyka, a ta usuwa wiersz
  zdarzenia z outboxu.
- Dostarczenie, na które przez minutę nie ma odpowiedzi, trafia raz do logu, na przykład
  `Delivery waits for 1 minute: ShoppingCart 42 version 3 to CartsList (AggregatesDelivery)`. Takich
  linii powstaje co najwyżej 10 co 5 sekund, a co 5 minut podsumowanie.
- Dostarczenie, na które przez 10 minut nie ma odpowiedzi, kończy się wpisem w logu:
  `Undelivered after 10 minutes: ShoppingCart 42 version 3 to CartsList (AggregatesDelivery); it stays in the outbox until it is published again`.
  Szyna o nim zapomina, a wiersz w outboxie zostaje.

(Szyna nazywa subskrybenta nazwą jego aktora, tą podaną w `system.actorOf`; własne linie logu
projekcji używają jej `projectionName`. U nas obie nazwy to „CartsList”.)

Zapytaj szynę, jak jej idzie, wysyłając `GetDeliveryStatus`:

<!-- snippet: cart/FailuresSpec.scala#delivery -->
```scala
import io.reactivecqrs.core.eventbus.EventsBusActor.{DeliveryStatus, GetDeliveryStatus}

val delivery = Await.result((eventBus ? GetDeliveryStatus).mapTo[DeliveryStatus], 5.seconds)
```

| Pole | Znaczenie |
|---|---|
| `waiting` | dostarczenia czekające na odpowiedź |
| `undelivered` | 100 ostatnich dostarczeń zakończonych po upływie terminu, każde jako `Undelivered(aggregateType, aggregateId, version, subscriber, kind, at)`; pozostałe zlicza log |
| `stopped` | subskrybenci, którzy się zatrzymali (dalej w tym rozdziale) |
| `held`, `behind` | kontrola przepływu (dalej w tym rozdziale) |

Szyna bierze swoje czasy z `DeliveryOptions`, które możesz przekazać przy jej tworzeniu, na przykład
`Props(new EventsBusActor(DeliveryOptions(deadline = 20.minutes)))`. Pola dotyczące tej sekcji to
`deadline` (10 minut), `stallAfter` (1 minuta), `summaryInterval` (5 minut), `tickInterval` (5 sekund)
i `stallLinesPerTick` (10).

## Outbox: lista zadań odporna na awarie

Każde zdarzenie dostaje wiersz w `events_to_publish`, czyli w **outboxie**, w tej samej transakcji,
która zapisuje zdarzenie. Wiersz znika dopiero wtedy, gdy odpowiedzą wszyscy subskrybenci. Kiedy więc
maszyna padnie tuż po zatwierdzeniu transakcji albo dostarczenie skończy się po upływie terminu,
wiersz wciąż tam jest i ktoś musi dostarczyć zdarzenie jeszcze raz: `OutboxPollerActor`, jeden na
system, uruchomiony w rozdziale 4.

Co 30 sekund (pierwszy raz 10 sekund po starcie) poller sprawdza, które agregaty wciąż mają czekające
wiersze, najwyżej 100 000 agregatów, zaczynając od tych, które czekają najdłużej, i decyduje:

| Najstarsze czekające zdarzenie zapisano | Poller |
|---|---|
| 3 minuty temu | prosi właściciela koszyka, żeby opublikował je ponownie |
| 6 minut temu | prosi jeszcze raz |
| 10 minut temu | jednorazowo zapisuje w logu `Undelivered: ShoppingCart 42 from version 3 still waits in the outbox after 10 minutes; it is re-published again every 30 minutes` |
| dawniej | prosi znowu co 30 minut, na wypadek gdyby subskrybent wrócił |

Wiersze zapisane przed startem procesu są publikowane przy pierwszym przebiegu pollera, a potem po 3 i
6 minutach. Subskrybenci, którzy już mają te zdarzenia, po prostu potwierdzają je jeszcze raz. W
jednym przebiegu ponownie publikuje się co najwyżej 1 000 agregatów, najpierw te, które czekają
najdłużej, żeby zaległości po awarii nie obudziły naraz wszystkich właścicieli.

<!-- snippet: cart/FailuresSpec.scala#outboxStatus -->
```scala
import io.reactivecqrs.core.delivery.OutboxPollerActor.{GetOutboxStatus, OutboxStatus}

val outbox = Await.result((outboxPoller ? GetOutboxStatus).mapTo[OutboxStatus], 5.seconds)
// outbox.waiting: aggregates with rows waiting at the last poll; outbox.gaveUp: those reported undelivered
```

Czasy możesz zmienić przez `OutboxPollerOptions` przy tworzeniu pollera:

<!-- snippet: cart/FailuresSpec.scala#pollerOptions -->
```scala
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}

val outboxPoller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
  OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000)))), "outboxPoller")
```

Wartości domyślne: `firstPollAfter` 10 sekund, `pollInterval` 30 sekund, `maxTracked` 100 000, a w
`RedeliveryPolicy.Options`: `attemptsAfter` 3 i 6 minut, `giveUpAfter` 10 minut,
`maxRepublishPerPoll` 1 000 i `retryGivenUpEvery` 30 minut. Gdy czeka więcej koszyków niż `maxTracked`,
poller czyta też 50 000 najnowszych, więc koszyki czekające na subskrybenta, którego długo nie ma, nie
zasłonią nowszego zgubionego dostarczenia. Koszyk, który poller zobaczy po raz pierwszy późno, dostaje
po swojej ponownej publikacji tyle samo czasu co każdy inny, zanim zostanie zgłoszony.
`OutboxStatus.gaveUp` wymienia 100 czekających najdłużej, a `gaveUpTotal` liczy wszystkie.

> **Pamiętaj:** listener, który zawodzi na dobre, *nie* zatrzymuje wierszy w outboxie: jego
> `MessageFailed` też jest odpowiedzią. Poller jest od ciszy (zgubionych wiadomości, maszyn, które
> padły, zatrzymanych subskrybentów); od awarii są wpisy o awariach i opisane wyżej komunikaty.

> **Pod maską:** „opublikuj ponownie” oznacza, że poller wysyła `Republish` do szyny zdarzeń, ta
> przekazuje `RepublishOutbox` szynie poleceń typu koszyka, a ta przekazuje go właścicielowi koszyka
> (w razie potrzeby go budząc). Właściciel publikuje od swojej najstarszej czekającej wersji do
> bieżącej, ze swoim bieżącym stanem; długi zakres idzie w częściach po 1 000 zdarzeń albo 16 MB.
> Zobacz [ARCHITECTURE.md, sekcja 5.1](../ARCHITECTURE.md).

## Wolna projekcja: kontrola przepływu

Wyobraź sobie kelnera, który obsługuje stolik dla ośmiu osób. Kuchnia nie wydaje wszystkich
czterdziestu dań naraz: kelner przynosi tyle, ile zmieści się na stole, a reszta czeka ciepła w
kuchni, aż zrobi się miejsce. Szyna zdarzeń robi to samo z projekcją, która nie nadąża:

- Projekcja dostaje co najwyżej **okno** zdarzeń, na które jeszcze nie odpowiedziała: 10 000 zdarzeń
  albo 64 MB ich zapisanego rozmiaru, w zależności od tego, co nastąpi pierwsze.
- To, co się nie mieści, jest **wstrzymywane**: zdarzenia zostają w bazie danych, ich wiersze w
  outboxie również, a szyna pamięta tylko, które koszyki zostały w tyle.
- Gdy projekcja odpowie na połowę swojego okna, szyna prosi właścicieli wstrzymanych koszyków, żeby
  opublikowali je ponownie dla tej projekcji, od najstarszych, po co najwyżej 100 koszyków naraz.
  Dopóki jakieś koszyki są wstrzymane, nowe zdarzenia zajmują najwyżej połowę okna, więc wstrzymane
  wciąż posuwają się naprzód.
- Szybka projekcja obok wolnej nie jest przez nią spowalniana: każdy subskrybent ma własne okno.
- Projekcja, która przez cały termin nie odpowiada na nic, nie dostaje nowego okna: to, co do niej
  wysłano, liczy się dalej, dopóki nie odpowie na coś wysłanego później. W tym czasie dostaje jeden
  komunikat na termin, więc zgubione komunikaty nie mogą jej odciąć na dobre.
- W klastrze listener stanów na innym węźle dostaje w każdym komunikacie kopię stanu koszyka, więc
  okno liczy też rozmiar stanu.

`GetDeliveryStatus` pokazuje to wszystko: `held` liczy wstrzymane dostarczenia, a `behind` przypisuje
każdemu subskrybentowi ze wstrzymanymi koszykami ich liczbę. Subskrybentów, którzy zostali w tyle,
wymienia też pięciominutowe podsumowanie szyny. Instancja zarządzanej projekcji nosi nazwę projekcji
(`carts-build-2-1`), więc te nazwy, metryka `bus.behind` i logi odróżniają projekcje od siebie.

Żeby dać projekcjom większe albo mniejsze okno, przekaż szynie `DeliveryOptions`, na przykład
`DeliveryOptions(window = 20000, windowBytes = 128L * 1024 * 1024)`. Większe okno pozwala projekcji
wziąć więcej naraz, kosztem pamięci zajmowanej przez jej skrzynkę pocztową. W klastrze szyna każdego
węzła ma własne okno (rozdział 17).

Jak szybko to wystarczająco szybko? Pomiary samej biblioteki dają pewne wyobrażenie, ale nie są
deklaracją przepustowości: to pojedyncze uruchomienia na jednej maszynie z Windows (12 wątków
procesora, PostgreSQL na tej samej maszynie), zmierzone 17/10/2026. Projekcja oparta na zdarzeniach
stosowała 2 732 zdarzenia na sekundę, podczas gdy wykonywały się polecenia
(`docs/perf/baseline-M4.md`), a po 60-sekundowej serii 671 768 poleceń dwie projekcje nadrobiły
zaległości 89 sekund po jej zakończeniu (`docs/perf/flow-M5.md`).

> **Wskazówka:** dbaj, żeby listenery były tanie, zostaw zapas między tempem poleceń a najwolniejszą
> projekcją i obserwuj `held` i `behind`. Projekcję, która została daleko w tyle, często szybciej jest
> przebudować (rozdział 10), niż dokarmiać. Szczegóły znajdziesz w [SCALING.md, sekcja 5](../SCALING.md).

## Projekcja, która się zatrzymuje

Projekcja nie zatrzymuje się dlatego, że zawiódł listener albo zapytanie: odpowiada informacją o
awarii i działa dalej. Ale aktor wciąż może się zatrzymać: ktoś go zatrzyma albo wyłącza się część
systemu. Szyna obserwuje swoich subskrybentów i gdy któryś się zatrzyma, zapisuje w logu błąd
`Subscriber …/user/CartsList stopped; its events stay in the outbox until the next start`, wpisuje go
do `DeliveryStatus.stopped` i nic mu już nie wysyła. Jego zdarzenia czekają w outboxie (poller wciąż
próbuje, a pozostali subskrybenci potwierdzają powtórki), aż system wystartuje ponownie razem z nim.
Pozostałych subskrybentów to nie dotyczy. Projekcja zarządzana (rozdział 10) jest po krótkiej chwili
automatycznie zastępowana.

Dwa rodzaje zatrzymania nie są błędem, więc szyna zapisuje je w logu na poziomie INFO: subskrybent
zatrzymuje się, bo wyłącza się jego węzeł, albo jego następca jest już w drodze, jak w przypadku
instancji projekcji zarządzanej (jej nadzorca uruchamia nową) lub projekcji, która w klastrze działa w
jednym egzemplarzu i przenosi się na inny węzeł (rozdział 17).

## Co wstrzymuje awaria subskrypcji

Wpis o awarii to obietnica: „ten koszyk wciąż jest coś winien tej projekcji”. Dwie operacje, które
złamałyby tę obietnicę, czekają, aż wpis zniknie:

- **Squash** (rozdział 14): na `SquashAggregate(cartId, upTo)` odpowiedzią jest
  `SquashRefused(cartId, "a subscription of the aggregate is failed")`.
- **Trwałe usunięcie** (rozdział 6): koszyk na razie nie zostaje usunięty. Szyna poleceń zapisuje w
  logu `Aggregate 43 of type ShoppingCart is not deleted yet: a subscription of it is failed`, a
  wiersz outboxu ze zdarzeniem usunięcia zostaje. Gdy awaria zniknie dzięki ponowieniu albo
  pominięciu, projekcja od razu prosi właściciela koszyka o ponowne opublikowanie tego wiersza i
  usunięcie dobiega końca. Wpisu o awarii usuniętego w inny sposób (`clearSubscriptionsInfo` niżej)
  nikt nie zauważa: usunięcie czeka wtedy na następną rundę pollera, najwyżej 30 minut.

> **Ostrzeżenie:** projekcja usunięta z kodu zostawia po sobie wpisy o awariach, przez które squashe
> wciąż są odrzucane, a trwałe usunięcia czekają. W przypadku projekcji zarządzanej posprząta
> `Projections.retire(name)` (rozdział 10). Dla projekcji uruchamianej samodzielnie wywołaj
> `subscriptionsState.clearSubscriptionsInfo("com.example.CartsListProjection")` z pełną nazwą jej
> klasy: to wywołanie zapomina o wersjach i awariach tej projekcji.

## Co już umiesz

- Dostarczanie działa **co najmniej raz, po kolei w obrębie agregatu i z jednokrotnym zastosowaniem**
  dzięki notatnikowi (`subscriptions`) zapisywanemu w transakcji twojego listenera. Zapisy przez sesję
  można bezpiecznie powtarzać; innych skutków ubocznych nie, a ich miejsce jest w sagach albo w
  osobnych, idempotentnych listenerach.
- Listener, który rzuca wyjątek, jest **ponawiany przez około dwie minuty**; potem awaria zostaje
  **zapisana**, ta projekcja odpowiada na dostarczenia tego koszyka awarią, a cała reszta płynie dalej.
- **`GetProjectionStatus`** pokazuje awarie, ponowienia i luki; **`RetryFailedSubscription`**,
  **`RetryAllFailedSubscriptions`** i **`SkipFailedVersion`** je naprawiają, a przy każdym starcie
  projekcja ponawia próby dla zapisanych awarii.
- O brakujące wersje projekcja prosi właściciela agregatu; po 10 minutach luka staje się awarią.
- **Outbox** przechowuje to, co nie zostało dostarczone; **poller outboxu** publikuje to ponownie po 3
  i 6 minutach, zgłasza po 10, a potem próbuje znowu co 30 minut. **`GetDeliveryStatus`** i
  **`GetOutboxStatus`** pokazują, jak sprawy stoją.
- **Kontrola przepływu** wysyła wolnej projekcji naraz co najwyżej 10 000 zdarzeń albo 64 MB; reszta
  czeka w bazie danych.
- Nieudana subskrypcja wstrzymuje **squash** i **trwałe usunięcie** swojego agregatu.

Dalej: [rozdział 10](10-rebuilding-projections.md) pokazuje, jak przebudować projekcję z magazynu
zdarzeń, podczas gdy stara wciąż obsługuje zapytania.
