# Rozdział 9. Kiedy w projekcjach coś idzie nie tak

Projekcje to kod, a kod ulega uszkodzeniu: błąd w programie nasłuchującym, baza danych, która zawodzi, projekcja
wolniejszy niż polecenia, które wykonuje, maszyna, która umiera milisekundę po zatwierdzeniu. W tym
w rozdziale zobaczysz, co ReactiveCQRS obiecuje w kwestii dostarczania zdarzeń i co robi samo, gdy
coś idzie nie tak, a garść wiadomości, których używasz, aby przyjrzeć się uszkodzeniom i je naprawić. Przez
na końcu będziesz mógł przeczytać linie logu, zapytać projekcję, co jest z nią nie tak i uzyskać odpowiedź
zablokowany wózek znów rusza.

---

## Obietnica wyrażona prostymi słowami

Wyobraź sobie list polecony. Listonosz dzwoni, aż ktoś podpisze, a jeśli nikt tego nie zrobi, wraca.
Kolejny dzień. Każdy list od nadawcy ma numer. A w domu jest notes: „od
bank, ostatni otwarty list: numer 7". Litera 7, która pojawia się dwa razy, jest rozpoznawana i umieszczana
na bok. List 9, który się pojawia, podczas gdy 8 nigdy nie nadszedł, sprawia, że ​​domownicy dzwonią do banku: „proszę wysłać
numer 8 ponownie".

Właśnie to obiecuje projekcja ReactiveCQRS:

1. **Przynajmniej raz.** Każde zdarzenie dociera do każdej projekcji, która słucha jego typu zbiorczego, nawet
   podczas awarii i ponownych uruchomień. Czasami więcej niż raz.
2. **W kolejności, w ujęciu zbiorczym.** Zdarzenia z koszyka 42 są stosowane w kolejności wersji. Nie ma
   obietnica dotycząca wszystkich koszyków: wersja 5 z koszyka 7 może zostać zastosowana przed wersją 2 z koszyka 42, nawet jeśli
   Wózek 42 został zatwierdzony jako pierwszy.
3. **Zastosowano raz.** Notatnik jest tabelą `subskrypcji`: dla każdej projekcji, rodzaj słuchacza
   i zbiorczo, ostatnia zastosowana wersja. Jest zapisywana w tej samej transakcji, co Twój odczyt.
   model. Dostawa na poziomie tej wersji lub niższym jest duplikatem: potwierdzonym, nie zastosowanym ponownie.
4. **Brak dziur.** Wersja wykraczająca poza oczekiwaną oznacza, że ​​niektóre z nich zaginęły. Projekcja
   prosi właściciela agregatu o ponowne ich opublikowanie i stosuje wszystko po kolei, gdy już zostaną opublikowane
   przyjechać.

> **Pamiętaj:** zamówienie jest tylko zbiorcze. Model odczytu, który łączy kilka koszyków (łącznie ponad
> wszystkie wózki, powiedzmy) nie mogą zakładać, że zdarzenia różnych wózków przybywają w kolejności, w jakiej zostały dostarczone
> zaangażowany.

## Dlaczego Twój słuchacz musi być bezpieczny, aby uruchomić go dwa razy

Zasada „Zastosowano raz” odnosi się do tego, co słuchacz zapisuje w trakcie sesji: te zapisy są zatwierdzane
notatnik lub cofnij się do niego. Wszystko inne, co robi Twój słuchacz, nie jest objęte i może
wydarzyć się więcej niż raz:

- aktualizacje do 100 koszyków współdzielą jedną transakcję; w przypadku awarii jednego koszyka wszystkie są wycofywane
  z powrotem i każdy biegnie dalej sam;
- słuchacz, który nie spełnia wymagań, jest poddawany próbie aż dziewięć razy (następna sekcja);
- obiekt nasłuchujący `noDBSession` uruchamia się przed zapisaniem jego wersji, więc jeśli zapis się nie powiedzie, widzi
  te same wydarzenia jeszcze raz;
- odbudowa (rozdział 10) odtwarza historię za pośrednictwem słuchaczy.

Dlatego trzymaj e-maile, połączenia HTTP i wiadomości do innych aktorów poza zasięgiem zwykłych słuchaczy. Ich miejsce jest w
saga (rozdział 12) lub w swoim własnym programie nasłuchującym, który używa `noDBSession` (bez transakcji, nigdy
zgrupowane z innymi wózkami), nie robi nic, podczas gdy `odbudowa` jest prawdą i jest idempotentne: wykonuje
ta sama rzecz dwa razy musi być nieszkodliwa.

```scala
import io.reactivecqrs.core.projection.{AggregateListenerActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart.{Koszyk, Koszyk usunięty}

cecha Mailer { def sendOnce(key: String, text: String): Unit } // twoja własna usługa: jeden e-mail na klucz

klasa CartDeletedMailer(val subscriptionsState: SubscriptionsState, mailer: Mailer) rozszerza AggregateListenerActor {
  nadpisz chronioną wartość projectionName: String = "CartDeletedMailer"

  chronionych słuchaczy wartości = Lista(
    EventsListener.noDBSession[ShoppingCart] { (cartId, events) =>
      jeśli (!rebuilding && events.exists(_.event == ShoppingCartDeleted()))
        mailer.sendOnce("cart-deleted-" + cartId.asLong, "Twój koszyk " + cartId.asLong + " został usunięty.")
    })
}
```

`AggregateListenerActor` to projekcja, której `receiveQuery` ignoruje każdą wiadomość: obiekt nasłuchujący
nie mając o co pytać. Zaczynasz i rejestrujesz to jak każdą projekcję (rozdział 8).

## Kiedy słuchacz rzuca

Zepsujmy coś celowo. Załóżmy, że zdecydowaliśmy, że nazwy koszyków muszą być unikalne na naszej liście.
i nadał sklepowi z rozdziału 8 unikalny indeks:

```scala
nowy PostgresDocumentStore[CartSummary]("carts_list", mpjsons, nowy NoopDocumentStoreCache,
  Seq(UniqueTextIndex(1, Seq("nazwa"))))
```

Alicja ma wózek o nazwie „Spożywcze”. Następnie Bob tworzy wózek o tej samej nazwie. Strona pisząca…
nie ma problemu, ponieważ nazwy koszyków nie są tam unikalne, ale nasz `insertDocument` słuchacza dla Boba
Wózek (wózek 43) przerywa indeks. Oto, co dzieje się dalej:

1. Wstawianie powoduje błąd. Transakcja zostaje wycofana: brak dokumentu, a notatnik nadal wyświetla komunikat „nic”.
   „zastosowano” dla koszyka 43. W dzienniku pojawia się ostrzeżenie:

   ```
   Niepowodzenie projekcji CartsList w ShoppingCart 43 w wersjach 1-1 (próba 1), ponawianie próby: org.postgresql.util.PSQLException: BŁĄD: zduplikowana wartość klucza narusza ograniczenie unikatowości ...
   ```

2. Projekcja próbuje ponownie po 1, 2, 4, 8, 16, 30, 30 i 30 sekundach: dziewięć prób w ciągu około
   Dwie minuty. Późniejsze wydarzenia z wózka 43 czekają za tym, który uległ awarii. Każdy kolejny wózek jedzie dalej.
   zwykły.
3. Jeśli próba się powiedzie (np. po awarii bazy danych), to koniec: brak rekordu, nic
   naprawić.
4. Po dziewiątej porażce projekcja rezygnuje z karty 43. Zapisuje wiersz do
   `subscription_failures` (wersja, której nie można zastosować, błąd i od kiedy) i rejestruje
   błąd:

   ```
   Projection CartsList zrezygnował z ShoppingCart 43 w wersji 1 (PSQLException: BŁĄD: zduplikowana wartość klucza...); dostawy kończą się niepowodzeniem do momentu ponowienia próby lub pominięcia
   ```

5. Od tej pory na każdą dostawę koszyka 43 odpowiada komunikatem „MessageFailed”, bez konieczności podejmowania kolejnych prób.
   Szyna zdarzeń uznaje to za odpowiedź (i za każdym razem rejestruje ostrzeżenie), więc wiersze skrzynki nadawczej wózka 43
   są usuwane jak zwykle. Wiersz w `subscription_failures` przypomina teraz o tym, co pozostało do zrobienia.

Jeszcze jeden przypadek: jeśli nawet awarii nie da się zapisać, bo sama baza danych jest nieczynna,
projekcja nie odpowiada na nic i próbuje ponownie wykonać partię co 30 sekund, aż będzie mogła ją zastosować
lub zarejestruj awarię.

> **Pamiętaj:** awaria dotyczy jednej projekcji, jednego rodzaju słuchacza i jednego agregatu. Koszyk 43 to
> utknąłem w `CartsList`; ten sam wózek w innych projekcjach i każdy inny wózek w tej są
> dobrze.

> **Wskazówka:** model odczytu to niewłaściwe miejsce na reguły biznesowe. O zmianie dowiaduje się dopiero po
> zmiana jest zapisywana, więc jedyne, co może się zdarzyć, to błąd. Reguły należą do strony zapisu, gdzie polecenie
> nadal może zostać odrzucona.

## Przyglądamy się uszkodzeniom: GetProjectionStatus

Zapytaj samą projekcję:

```scala
import io.reactivecqrs.core.projection.{GetProjectionStatus, ProjectionStatus}

status val = Await.result((cartsList ? GetProjectionStatus).mapTo[ProjectionStatus], 5.sekund)
status.failures.foreach { f =>
  println(f.aggregateType.simpleName + " " + f.aggregateId.asLong + " utknąłem na wersji " + f.version.asInt + ": " + f.error)
}
// ShoppingCart 43 utknął w wersji 1: PSQLException: BŁĄD: zduplikowana wartość klucza narusza ograniczenie unikatowości ...
```

Odpowiedź brzmi `ProjectionStatus(projectionName, failures, retrying, waitingForGap)`:

- **`failures`**: jeden `SubscriptionFailure` na zablokowaną subskrypcję, z jej `subscriptionType`
  (jaki rodzaj słuchacza), `aggregateType`, `aggregateId`, `version` (pierwsza wersja nie została zastosowana),
  `błąd` i `od`;
- **`retrying`**: karty, których program nasłuchujący uległ awarii i jest właśnie ponawiany (krok 2 powyżej);
- **`waitingForGap`**: wózki czekające na brakujące wersje (dalej w tym rozdziale).

Trzy puste listy oznaczają, że wszystko jest w porządku. Awarie znajdują się w tabeli `subscription_failures`, więc
przetrwanie ponownych uruchomień: zablokowany wózek pozostaje zablokowany, dopóki ktoś nie podejmie z tym działań.

> **Wskazówka:** umieść `GetProjectionStatus` za stroną administracyjną lub kontrolą stanu dla każdej projekcji.
> Rozdział 18 zawiera więcej informacji na temat monitorowania działającego systemu.

## Naprawianie: ponów próbę lub pomiń

Najpierw napraw przyczynę. Tutaj zamieniamy indeks unikalny na zwykły, pod nowym numerem (pod
start, sklep usuwa indeks 1 i tworzy indeks 2), a następnie restart:

```scala
Seq(MultipleTextIndex(2, Seq("nazwa")))
```

Być może nie będzie trzeba robić nic więcej: zarejestrowane wcześniej błędy zostaną automatycznie powtórzone, gdy
Projekcja rozpoczyna się natychmiast po jej zarejestrowaniu przez magistralę, 20 na sekundę. Po tym restarcie, karta
43 otrzymuje swój dokument bez kiwnięcia palcem. Kiedy naprawisz przyczynę bez restartu lub
ponowna próba uruchomienia nie pomogła, te trzy wiadomości, wysłane do projekcji, robią to ręcznie:

| Wiadomość | Odpowiedź | Co robi |
|---|---|---|
| `RetryFailedSubscription(cartId)` | `FailedSubscriptionsHandled(cartId, count)` | pyta właściciela koszyka o nieudaną wersję i kolejne, aż do ich znalezienia; po zastosowaniu nieudanej wersji rekord błędu jest usuwany |
| `RetryAllFailedSubscriptions` | `FailedSubscriptionsRetried(count)` | to samo dla każdego niepowodzenia projekcji, 20 na sekundę |
| `SkipFailedVersion(cartId)` | `FailedSubscriptionsHandled(cartId, count)` | oznacza nieudaną wersję jako zastosowaną bez jej stosowania, usuwa rekord i pyta raz o wersje po nim |

`count` informuje, ile nieudanych subskrypcji zostało obsłużonych; 0 oznacza, że ​​nic nie trzeba było zrobić.

```scala
import io.reactivecqrs.core.projection.{Obsłużone nieudane subskrypcje, Ponów nieudaną subskrypcję}

Await.result((cartsList ? RetryFailedSubscription(cartId)).mapTo[FailedSubscriptionsHandled], 5.sekund)
// Obsługiwane nieudane subskrypcje (Id(43),1)
```

Gdy to zadziała, w dzienniku pojawi się informacja: „Projection CartsList odzyskał sprawność po awarii w ShoppingCart 43”.
Jeśli nasłuchiwanie nadal nie powiedzie się, wrócisz do kroku 1: ponownych prób, a następnie nowego rekordu.

> **Ostrzeżenie:** pomijanie powoduje utratę czegoś, a to, co tracimy, zależy od słuchacza. Stan
> słuchacz otrzymuje cały stan wraz z następną wersją, więc jego dokument jest ponownie poprawny
> w miarę zmian w koszyku (jeśli w ogóle do nich dojdzie). Słuchacz zdarzeń nigdy więcej nie widzi pominiętego zdarzenia: jego
> efekt zniknął na dobre. Istnieje pułapka dla słuchaczy stanu, którzy polegają na `created`: jeśli
> pominięta wersja to ta, która utworzyła dokument, następna wersja próbuje
> `overwriteDocument` dokument, który nie istnieje i kończy się niepowodzeniem. Nasłuchiwacz napisany z
> `updateDocument(0, cartId.asLong, _ => Document(summary))`, który wstawia, gdy nic nie ma
> jeszcze do tego nie pasuje.

## Brakujące wersje

Czasami przesyłka nigdy nie dociera: wiadomość została utracona, gdy węzeł przestał działać lub projekcja, która
dołączyły po pierwszych wydarzeniach w wózku (rozdział 8). Projekcja informuje, kiedy nastąpi kolejna dostawa
Pojawia się: „Jestem w wersji 3, a to jest wersja 5”. Następnie:

1. Prosi właściciela wózka, za pośrednictwem magistrali, o ponowne opublikowanie wszystkiego od wersji 4 do tej
   Tylko projekcja. W dzienniku jest napisane: „Projekcja CartsList prosi o wersje z 4 z ShoppingCart 42”.
   Wersja 5 czeka.
2. Po otrzymaniu wersji stosuje kolejno numery 4 i 5 i potwierdza oczekiwaną dostawę.
3. Jeśli nie ma odpowiedzi, pyta ponownie po 10 sekundach, potem po 20, 40 i tak dalej, aż do jednego razu
   5 minut.
4. Jeżeli po 10 minutach wersje nadal nie nadchodzą, rejestruje błąd w wersji 4 („wersje
   z 4 brakuje: zapytano … razy w ciągu 10 minut bez odpowiedzi"), z którymi radzisz sobie jak z każdym innym
   inna awaria.

`ProjectionStatus.waitingForGap` wyświetla koszyki w krokach od 1 do 3.

## Opcje awarii

Opcje z rozdziału 8 `ProjectionActorOptions` kształtują wszystko powyżej:

| Opcja (domyślna) | Co kontroluje |
|---|---|
| `retries` (`Backoff(1.second, 30.seconds, maxAttempts = 9)`) | w jaki sposób ponawiany jest nieudany nasłuchiwacz przed zarejestrowaniem błędu: oczekiwanie rozpoczyna się od pierwszego czasu trwania i podwaja się do drugiego, dla takiej samej liczby prób |
| `retryFailuresOnStart` (prawda) | ponów próby wykonania błędów zarejestrowanych przed rozpoczęciem |
| `failureRetryInterval` (50 milisekund) | tempo ponawiania prób w przypadku wielu błędów: na początku i dla `RetryAllFailedSubscriptions` |
| `gapRequestRepeat` (10 sekund) | jak długo żądanie brakujących wersji czeka przed ponownym wysłaniem, czas ten ulega podwojeniu do 5 minut |
| `gapGiveUpAfter` (10 minut) | jak długo brakujące wersje są żądane, zanim luka stanie się odnotowaną awarią |

```scala
import io.reactivecqrs.core.projection.ProjectionActorOptions
import io.reactivecqrs.core.util.Backoff

OpcjeAktoraProjekcji.DOMYŚLNE
  .withRetries(Backoff(1.second, 10.seconds, maxAttempts = 4)) // cztery próby, około 7 sekund oczekiwania
  .copy(gapGiveUpAfter = 30 minut)
```

## Tymczasem w autobusie eventowym

Szyna zdarzeń przechowuje mały rejestr dla każdego dostarczanego zdarzenia, dla każdego rodzaju abonenta i słuchacza,
dopóki subskrybent nie odpowie komunikatem `MessageAck` (zastosowano teraz lub wcześniej) lub `MessageFailed` (podano
(w górę). Następnie:

- Gdy każdy abonent odpowie, magistrala przekazuje polecenie magistrali wózka, która usuwa
  wiersz zdarzenia ze skrzynki nadawczej.
- Przesyłka bez odpowiedzi przez minutę jest rejestrowana jednokrotnie, np.
  `Dostawa czeka 1 minutę: ShoppingCart 42 wersja 3 do CartsList (AggregatesDelivery)`.
  Większość z 10 takich linijek jest pisana co 5 sekund, a podsumowanie co 5 minut.
- Przesyłka bez odpowiedzi przez 10 minut kończy się:
  `Niedostarczone po 10 minutach: ShoppingCart 42 wersja 3 do CartsList (AggregatesDelivery); pozostaje w skrzynce nadawczej do momentu ponownej publikacji`.
  Autobus o tym zapomina, a rząd skrzynek nadawczych pozostaje.

(Magistrala nadaje nazwę abonentowi według nazwy jego aktora, tej, którą podałeś `system.actorOf`; projekcja
Własne wiersze logu używają swojej `projectionName`. Oba nazwaliśmy „CartsList”.)

Zapytaj autobus, jak sobie radzi z funkcją `GetDeliveryStatus`:

```scala
import io.reactivecqrs.core.eventbus.EventsBusActor.{DeliveryStatus, GetDeliveryStatus}

val delivery = Await.result((eventBus ? GetDeliveryStatus).mapTo[DeliveryStatus], 5.seconds)
```

| Pole | Znaczenie |
|---|---|
| `oczekiwanie` | przesyłki oczekujące na odpowiedź |
| `niedostarczone` | najnowsze, które zakończyły się przed upływem terminu (przechowywanych jest maksymalnie 1000), każdy o nazwie `Niedostarczone(aggregateType, aggregateId, version, subscriber, kind, at)` |
| `zatrzymany` | subskrybenci, którzy przestali (dalej w tym rozdziale) |
| `trzymane`, `za` | kontrola przepływu (dalej w tym rozdziale) |

Autobus pobiera swoje czasy z `DeliveryOptions`, które możesz przekazać podczas jego tworzenia, np.
przykład `Props(new EventsBusActor(DeliveryOptions(deadline = 20.minute)))`. Pola dla tego
sekcje to `deadline` (10 minut), `stallAfter` (1 minuta), `summaryInterval` (5 minut),
`tickInterval` (5 sekund) i `stallLinesPerTick` (10).

## Skrzynka nadawcza: lista zadań, która przetrwa awarię

Każde zdarzenie otrzymuje wiersz w `events_to_publish`, **outbox**, w tej samej transakcji, w której jest przechowywane
Zdarzenie. Wiersz jest usuwany dopiero po tym, jak wszyscy abonenci odpowiedzieli. Tak więc, gdy maszyna umiera,
po zatwierdzeniu lub zakończeniu dostawy w terminie spór nadal trwa i ktoś musi
dostarcz ponownie zdarzenie: `OutboxPollerActor`, jeden na system, którego działanie rozpocząłeś w rozdziale 4.

Co 30 sekund (pierwszy raz 10 sekund po uruchomieniu) sonda odczytuje, które agregaty są jeszcze
ma rzędy oczekujących, maksymalnie 100 000 z nich, najdłużej czekający pierwszy, i decyduje:

| Najstarsze oczekujące zdarzenie zostało zapisane | Ankieta |
|---|---|
| 3 minuty temu | prosi właściciela koszyka o ponowne opublikowanie |
| 6 minut temu | pyta ponownie |
| 10 minut temu | logi `Niedostarczone: ShoppingCart 42 od wersji 3 nadal czeka w skrzynce nadawczej po 10 minutach; jest ponownie publikowany co 30 minut`, raz |
| już dawno temu | pyta ponownie co 30 minut, na wypadek gdyby subskrybent wrócił |

Wiersze napisane przed rozpoczęciem procesu są publikowane w pierwszym głosowaniu, następnie po 3 i 6 minutach
później. Subskrybenci, którzy już mają dostęp do wydarzeń, po prostu potwierdzają je ponownie. Maksymalnie 1000
agregaty są publikowane ponownie według sondażu, przy czym najdłużej oczekujące dane są najpierw publikowane, tak aby po ankiecie nie było zaległości.
przerwa w dostawie prądu nie budzi wszystkich właścicieli od razu.

```scala
import io.reactivecqrs.core.delivery.OutboxPollerActor.{GetOutboxStatus, OutboxStatus}

val outbox = Await.result((outboxPoller ? GetOutboxStatus).mapTo[OutboxStatus], 5.sekund)
// outbox.waiting: agreguje z wierszami oczekującymi na ostatnie sprawdzenie; outbox.gaveUp: te zgłoszone jako niedostarczone
```

Możesz zmienić czasy za pomocą `OutboxPollerOptions` podczas tworzenia sondy:

```scala
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}

val outboxPoller = system.actorOf(Props(nowy OutboxPollerActor(eventStore, eventBus,
  OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000)))), "outboxPoller")
```

Domyślne wartości: `firstPollAfter` 10 sekund, `pollInterval` 30 sekund, `maxTracked` 100 000 i w
`RedeliveryPolicy.Options`: `attemptsAfter` po 3 i 6 minutach, `giveUpAfter` po 10 minutach,
`maxRepublishPerPoll` 1 000 i `retryGivenUpEvery` 30 minut.

> **Pamiętaj:** obiekt nasłuchujący, który nie działa, *nie* powoduje, że wiersze skrzynki odbiorczej czekają: jego
> `MessageFailed` to odpowiedź. Moduł sondujący jest tam, gdzie panuje cisza (utracone wiadomości, awarie komputerów,
> zatrzymanych abonentów); błędy występują w rekordach błędów i komunikatach powyżej.

> **Pod maską:** „opublikuj ponownie” oznacza, że ​​sonda wysyła polecenie „Republish” do magistrali zdarzeń, co
> przekazuje `RepublishOutbox` do magistrali poleceń typu koszyka, która przekazuje ją właścicielowi koszyka
> (budząc go, jeśli to konieczne). Właściciel publikuje od najstarszej oczekującej wersji do bieżącej,
> z jego obecnym stanem; długi zakres obejmuje części 1000 zdarzeń lub 16 MB. Zobacz
> [ARCHITECTURE.md, sekcja 5.1](../ARCHITECTURE.md).

## Powolna projekcja: kontrola przepływu

Wyobraź sobie kelnera obsługującego stolik dla ośmiu osób. Kuchnia nie wysyła wszystkich czterdziestu dań na raz:
Kelner przynosi tyle, ile mieści się na stole, a reszta pozostaje ciepła w kuchni, dopóki nie zwolni się miejsce.
Szyna zdarzeń wykonuje to samo w przypadku projekcji, która nie nadąża:

- Projekcja wysyła maksymalnie **okno** zdarzeń, na które jeszcze nie odpowiedziała: 10 000 zdarzeń lub
  64 MB rozmiaru przechowywanych danych, w zależności od tego, co nastąpi pierwsze.
- To, co się nie zmieści, jest **zachowane**: zdarzenia pozostają w bazie danych, ich wiersze w skrzynce nadawczej również, a magistrala
  pamięta tylko, które wózki są z tyłu.
- Gdy projekcja odpowiedziała na połowę swojego okna, autobus prosi właścicieli trzymanych wózków, aby
  Opublikuj je ponownie w tej samej projekcji, najpierw najstarsze, maksymalnie 100 koszyków na raz. Podczas gdy koszyki są
  odbywają się nowe wydarzenia, które zajmują co najwyżej połowę okna, więc te odbywające się w czasie są kontynuowane.
- Szybka projekcja obok wolnej nie jest pomijana: każdy abonent ma własne okno.

`GetDeliveryStatus` pokazuje wszystko: `held` zlicza wstrzymane dostawy, a `behind` mapuje każdą z nich
abonent z wózkami, ile ich jest. Pięciominutowe podsumowanie autobusu zawiera również nazwiska abonentów.

Aby nadać projekcjom większe lub mniejsze okno, przekaż na przykład `DeliveryOptions` do magistrali
`DeliveryOptions(window = 20000, windowBytes = 128L * 1024 * 1024)`. Większe okno pozwala
projekcja pobiera więcej danych naraz, kosztem pamięci w swojej skrzynce pocztowej. W klastrze magistrala każdego węzła
ma własne okno (rozdział 17).

Jak szybko to wystarczająco szybko? Pomiary biblioteki dają pewne pojęcie, a nie liczbę określającą pojemność: pojedyncze
działa na jednej maszynie z systemem Windows (12 wątków procesora, PostgreSQL na tej samej maszynie), pomiary z dnia 17.10.2026 r.
Projekcja oparta na zdarzeniach zastosowała 2732 zdarzenia na sekundę podczas wykonywania poleceń
(`docs/perf/baseline-M4.md`) i po 60-sekundowej serii 671 768 poleceń, dwie projekcje miały
nadrobiono 89 sekund po zakończeniu serii (`docs/perf/flow-M5.md`).

> **Wskazówka:** zadbaj o to, aby słuchacze byli tani, zostaw margines między szybkością polecenia a najwolniejszą
> projekcja i obserwowanie „trzymane” i „za”. Projekcja, która znacznie odstała, często
> szybsza odbudowa (rozdział 10) niż karmienie. [SCALING.md, sekcja 5](../SCALING.md) zawiera
> szczegóły.

## Projekcja, która się zatrzymuje

Projekcja nie zatrzymuje się z powodu niepowodzenia programu nasłuchującego lub zapytania: odpowiada informacją o niepowodzeniu i
Trwa. Ale aktor nadal może się zatrzymać: ktoś to zatrzyma albo część systemu się wyłączy. Autobus
pilnuje swoich abonentów, a gdy któryś się zatrzyma, autobus rejestruje
`Subskrybent …/user/CartsList został zatrzymany; jego zdarzenia pozostają w skrzynce nadawczej do następnego uruchomienia`, wyświetla je
w `DeliveryStatus.stopped` i nie wysyła niczego więcej. Jego zdarzenia czekają w skrzynce nadawczej (pomijarka
próbuje dalej, a pozostali subskrybenci potwierdzają powtórzenia) aż do ponownego uruchomienia systemu
To. Pozostali subskrybenci nie są dotknięci. Zarządzana projekcja (rozdział 10) zostaje zastąpiona
automatycznie po krótkim oczekiwaniu.

## Co czeka na nieudaną subskrypcję

Rekord awarii to obietnica: „ten wózek wciąż coś zawdzięcza tej projekcji”. Dwie operacje
to byłoby złamaniem tej obietnicy, poczekaj, aż płyta zniknie:

- **Squash** (rozdział 14): Na `SquashAggregate(cartId, upTo)` odpowiada się
  `SquashRefused(cartId, "subskrypcja agregatu nie powiodła się")`.
- **Trwałe usunięcie** (rozdział 6): wózek nie został jeszcze usunięty. Rejestry magistrali poleceń
  `Agregat 43 typu ShoppingCart nie został jeszcze usunięty: jego subskrypcja nie powiodła się`, a
  Wiersz skrzynki nadawczej zdarzenia usuwania pozostaje. Po ponownym próbie usunięcia lub pominięciu błędu, następna publikacja
  ten wiersz w harmonogramie sondy powyżej kończy usuwanie.

> **Ostrzeżenie:** projekcja, którą usuniesz z kodu, pozostawia po sobie rekordy błędów, a one
> odrzucaj spychanie i trwałe usuwanie w oczekiwaniu. W przypadku zarządzanej projekcji,
> `Projections.retire(name)` czyści (rozdział 10). W przypadku projektu, który sam uruchomiłeś, wywołaj
> `subscriptionsState.clearSubscriptionsInfo("com.example.CartsListProjection")` z pełną klasą
> nazwa: zapomina o wersjach i błędach projekcji.

## Czego się nauczyłeś

- Dostawa jest **przynajmniej jednorazowa, w kolejności zbiorczej, stosowana jednorazowo**, dzięki notatnikowi
  (`subskrypcje`) zapisane w transakcji słuchacza. Zapisy w trakcie sesji są bezpieczne.
  powtórz; inne skutki uboczne nie występują i należą do sag lub idempotentnych słuchaczy.
- Słuchacz, który zgłasza błąd, jest **ponownie sprawdzany przez około dwie minuty**, po czym błąd jest **rejestrowany**,
  odpowiedź na to pytanie brzmi: czy ten wózek nie powiódł się w tej projekcji, a wszystko inne płynie dalej.
- **`GetProjectionStatus`** pokazuje błędy, ponowne próby i przerwy; **`RetryFailedSubscription`**,
  **`RetryAllFailedSubscriptions`** i **`SkipFailedVersion`** naprawiają je, a awarie są
  ponawiane przy każdym starcie.
- Brakujące wersje są wymagane od właściciela agregatu; po 10 minutach luka staje się awarią.
- **Skrzynka nadawcza** przechowuje to, co nie zostało dostarczone; **aplet skrzynki nadawczej** publikuje to ponownie w punktach 3 i 6
  minut, raportuje o 10, a następnie próbuje ponownie co 30 minut. **`GetDeliveryStatus`** i
  **`GetOutboxStatus`** pokazuje aktualną sytuację.
- **Kontrola przepływu** wysyła powolną projekcję maksymalnie 10 000 zdarzeń lub 64 MB na raz; reszta czeka
  w bazie danych.
- Nieudana subskrypcja wstrzymuje **squash** i **trwałe usunięcie** jej agregatu.

Dalej: [rozdział 10](10-rebuilding-projections.md) pokazuje, jak odbudować projekcję ze zdarzenia
Przechowywać, dopóki stary nadal służy.