# Rozdział 11. Aktualizacje na żywo z projekcjami subskrybowalnymi

Pod koniec tego rozdziału strona koszyka będzie aktualizować się automatycznie za każdym razem, gdy ktoś zmieni zawartość koszyka:
Bob otwiera wózek „Zakupy” rodziny, Alicja dodaje mleko do swojego telefonu, a mleko pojawia się na
Ekran Boba bez kiwnięcia palcem. Bez przycisku odświeżania, bez proszenia co kilka sekund. Będziesz…
użyj `SubscribableProjectionActor`: projekcji z rozdziału 8, która przechowuje również listę
subskrybentów i przesyła im najnowsze informacje.

---

## Od proszenia do otrzymania polecenia

Zamówiłeś tort urodzinowy. Możesz dzwonić do cukierni co dziesięć minut, żeby zapytać, czy jest gotowy.
albo możesz zostawić swój numer i pozwolić im do Ciebie napisać. Drugi sposób oznacza mniej pracy dla wszystkich, ponieważ
dopóki piekarnia pamięta twój numer i dopóki od czasu do czasu będziesz im mówić, że nadal
opieka (piekarnia, która wysyła SMS-y na każdy numer, jaki kiedykolwiek jej podano, wkrótce zacznie wysyłać SMS-y do nieznajomych).

To jest **subskrypcja push**:

- **subskrybujesz**: „opowiedz mi o koszyku 42”;
- projekcja **wypycha** aktualizację za każdym razem, gdy zmienia się wózek 42;
- **odnawiasz** od czasu do czasu: „nadal zainteresowany”;
- **anulujesz**, gdy strona się zamknie.

`SubscribableProjectionActor` to zwykła projekcja (nasłuchiwacze, magazyn dokumentów, zapytania,
wszystko z rozdziału 8) co obejmuje wszystkie cztery.

```
projekcja LiveCarts dla sesji strony
     │── WatchCart("7f3c...", 42) ──────────────────────▶│ handleSubscribe
     │◀────────────── SubscribedForProjectionUpdates("7f3c...")
     │ │◀── zmieniono kartę 42 (z magistrali zdarzeń):
     │ │ słuchacz zapisuje, a następnie wysyła funkcję sendUpdate(...)
     │◀─────────────── SubscriptionUpdated("7f3c...", CartPageUpdate(...))
     │── RenewSubscription("7f3c...") ───────────────────▶│ co kilka minut
     │── CancelProjectionSubscriptions(List("7f3c...")) ▶│ gdy strona się zamknie
```

## Jak aktualizacja dociera do swoich subskrybentów

Projekcja subskrybowana dodaje do projekcji trzy rzeczy:

- **`receiveSubscriptionRequest`**, `Receive`, który piszesz i który obsługuje Twoją własną subskrypcję
  wiadomości, podobnie jak `receiveQuery` obsługuje zapytania;
- **`handleSubscribe[DATA, UPDATE](subscriptionId, listener, filter)`**, którą tam wywołujesz
  dodaj subskrypcję;
- **`sendUpdate[DATA](data)`**, którą wywołują Twoi słuchacze, aby ogłosić zmianę.

„DANE” to to, co ogłasza projekcja; „AKTUALIZACJA” to to, co otrzymuje subskrybent. Każda
`sendUpdate[DATA](data)` jest oferowane dla każdej subskrypcji utworzonej przy użyciu tego samego typu `DATA` i każdej
**filtr** subskrypcji, funkcja `DATA => Option[UPDATE]`, decyduje: `None` oznacza „nie dla
„me”, „Some(update)” oznacza „wyślij to”. Subskrybent otrzymuje wtedy
`SubscriptionUpdated(subscriptionId, update)`.

Wiadomości wysyłane i odbierane przez subskrybenta na żywo w elemencie towarzyszącym `SubscribableProjectionActor`:
`SubscribedForProjectionUpdates`, `SubscriptionUpdated`, `OdnówSubskrypcję`,
`CancelProjectionSubscriptions` i `ProjectionSubscriptionsCancelled`.

## Tworzenie strony koszyka na żywo, krok po kroku

Ponownie wykorzystujemy `CartSummary` z rozdziału 8 (nazwa, liczba pozycji i wersja koszyka) i wszystko
rozdział 4 podłączony.

1. **Zapisz aktualizację i komunikaty.**

   ```scala
   importuj io.reactivecqrs.api.id.AggregateId

   klasa przypadku CartPageUpdate(cartId: AggregateId, summary: Option[CartSummary]) // Brak: nie ma już takiego koszyka

   klasa przypadku WatchCart(subscriptionId: String, cartId: AggregateId) // subskrybuj jeden koszyk
   klasa przypadku GetCartPage(cartId: AggregateId) // odpowiedź udzielona za pomocą CartPageUpdate
   ```

   Zapytanie odpowiada tym samym typem co polecenie push, więc strona może traktować oba typy tak samo.

2. **Napisz projekcję.**

   ```scala
   import io.reactivecqrs.core.documentstore.{Dokument, Magazyn dokumentów}
   import io.reactivecqrs.core.projection.{SubscribableProjectionActor, SubscriptionsState}
   import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

   klasa LiveCartsProjection(val subscriptionsState: SubscriptionsState,
                             sklep: DocumentStore[CartSummary]) rozszerza SubscribableProjectionActor {

     nadpisz chronioną wartość projectionName: String = "LiveCarts"

     chronionych słuchaczy wartości = Lista(
       AggregateListener[ShoppingCart] { (cartId, wersja, utworzony, koszyk) => niejawna sesja =>
         podsumowanie wartości = cart.map(c => CartSummary(c.name, c.items.size, version.asInt))
         podsumowanie dopasowania {
           przypadek Some(s) => store.updateDocument(0, cartId.asLong, _ => Document(s))
           przypadek None => store.removeDocument(cartId.asLong)
         }
         jeśli (!rebuilding) sendUpdate[CartPageUpdate](CartPageUpdate(cartId, summary))
       })

     nadpisz chronione def receiveSubscriptionRequest: Receive = {
       przypadek WatchCart(subscriptionId, cartId) =>
         handleSubscribe[CartPageUpdate, CartPageUpdate](subscriptionId, sender(),
           aktualizacja => jeśli (aktualizacja.cartId == cartId) Some(aktualizacja) else None)
     }

     nadpisz chronione def receiveQuery: Receive = {
       przypadek GetCartPage(cartId) =>
         sender() ! CartPageUpdate(cartId, store.getDocument(cartId.asLong).map(_.document))
     }
   }
   ```

   Linijka po linijce:

    - Słuchacz utrzymuje dokument na bieżąco, jak w rozdziale 8 (`updateDocument` wstawia
      dokumentu, gdy jeszcze go nie ma), a następnie ogłasza zmianę za pomocą `sendUpdate`.
    - `if (!rebuilding)`: podczas gdy projekcja jest odbudowywana z historii (rozdział 10), jej słuchacze
      odtwarzać stare wydarzenia. Nikt nie chce, żeby mleko z zeszłego roku było wyświetlane na jego ekranie.
    - `handleSubscribe[CartPageUpdate, CartPageUpdate]` nadaje nazwy obu typom: co ogłasza `sendUpdate`
      i co otrzymuje abonent. Tutaj są one takie same.
    - Filtr zachowuje aktualizacje jednego koszyka, który obserwuje dany subskrybent.
    - Nasłuchiwaczem jest `sender()`: ktokolwiek wysłał `WatchCart`, otrzymuje aktualizacje. Projekcja potwierdza
      z `SubscribedForProjectionUpdates(subscriptionId)`, wysłanym do tego odbiornika.

3. **Uruchom go i zarejestruj**, dokładnie tak jak w rozdziale 8, z własną tabelą:

   ```scala
   val liveCarts = system.actorOf(
     Rekwizyty (nowe LiveCartsProjection (stan subskrypcji,
       nowy PostgresDocumentStore[CartSummary]("live_carts", mpjsons, nowy NoopDocumentStoreCache)))
       .withDispatcher(BlockingDispatcher.id),
     „LiveCarts”)

   eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList, liveCarts))
   ```

   Trafia na tę samą listę, co każdy inny abonent; autobus korzysta z tej listy tylko raz, na starcie.

4. **Napisz stronę strony.** Każda otwarta strona ma swojego własnego małego aktora, sesję, która
   subskrybuje po rozpoczęciu, przekazuje wiadomości do przeglądarki, odnawia subskrypcję i
   anuluje, gdy się zatrzyma:

   ```scala
   importuj java.util.UUID
   import org.apache.pekko.actor.{Actor, ActorRef, Timery}
   import io.reactivecqrs.core.projection.SubscribableProjectionActor._
   import scala.concurrent.duration._

   obiekt CartPageSession {
     prywatny obiekt sprawy Odnów
   }

   /** Jedna otwarta strona koszyka; `push` wysyła zmianę do przeglądarki (ta część to Twoja warstwa internetowa). */
   klasa CartPageSession(liveCarts: ActorRef, cartId: AggregateId, push: CartPageUpdate => Unit)
     rozszerza aktora o timery {
     importuj CartPageSession.Renew

     prywatna zmienna subscriptionId = ""
     prywatna zmienna pokazanoVersion = 0

     nadpisz def preStart(): Jednostka = subscribe()

     prywatna def subscribe(): Jednostka = {
       subscriptionId = UUID.randomUUID().toString
       liveCarts ! WatchCart(subscriptionId, cartId) // pierwsza subskrypcja,
       liveCarts ! GetCartPage(cartId) // następnie załaduj to, co jest teraz
     }

     nadpisz def receive: Receive = {
       przypadek SubscribedForProjectionUpdates(id), jeśli id ​​== subscriptionId =>
         timers.startTimerWithFixedDelay(Odnów, Odnów, 3.minuty)
       przypadek SubscriptionUpdated(id, aktualizacja: CartPageUpdate) jeśli id ​​== subscriptionId =>
         pokaż(aktualizuj)
       aktualizacja przypadku: CartPageUpdate => // odpowiedź na GetCartPage
         pokaż(aktualizuj)
       sprawa Odnów =>
         liveCarts! Odnów subskrypcję (subscriptionId)
       przypadek ProjectionSubscriptionsCancelled(ids), jeśli ids.contains(subscriptionId) =>
         subscribe() // projekcja o nas zapomniała: zasubskrybuj ponownie i załaduj ponownie
     }

     // Przesyłanie i odpowiedzi mogą się wzajemnie nakładać: wyświetlaj tylko te elementy, które są nowsze od strony.
     prywatna def show(aktualizacja: CartPageUpdate): Jednostka = aktualizacja.podsumowanie dopasowanie {
       przypadek Some(podsumowanie) jeśli summary.version <= shownVersion => ()
       przypadek Some(podsumowanie) =>
         pokazanoVersion = podsumowanie.wersja
         push(aktualizacja)
       przypadek None => push(aktualizacja)
     }

     nadpisz def postStop(): Unit = liveCarts ! CancelProjectionSubscriptions(List(subscriptionId))
   }
   ```

   Gdy strona się otworzy, rozpocznij sesję; gdy się zamknie, zatrzymaj ją:

   ```scala
   val session = system.actorOf(Props(new CartPageSession(liveCarts, cartId, update => browser.send(update))))
   // ... i kiedy strona się zamknie:
   system.stop(sesja)
   ```

   (`browser.send` oznacza warstwę internetową: WebSocket, zdarzenia wysyłane przez serwer, cokolwiek używasz.)

> **Wskazówka:** najpierw zasubskrybuj, a następnie załaduj i zachowaj najwyższą wersję. Zasubskrybowanie najpierw oznacza brak zmian.
> może prześlizgnąć się między obciążeniem a subskrypcją; wersja informuje stronę, która z dwóch
> Wiadomości są nowsze, gdy push i odpowiedź na `GetCartPage` się krzyżują. Dlatego `CartSummary`
> niesie wersję wózka.

> **Wskazówka:** nadaj każdej subskrypcji nowy identyfikator, taki jak UUID, tak jak robi to sesja. Identyfikator użyty dwukrotnie
> w jednej projekcji miesza dwie subskrypcje.

## Co się stanie, gdy Alicja doda mleka

1. Strona Boba pokazuje koszyk 42 w wersji 3: „Artykuły spożywcze”, dwa artykuły. Jego sesja została zasubskrybowana
   identyfikator `"7f3c..."`.
2. Alicja wysyła `AddItem(alicja, cartId, AggregateVersion(3), "milk")`. Wózek przechodzi do wersji 4.
3. Szyna zdarzeń dostarcza nowy stan do „LiveCarts”. Słuchacz zapisuje dokument i wywołuje
   `sendUpdate[CartPageUpdate](CartPageUpdate(cartId, Some(CartSummary("Artykuły spożywcze", 3, 4))))`.
4. Projekcja oferuje aktualizację każdej subskrypcji „CartPageUpdate”. Odpowiedzi Boba na filtry
   `Some(update)`; Carol, która obserwuje wózek nr 7, odpowiada `None`.
5. Sesja Boba otrzymuje `SubscriptionUpdated("7f3c...", CartPageUpdate(...))`, widzi tę wersję
   Wersja 4 jest nowsza niż wersja 3, którą pokazuje, i przesyła ją do przeglądarki. Mleko!

## Odnawianie, wygasanie i anulowanie

Subskrypcje są przechowywane w pamięci projekcji, a nie w bazie danych, i nie są przechowywane wiecznie:

- Subskrypcja, która nie została odnowiona przez **10 minut**, wygasa. Projekcja sprawdza raz
  minutę i bez słowa usuwa wygasłe subskrypcje.
- **`RenewSubscription(id)`** dla subskrypcji, o której projekcja wie, że ją odnawia, ale nie otrzymuje odpowiedzi.
  Po pierwsze, nie wie, że odpowiedź brzmi **`ProjectionSubscriptionsCancelled(List(id))`**:
  subskrypcja wygasła, projekcja została ponownie uruchomiona lub w klastrze została przeniesiona na inny węzeł
  (rozdział 17). Następnie powyższa sesja ponownie się subskrybuje i ponownie ładuje koszyk, ponieważ może
  W międzyczasie przegapiłem aktualizacje.
- **`CancelProjectionSubscriptions(ids)`** kończy subskrypcje i otrzymuje odpowiedź
  `ProjectionSubscriptionsCancelled(ids)`. Identyfikatory, których projekcja nie zna, są po prostu ignorowane.
  sesja wysyła ją w momencie zatrzymania, więc odpowiedź nie trafia donikąd, co jest w porządku.

Czas życia jest parametrem konstruktora podanym w milisekundach:

```scala
klasa LiveCartsProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[CartSummary])
  rozszerza SubscribableProjectionActor(subscriptionTTL = 5 * 60 * 1000) { // 5 minut zamiast 10
  // ... jak poprzednio
}
```

> **Pamiętaj:** odnawiaj się dobrze w czasie życia (np. co 3 minuty przez 10 minut) i lecz
> `ProjectionSubscriptionsCancelled` jako „zapisz się ponownie i załaduj ponownie”.

## Nadrabianie pominiętych aktualizacji: pamięć podręczna aktualizacji

Czasami subskrybent pojawia się chwilę za późno: strona wysyła polecenie i od razu subskrybuje
po tym, jak może przegapić impuls własnej zmiany. W tym celu projekcja subskrybowana może zachować krótki
zapamiętuje wysłane aktualizacje i odtwarza je nowym subskrybentom. Dwie rzeczy go włączają:
konstruktora `updatesCacheTTL`, `java.time.Duration` (`Duration.ZERO`, wartość domyślna, zachowuje
nic) i filtr „missedUpdatesFilter” w „handleSubscribe”, który wybiera zapamiętane aktualizacje
abonent chce (domyślnie nic):

```scala
klasa LiveCartsProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[CartSummary])
  rozszerza SubscribableProjectionActor(updatesCacheTTL = java.time.Duration.ofSeconds(30)) {

  // ... słuchacze i zapytania jak poprzednio

  nadpisz chronione def receiveSubscriptionRequest: Receive = {
    przypadek WatchCart(subscriptionId, cartId) =>
      handleSubscribe[CartPageUpdate, CartPageUpdate](subscriptionId, sender(),
        filtr = aktualizacja => jeśli (aktualizacja.cartId == cartId) Some(aktualizacja) else None,
        missingUpdatesFilter = aktualizacja => aktualizacja.cartId == cartId)
  }
}
```

Nowa subskrypcja otrzymuje teraz, zaraz po `SubscribedForProjectionUpdates`, każdą aktualizację ostatniej
30 sekund, które przechodzą przez filtr „missedUpdatesFilter”, każdy jak zwykle przez swój „filtr”. Projekcja
przechowuje w pamięci każdą aktualizację ogłoszoną w tym czasie, więc nie marnuj czasu na zajęty
występ.
Na naszej stronie „najpierw subskrybuj, potem ładuj” już wypełnia lukę; pamięć podręczna jest przeznaczona dla subskrybentów
które nie mają nic do załadowania.

## Rzeczy, o których należy pamiętać

> **Ostrzeżenie:** subskrypcje i aktualizacje są dopasowywane według typu `DATA`, porównywane według nazwy i
> musi być dokładnie tego samego typu po obu stronach. `sendUpdate` podtypu lub nadtypu tego samego typu
> zasubskrybowałeś z „nie dociera do nikogo”, bez błędu. Zapisuję parametr typu w obu wywołaniach,
> jak w `handleSubscribe[CartPageUpdate, CartPageUpdate]` i `sendUpdate[CartPageUpdate]`, zachowuje
> ich w kroku.

> **Ostrzeżenie:** aktualizacja może dotrzeć dwa razy lub trochę wcześniej. `sendUpdate` powoduje wysłanie aktualizacji o
> raz, gdy transakcja słuchacza jest nadal otwarta. Jeśli ta transakcja zostanie wycofana,
> (inny wózek w tej samej grupie nie powiódł się lub Twój program nasłuchujący zgłosił błąd po wywołaniu `sendUpdate`),
> program nasłuchujący zostanie uruchomiony ponownie, a strona może otrzymać tę samą aktualizację dwa razy; jeśli program nasłuchujący będzie się powtarzał,
> strona może nawet zobaczyć zmianę, której model odczytu nigdy nie zapisał. Dlatego przesyłaj całe dokumenty za pomocą
> ich wersji, a nie różnic, i wywołaj `sendUpdate` jako ostatnią czynność wykonywaną przez program nasłuchujący.

> **Ostrzeżenie:** każda aktualizacja uruchamia filtr każdej subskrypcji tego typu, jedną po drugiej,
> w wątku projekcji. Zachowaj filtry do szybkiego porównania. Filtr, który generuje błąd, jest rejestrowany,
> i aktualizacja w tym miejscu się zatrzymuje: subskrypcje znajdujące się po niej nie otrzymują jej.

> **Pamiętaj:** w klastrze projekcja uruchomiona za pomocą `Projections.start` działa na jednym węźle i
> wiadomości do niego mogą przekraczać sieć (rozdział 17). Twoje własne wiadomości nie mogą zawierać atrybutu `ActorRef`
> pola, co stanowi kolejny powód, aby pobrać obiekt nasłuchujący z `sender()`, a nie z pola
> `WatchCart`. Aktualizacje są przesyłane w ramach `SubscriptionUpdated` biblioteki, więc `CartPageUpdate`
> musi być prostą klasą przypadku, którą serializator klastra może zapisać, jak dowolną odpowiedź na zapytanie.

> **Pod maską:** `sendUpdate` dociera do subskrypcji za pomocą wiadomości wysyłanej przez projekcję
> do siebie, więc słuchacz może wywołać go nawet z wątków `parallelUpdateProcessing`. Kiedy
> zarządzana projekcja przełącza się na nową kompilację (rozdział 10), stara instancja przekazuje swoją
> subskrypcje, filtry dołączone do nowego, a strony nie zauważają; dlatego też
> Filtr nie może korzystać ze starych magazynów kompilacji. Raz na minutę, co powoduje wygaśnięcie subskrypcji.
> przycina również pamięć podręczną aktualizacji. Zobacz [ARCHITECTURE.md, sekcja 6.6](../ARCHITECTURE.md).

Własne testy biblioteki wykorzystują projekcje subskrybowalne w `SubscribableSpec` (w module `core`)
i w `ProjectionsSpec`, którego `ManagedNotes` wypycha linie każdej notatki, chyba że jest w trakcie przebudowy, i
utrzymuje swoich abonentów podczas odbudowy.

## Czego się nauczyłeś

- **`SubscribableProjectionActor`** to projekcja, która również wysyła aktualizacje do subskrybentów.
- Subskrybent wysyła własną wiadomość subskrybującą; `receiveSubscriptionRequest` obsługuje ją za pomocą
  **`handleSubscribe(subscriptionId, listener, filter)`**, a słuchacz otrzymuje
  `SubscribedForProjectionUpdates`.
- Słuchacze ogłaszają zmiany za pomocą **`sendUpdate[DATA](data)`**, pomijane podczas `odbudowy`; każdy
  Filtr subskrypcji zmienia aktualizację na `SubscriptionUpdated(id, update)` lub na nic.
- Subskrypcje są zapisywane w pamięci: **odnów** je w ciągu 10 minut za pomocą opcji `RenewSubscription`,
  `ProjectionSubscriptionsCancelled` jako „subskrybuj ponownie i odśwież” i **anuluj** za pomocą
  `Anuluj subskrypcje projekcji`.
- **Najpierw zasubskrybuj, potem załaduj i zachowaj najwyższą wersję**: pushe mogą się powtarzać i mogą się krzyżować
  odpowiedzi na zapytania.
- `updatesCacheTTL` z `missedUpdatesFilter` odtwarza ostatnie aktualizacje nowym subskrybentom.

Dalej: [rozdział 12](12-sagas.md) pozostawia modele odczytu i uruchamia kilka agregatów
razem.