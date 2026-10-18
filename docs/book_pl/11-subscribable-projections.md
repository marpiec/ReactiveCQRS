# Rozdział 11. Aktualizacje na żywo z projekcjami subskrybowalnymi

Pod koniec tego rozdziału strona koszyka będzie się sama odświeżać, gdy ktoś inny zmieni koszyk: Bob
ma otwarty rodzinny koszyk „Zakupy”, Alicja dodaje mleko w telefonie, a mleko pojawia się na ekranie
Boba, choć ten nie kiwnął nawet palcem. Bez przycisku odświeżania i bez dopytywania co kilka sekund.
Użyjesz do tego `SubscribableProjectionActor`: projekcji z rozdziału 8, która dodatkowo prowadzi listę
subskrybentów i wypycha im nowości.

---

## Od dopytywania do powiadomień

Zamawiasz tort urodzinowy. Możesz co dziesięć minut dzwonić do cukierni z pytaniem, czy już jest
gotowy, albo zostawić swój numer i poczekać na SMS-a. Drugi sposób to mniej pracy dla wszystkich, o
ile cukiernia pamięta twój numer i o ile co jakiś czas dajesz znać, że wciąż ci zależy (cukiernia,
która pisze na każdy numer, jaki kiedykolwiek dostała, wkrótce pisałaby do obcych ludzi).

Tak działa **subskrypcja typu push**:

- **subskrybujesz**: „daj mi znać o koszyku 42”;
- projekcja **wypycha** aktualizację za każdym razem, gdy koszyk 42 się zmieni;
- co jakiś czas **odnawiasz**: „nadal mnie to interesuje”;
- **anulujesz**, gdy strona się zamyka.

`SubscribableProjectionActor` to zwykła projekcja (listenery, magazyn dokumentów, zapytania,
wszystko z rozdziału 8), która robi wszystkie cztery rzeczy.

```
page session                                        LiveCarts projection
     │── WatchCart("7f3c...", 42) ──────────────────────▶│ handleSubscribe
     │◀────────────── SubscribedForProjectionUpdates("7f3c...")
     │                                                   │◀── cart 42 changed (from the event bus):
     │                                                   │    the listener writes, then sendUpdate(...)
     │◀────────────── SubscriptionUpdated("7f3c...", CartPageUpdate(...))
     │── RenewSubscription("7f3c...") ──────────────────▶│ every few minutes
     │── CancelProjectionSubscriptions(List("7f3c...")) ▶│ when the page closes
```

## Jak aktualizacja trafia do subskrybentów

Projekcja subskrybowalna dodaje do projekcji trzy rzeczy:

- **`receiveSubscriptionRequest`**, czyli `Receive` pisany przez ciebie, który obsługuje twoje własne
  komunikaty subskrypcji, podobnie jak `receiveQuery` obsługuje zapytania;
- **`handleSubscribe[DATA, UPDATE](subscriptionId, listener, filter)`**, które tam wywołujesz, żeby
  dodać subskrypcję;
- **`sendUpdate[DATA](data)`**, które wywołują twoje listenery, żeby ogłosić zmianę.

`DATA` oznacza to, co ogłasza projekcja, a `UPDATE` to, co dostaje subskrybent. Każde
`sendUpdate[DATA](data)` jest podsuwane każdej subskrypcji założonej z tym samym typem `DATA`, a
**filtr** subskrypcji, funkcja `DATA => Option[UPDATE]`, decyduje: `None` znaczy „nie dla mnie”,
`Some(update)` znaczy „wyślij to”. Subskrybent dostaje wtedy
`SubscriptionUpdated(subscriptionId, update)`.

Komunikaty, które subskrybent wysyła i dostaje, mieszkają w obiekcie towarzyszącym
`SubscribableProjectionActor`: `SubscribedForProjectionUpdates`, `SubscriptionUpdated`,
`RenewSubscription`, `CancelProjectionSubscriptions` i `ProjectionSubscriptionsCancelled`.

## Tworzymy stronę koszyka na żywo, krok po kroku

Użyjemy znów `CartSummary` z rozdziału 8 (nazwa, liczba pozycji i wersja koszyka) oraz wszystkiego,
co połączyliśmy w rozdziale 4. Kod leży w `examples/src/test/scala/io/reactivecqrs/examples/cart/`:
`LiveCarts.scala` oraz `LiveUpdatesSpec`, który otwiera stronę i sprawdza, co ona pokazuje.

1. **Zapisz aktualizację i komunikaty.**

   <!-- snippet: cart/LiveCarts.scala#messages -->
   ```scala
   import io.reactivecqrs.api.id.AggregateId

   case class CartPageUpdate(cartId: AggregateId, summary: Option[CartSummary]) // None: no such cart (any more)

   case class WatchCart(subscriptionId: String, cartId: AggregateId) // subscribe to one cart
   case class GetCartPage(cartId: AggregateId)                       // answered with a CartPageUpdate
   ```

   Zapytanie odpowiada tym samym typem co wypychane aktualizacje, więc strona może traktować jedno i
   drugie tak samo.

2. **Napisz projekcję.**

   <!-- snippet: cart/LiveCarts.scala#projection -->
   ```scala
   import java.time.Duration
   import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
   import io.reactivecqrs.core.projection.{SubscribableProjectionActor, SubscriptionsState}
   import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

   class LiveCartsProjection(val subscriptionsState: SubscriptionsState,
                             store: DocumentStore[CartSummary],
                             updatesCacheTTL: Duration = Duration.ZERO,
                             subscriptionTTL: Int = SubscribableProjectionActor.defaultSubscriptionTTL)
     extends SubscribableProjectionActor(updatesCacheTTL, subscriptionTTL) {

     override protected val projectionName: String = "LiveCarts"

     protected val listeners = List(
       AggregateListener[ShoppingCart] { (cartId, version, created, cart) => implicit session =>
         val summary = cart.map(c => CartSummary(c.name, c.items.size, version.asInt))
         summary match {
           case Some(s) => store.updateDocument(0, cartId.asLong, _ => Document(s))
           case None => store.removeDocument(cartId.asLong)
         }
         if (!rebuilding) sendUpdate[CartPageUpdate](CartPageUpdate(cartId, summary))
       })

     override protected def receiveSubscriptionRequest: Receive = {
       case WatchCart(subscriptionId, cartId) =>
         handleSubscribe[CartPageUpdate, CartPageUpdate](subscriptionId, sender(),
           filter = update => if (update.cartId == cartId) Some(update) else None,
           missedUpdatesFilter = update => update.cartId == cartId)
     }

     override protected def receiveQuery: Receive = {
       case GetCartPage(cartId) =>
         sender() ! CartPageUpdate(cartId, store.getDocument(cartId.asLong).map(_.document))
     }
   }
   ```

   Linijka po linijce:

   - Listener utrzymuje dokument w aktualnym stanie jak w rozdziale 8 (`updateDocument` wstawia
     dokument, gdy jeszcze go nie ma), a potem ogłasza zmianę przez `sendUpdate`.
   - `if (!rebuilding)`: gdy projekcja jest przebudowywana z historii (rozdział 10), jej listenery
     odtwarzają stare zdarzenia. Nikt nie potrzebuje na ekranie zeszłorocznego mleka.
   - `handleSubscribe[CartPageUpdate, CartPageUpdate]` podaje oba typy: to, co ogłasza `sendUpdate`,
     i to, co dostaje subskrybent. Tutaj są takie same.
   - Filtr przepuszcza aktualizacje tylko tego jednego koszyka, który obserwuje dany subskrybent.
   - Listenerem subskrypcji jest `sender()`: kto wysłał `WatchCart`, ten dostaje aktualizacje.
     Projekcja potwierdza to komunikatem `SubscribedForProjectionUpdates(subscriptionId)`, wysłanym
     do tego listenera.
   - Dwa ostatnie parametry konstruktora zostają tu przy wartościach domyślnych biblioteki, a
     `missedUpdatesFilter` ma znaczenie tylko z pamięcią podręczną: o obu mowa pod koniec rozdziału.

3. **Uruchom ją i zarejestruj**, dokładnie jak w rozdziale 8, z własną tabelą:

   <!-- snippet: cart/LiveUpdatesSpec.scala#start -->
   ```scala
   val liveCarts = system.actorOf(
     Props(new LiveCartsProjection(subscriptionsState,
       new PostgresDocumentStore[CartSummary]("live_carts", mpjsons, new NoopDocumentStoreCache)))
       .withDispatcher(BlockingDispatcher.id),
     "LiveCarts")

   eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList, liveCarts))
   ```

   Trafia na tę samą listę co wszyscy pozostali subskrybenci; szyna przyjmuje tę listę raz, przy
   starcie.

4. **Napisz obsługę strony.** Każda otwarta strona dostaje własnego małego aktora, sesję, która po
   starcie subskrybuje, przekazuje nowości do przeglądarki, odnawia subskrypcję i anuluje ją, gdy się
   zatrzymuje:

   <!-- snippet: cart/LiveCarts.scala#session -->
   ```scala
   import java.util.UUID
   import org.apache.pekko.actor.{Actor, ActorRef, Timers}
   import io.reactivecqrs.core.projection.SubscribableProjectionActor._
   import scala.concurrent.duration._

   object CartPageSession {
     private case object Renew
   }

   /** One open cart page; `push` sends a change to the browser (that part is your web layer). */
   class CartPageSession(liveCarts: ActorRef, cartId: AggregateId, push: CartPageUpdate => Unit)
     extends Actor with Timers {
     import CartPageSession.Renew

     private var subscriptionId = ""
     private var shownVersion = 0

     override def preStart(): Unit = subscribe()

     private def subscribe(): Unit = {
       subscriptionId = UUID.randomUUID().toString
       liveCarts ! WatchCart(subscriptionId, cartId) // first subscribe,
       liveCarts ! GetCartPage(cartId)               // then load what is there now
     }

     override def receive: Receive = {
       case SubscribedForProjectionUpdates(id) if id == subscriptionId =>
         timers.startTimerWithFixedDelay(Renew, Renew, 3.minutes)
       case SubscriptionUpdated(id, update: CartPageUpdate) if id == subscriptionId =>
         show(update)
       case update: CartPageUpdate => // the answer to GetCartPage
         show(update)
       case Renew =>
         liveCarts ! RenewSubscription(subscriptionId)
       case ProjectionSubscriptionsCancelled(ids) if ids.contains(subscriptionId) =>
         subscribe() // the projection forgot us: subscribe again, and load again
     }

     // Pushes and answers can overtake each other: show only what is newer than the page.
     private def show(update: CartPageUpdate): Unit = update.summary match {
       case Some(summary) if summary.version <= shownVersion => ()
       case Some(summary) =>
         shownVersion = summary.version
         push(update)
       case None => push(update)
     }

     override def postStop(): Unit = liveCarts ! CancelProjectionSubscriptions(List(subscriptionId))
   }
   ```

   Gdy strona się otwiera, uruchom sesję,

   <!-- snippet: cart/LiveUpdatesSpec.scala#openSession -->
   ```scala
   val session = system.actorOf(Props(new CartPageSession(liveCarts, cartId, update => browser.send(update))))
   ```

   a gdy się zamyka, zatrzymaj ją:

   <!-- snippet: cart/LiveUpdatesSpec.scala#closeSession -->
   ```scala
   system.stop(session)
   ```

   (`browser.send` zastępuje tu twoją warstwę webową: WebSocket, server-sent events, czegokolwiek
   używasz.)

> **Wskazówka:** najpierw subskrybuj, potem wczytuj i trzymaj najwyższą wersję. Dzięki temu, że
> subskrypcja idzie pierwsza, żadna zmiana nie prześlizgnie się między wczytaniem a subskrypcją.
> Wersja z kolei mówi stronie, która z dwóch wiadomości jest nowsza, gdy wypchnięta aktualizacja i
> odpowiedź na `GetCartPage` miną się po drodze. Dlatego `CartSummary` niesie wersję koszyka.

> **Wskazówka:** dawaj każdej subskrypcji świeży identyfikator, na przykład UUID, tak jak robi to
> sesja. Identyfikator użyty dwa razy w jednej projekcji miesza ze sobą obie subskrypcje.

## Co się dzieje, gdy Alicja dodaje mleko

1. Strona Boba pokazuje koszyk 42 w wersji 3: „Zakupy”, dwie pozycje. Jego sesja ma subskrypcję o
   identyfikatorze `"7f3c..."`.
2. Alicja wysyła `AddItem(alice, cartId, AggregateVersion(3), "milk")`. Koszyk przechodzi do
   wersji 4.
3. Szyna zdarzeń dostarcza nowy stan do `LiveCarts`. Listener zapisuje dokument i wywołuje
   `sendUpdate[CartPageUpdate](CartPageUpdate(cartId, Some(CartSummary("Groceries", 3, 4))))`.
4. Projekcja podsuwa aktualizację każdej subskrypcji `CartPageUpdate`. Filtr Boba odpowiada
   `Some(update)`; filtr Karoliny, która obserwuje koszyk 7, odpowiada `None`.
5. Sesja Boba dostaje `SubscriptionUpdated("7f3c...", CartPageUpdate(...))`, widzi, że wersja 4 jest
   nowsza niż pokazywana 3, i wypycha ją do przeglądarki. Mleko!

## Odnawianie, wygasanie i anulowanie

Subskrypcje żyją w pamięci projekcji, a nie w bazie danych, i nie żyją wiecznie:

- Subskrypcja nieodnowiona przez **10 minut** wygasa. Projekcja sprawdza to raz na minutę i bez słowa
  usuwa wygasłe subskrypcje. Subskrypcje aktora subskrybenta, który się zatrzyma, znikają od razu:
  projekcja obserwuje swoich subskrybentów.
- Projekcja trzyma najwyżej 100 000 subskrypcji; kolejna dostaje od razu
  `ProjectionSubscriptionsCancelled`. Każda aktualizacja uruchamia filtr każdej subskrypcji swojego
  typu, więc filtry powinny być tanie.
- **`RenewSubscription(id)`** dla subskrypcji, którą projekcja zna, odnawia ją i nie dostaje
  odpowiedzi. Dla nieznanej odpowiedzią jest **`ProjectionSubscriptionsCancelled(List(id))`**:
  subskrypcja wygasła, projekcja się zrestartowała albo, w klastrze, przeniosła się na inny węzeł
  (rozdział 17). Sesja z przykładu wyżej subskrybuje wtedy ponownie i jeszcze raz wczytuje koszyk, bo
  w międzyczasie mogła przegapić aktualizacje.
- **`CancelProjectionSubscriptions(ids)`** kończy subskrypcje, a odpowiedzią jest
  `ProjectionSubscriptionsCancelled(ids)`. Identyfikatory, których projekcja nie zna, są po prostu
  pomijane. Sesja wysyła ten komunikat, gdy się zatrzymuje, więc odpowiedź trafia donikąd, i to nie
  szkodzi.

Czas życia to parametr konstruktora `SubscribableProjectionActor`, w milisekundach, który projekcja
koszyków na żywo przekazuje dalej. Dla 5 minut zamiast 10:

<!-- snippet: cart/LiveUpdatesSpec.scala#ttl -->
```scala
val patientLiveCarts = Props(new LiveCartsProjection(subscriptionsState,
  new PostgresDocumentStore[CartSummary]("live_carts", mpjsons, new NoopDocumentStoreCache),
  subscriptionTTL = 5 * 60 * 1000)) // 5 minutes instead of 10
```

> **Pamiętaj:** odnawiaj z dużym zapasem przed upływem czasu życia (na przykład co 3 minuty przy 10)
> i traktuj `ProjectionSubscriptionsCancelled` jako „zasubskrybuj ponownie i wczytaj ponownie”.

## Nadrabianie przegapionych aktualizacji: pamięć podręczna aktualizacji

Czasem subskrybent przychodzi chwilę za późno: strona, która wysyła polecenie i zaraz potem
subskrybuje, może przegapić aktualizację o własnej zmianie. Na takie sytuacje projekcja
subskrybowalna może przez krótki czas pamiętać wysłane aktualizacje i odtwarzać je nowym
subskrybentom. Włączają to dwie rzeczy: parametr konstruktora `updatesCacheTTL`, typu
`java.time.Duration` (domyślne `Duration.ZERO` nie zachowuje niczego), który projekcja koszyków na
żywo przekazuje dalej,

<!-- snippet: cart/LiveUpdatesSpec.scala#cache -->
```scala
val cachingLiveCarts = system.actorOf(
  Props(new LiveCartsProjection(subscriptionsState,
    new PostgresDocumentStore[CartSummary]("cached_live_carts", mpjsons, new NoopDocumentStoreCache),
    updatesCacheTTL = java.time.Duration.ofSeconds(30)))
    .withDispatcher(BlockingDispatcher.id),
  "CachingLiveCarts")
```

oraz `missedUpdatesFilter` w `handleSubscribe`, który wybiera spośród zapamiętanych aktualizacji te,
których chce dany subskrybent (domyślnie żadnych). Subskrypcja koszyków na żywo ma taki filtr:

<!-- snippet: cart/LiveCarts.scala#subscribe -->
```scala
override protected def receiveSubscriptionRequest: Receive = {
  case WatchCart(subscriptionId, cartId) =>
    handleSubscribe[CartPageUpdate, CartPageUpdate](subscriptionId, sender(),
      filter = update => if (update.cartId == cartId) Some(update) else None,
      missedUpdatesFilter = update => update.cartId == cartId)
}
```

Nowa subskrypcja dostaje teraz, zaraz po `SubscribedForProjectionUpdates`, każdą aktualizację z
ostatnich 30 sekund, która przejdzie przez `missedUpdatesFilter`, a każda z nich jak zwykle przechodzi
też przez `filter`. Projekcja trzyma w pamięci każdą aktualizację ogłoszoną w tym czasie, więc w
ruchliwej projekcji ustaw ten czas krótko.
Na naszej stronie zasada „najpierw subskrybuj, potem wczytuj” już zamyka tę lukę; pamięć podręczna
przydaje się subskrybentom, którzy nie mają czego wczytywać.

## O czym pamiętać

> **Ostrzeżenie:** subskrypcje i aktualizacje są dopasowywane po typie `DATA`, porównywanym po nazwie,
> i po obu stronach musi to być dokładnie ten sam typ. `sendUpdate` z podtypem albo nadtypem typu, z
> którym założono subskrypcję, nie dociera do nikogo, i to bez żadnego błędu. Wpisywanie parametru
> typu w obu wywołaniach, jak w `handleSubscribe[CartPageUpdate, CartPageUpdate]` i
> `sendUpdate[CartPageUpdate]`, pilnuje, żeby się zgadzały.

> **Ostrzeżenie:** aktualizacja wychodzi dopiero wtedy, gdy transakcja twojego listenera zostanie
> zatwierdzona, nigdy wcześniej. Jeśli transakcja zostanie wycofana (zawiódł inny koszyk z tej samej
> grupy albo twój listener rzucił wyjątek po wywołaniu `sendUpdate`), nic nie zostaje wypchnięte, a
> przebieg, który się powiedzie, wypycha aktualizację raz. Ale aktualizacja wciąż może przepaść, gdy
> węzeł zatrzyma się tuż po zatwierdzeniu, a strona może przy subskrypcji zobaczyć aktualizację drugi
> raz, z pamięci podręcznej aktualizacji. Prawdą jest model odczytu: wypychaj całe dokumenty z ich
> wersją, a nie różnice, i wczytuj wszystko od nowa, gdy subskrypcja zostanie anulowana.

> **Ostrzeżenie:** każda aktualizacja uruchamia po kolei filtr każdej subskrypcji swojego typu, w
> wątku projekcji. Niech filtry ograniczają się do szybkiego porównania. Gdy filtr rzuci wyjątek,
> trafia to do logu, a tę aktualizację traci tylko jego własna subskrypcja.

> **Pamiętaj:** identyfikator subskrypcji oznacza jedną subskrypcję. Ponowna subskrypcja z
> identyfikatorem, który jest już w użyciu, zastępuje tamtą subskrypcję, a listener, do którego
> należał ten identyfikator, dostaje `ProjectionSubscriptionsCancelled`. Losowy identyfikator dla
> każdej strony, taki, jaki robi `CartPageSession`, nigdy nie trafi na inny.

> **Pamiętaj:** w klastrze projekcja uruchomiona przez `Projections.start` działa na jednym węźle, a
> komunikaty do niej mogą przechodzić przez sieć (rozdział 17). Twoje własne komunikaty nie mogą mieć
> pól `ActorRef`, i to kolejny powód, żeby brać listenera z `sender()`, a nie z pola `WatchCart`.
> Aktualizacje podróżują wewnątrz bibliotecznego `SubscriptionUpdated`, więc `CartPageUpdate` musi
> być zwykłą case class, którą serializator klastra potrafi zapisać, tak jak każda odpowiedź na
> zapytanie.

> **Pod maską:** `sendUpdate` dociera do subskrypcji przez komunikat, który projekcja wysyła sama do
> siebie, gdy transakcja listenera zostanie zatwierdzona (`afterCommit`, z którego mogą korzystać też
> twoje projekcje), więc listener może je wywołać nawet z wątków `parallelUpdateProcessing`. Gdy
> projekcja zarządzana przełącza się na nową budowę (rozdział 10), stara instancja przekazuje nowej
> swoje subskrypcje, razem z filtrami, a strony niczego nie zauważają; dlatego też filtr nie może
> korzystać z magazynów starej budowy. Sprawdzanie raz na minutę, które kończy wygasłe subskrypcje,
> przy okazji przycina pamięć podręczną aktualizacji. Zobacz
> [ARCHITECTURE.md, sekcja 6.6](../ARCHITECTURE.md).

Własne testy biblioteki używają projekcji subskrybowalnych w `SubscribableSpec` (w module `core`) i w
`ProjectionsSpec`, którego `ManagedNotes` wypycha linie każdej notatki, chyba że trwa przebudowa, i
zachowuje swoich subskrybentów przez całą przebudowę.

## Co już umiesz

- **`SubscribableProjectionActor`** to projekcja, która dodatkowo wypycha aktualizacje do
  subskrybentów.
- Subskrybent wysyła twój własny komunikat subskrypcji; `receiveSubscriptionRequest` obsługuje go
  przez **`handleSubscribe(subscriptionId, listener, filter)`**, a listener dostaje
  `SubscribedForProjectionUpdates`.
- Listenery ogłaszają zmiany przez **`sendUpdate[DATA](data)`**, pomijane podczas przebudowy
  (`rebuilding`); filtr każdej subskrypcji zamienia aktualizację w `SubscriptionUpdated(id, update)`
  albo w nic.
- Subskrypcje żyją w pamięci: **odnawiaj** je przed upływem 10 minut przez `RenewSubscription`,
  traktuj `ProjectionSubscriptionsCancelled` jako „zasubskrybuj ponownie i wczytaj od nowa”, a
  **anuluj** przez `CancelProjectionSubscriptions`.
- **Najpierw subskrybuj, potem wczytuj i trzymaj najwyższą wersję**: wypchnięte aktualizacje mogą
  się powtarzać i mijać z odpowiedziami na zapytania.
- `updatesCacheTTL` razem z `missedUpdatesFilter` odtwarza nowym subskrybentom niedawne aktualizacje.

W [rozdziale 12](12-sagas.md) zostawiamy modele odczytu i sprawiamy, że kilka agregatów zaczyna ze
sobą współpracować.
