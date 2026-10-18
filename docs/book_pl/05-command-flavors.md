# Rozdział 5. Polecenia mają różne smaki

W rozdziale 4 utworzyliśmy koszyk i włożyliśmy do niego jabłka. Nawet tego nie zauważając, użyliśmy
dwóch różnych rodzajów poleceń. ReactiveCQRS ma ich jeszcze kilka i każdy odpowiada na inne pytanie:
„czy to coś nowego?”, „a co, jeśli ktoś to zmieni, kiedy na to patrzę?”, „a co, jeśli moja prośba
dotrze dwa razy?”. Pod koniec rozdziału będziesz znać każdy rodzaj polecenia i wiedzieć, który
wybrać, co znaczy każda odpowiedź szyny poleceń i co robić, gdy handler polecenia działa wolno.
Poznasz też przełącznik, dzięki któremu agregat przyjmuje polecenia po jednym, oraz sposób na
zapisywanie własnych wierszy razem ze zdarzeniami.

---

## Menu

Pomyśl o restauracji. „Poproszę stolik dla dwojga” to prośba o coś nowego. „Stolik 12: zupa zamiast
sałatki, mam to zamówienie przed oczami” zmienia zamówienie, które znasz. „Jeszcze jedna szklanka
wody do stolika 12” też je zmienia, ale nikogo nie obchodzi, co jeszcze jest na rachunku. ReactiveCQRS
ma rodzaj polecenia na każdą z tych sytuacji:

| Rodzaj | Co mówi | W koszyku | Gdy ktoś zmienił agregat przed tobą |
|---|---|---|---|
| `FirstCommand` | „utwórz nowy” | `CreateShoppingCart` | (jest nowy) |
| `Command` | „zmień go; ostatnio widziana wersja: 3” | `AddItem` | dowiadujesz się o tym, a nic nie zostaje zapisane |
| `ConcurrentCommand` | „zmień go, obojętnie, w jakiej jest wersji” | `AddItemConcurrently` | biblioteka uruchamia je ponownie na nowszym stanie |

Dwa kolejne rodzaje poprawiają przeszłość, zamiast coś do niej dopisywać. Dostają krótką sekcję na
końcu tego rozdziału i cały rozdział 15 dla siebie.

Każdy rodzaj to klasa abstrakcyjna w `io.reactivecqrs.api`, a każda case class polecenia rozszerza
dokładnie jedną z nich. Pierwszy parametr typu to klasa stanu agregatu, drugi określa rodzaj
odpowiedzi. Domena testowa zawsze wpisuje tam `CustomCommandResponse[_]` i ty możesz robić tak samo:

<!-- snippet: shoppingcart/Commands.scala#addItem -->
```scala
case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]
```

Od tego miejsca przykłady używają koszyka z domeny testowej
(`io.reactivecqrs.testdomain.shoppingcart`), większego brata koszyka z rozdziału 3: ten sam stan, a
do tego dodatkowe polecenia i zdarzenia, o których mówi ten i następny rozdział. Jego
`CreateShoppingCart` ma jedno pole więcej, wyjaśnione niżej. Każdy blok tego rozdziału, który coś
wysyła, jest wykonywany w `CommandKindsSpec` (`examples/src/test/scala/io/reactivecqrs/examples/cart/`),
tuż przed liniami, które sprawdzają jego odpowiedzi; uruchomisz go przez
`sbt "examples/testOnly *CommandKindsSpec"`. Przykłady importują

<!-- snippet: cart/CommandKindsSpec.scala#imports -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._
```

i zaczynają od takiego przygotowania, w klasie, która ma system aktorów (`system`) i szynę poleceń
koszyków z rozdziału 4 (`commandBus`):

<!-- snippet: cart/CommandKindsSpec.scala#setup -->
```scala
import system.dispatcher                             // runs the map and foreach callbacks below
implicit val timeout: Timeout = Timeout(70.seconds)  // longer than the command timeout; see "Slow handlers"

val alice = UserId(1)
val bob = UserId(2)
```

## FirstCommand: tworzenie czegoś nowego

To polecenie znasz już z rozdziałów 3 i 4. Wersja z domeny testowej ma jedno pole więcej:

<!-- snippet: shoppingcart/Commands.scala#create -->
```scala
case class CreateShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
```

(Na razie pomiń `idempotencyId`; dostanie własną sekcję niżej.)

`FirstCommand` nie ma identyfikatora agregatu, bo agregat jeszcze nie istnieje. Szyna poleceń bierze
nowy identyfikator z puli pobranej zawczasu, twój handler działa na `initialAggregateRoot`
kontekstu, a odpowiedź mówi, jaki identyfikator dostał nowy koszyk:

<!-- snippet: cart/CommandKindsSpec.scala#create -->
```scala
val answer = (commandBus ? CreateShoppingCart(None, alice, "Groceries")).mapTo[CustomCommandResponse[_]]
answer.foreach {
  case SuccessResponse(cartId, version) => println(s"New cart $cartId at $version")  // version: AggregateVersion(1)
  case other                            => println(s"No cart: $other")
}
```

Handler `FirstCommand` odpowiada zdarzeniem `FirstEvent`, czyli takim, które otwiera pamiętnik i
mówi, do której organizacji należy nowy agregat. Szczegóły w rozdziale 6.

> **Wskazówka:** nowy identyfikator pojawia się tylko w odpowiedzi. Jeśli odpowiedź zginie po drodze
> (czkawka sieci, przekroczony limit czasu), a klient wyśle polecenie jeszcze raz, powstaną dwa
> koszyki. Rozwiązanie znajdziesz niżej, w sekcji „Samodzielny wybór identyfikatora”.

## Command: „ostatnio widziana wersja: 3”

Alicja i Bob patrzą na koszyk „Zakupy” w wersji 3. Alicja wyjmuje chleb; koszyk przechodzi do
wersji 4. Bob, który wciąż widzi wersję 3, dodaje masło, bo przecież jest chleb, który można nim
posmarować. Czy zmiana Boba powinna przejść? Bob zdecydował na podstawie czegoś, co już nie jest
prawdą.

`Command` niesie `expectedVersion`: wersję, na której nadawca oparł decyzję. Zanim właściciel
koszyka cokolwiek zapisze, porównuje tę wersję z wersją koszyka:

- **taka sama:** zdarzenia twojego handlera zostają zapisane jako kolejne wersje;
- **inna:** nic nie zostaje zapisane, a ty dostajesz `AggregateConcurrentModificationError`. Zwykle
  twój handler nawet się nie uruchamia.

<!-- snippet: cart/CommandKindsSpec.scala#addItem -->
```scala
def addItem(cartId: AggregateId, seen: AggregateVersion, item: String): Future[String] =
  (commandBus ? AddItem(bob, cartId, seen, item)).mapTo[CustomCommandResponse[_]].map {
    case SuccessResponse(_, version) =>
      s"Added $item; the cart is at version ${version.asInt} now"
    case AggregateConcurrentModificationError(_, _, expected, was) =>
      s"The cart changed while you looked (you saw version ${expected.asInt}, it is at ${was.asInt})"
    case FailureResponse(reasons) =>
      "Refused: " + reasons.mkString("; ")
    case other =>
      "Something went wrong: " + other
  }
```

Błąd niesie cztery rzeczy: identyfikator agregatu, jego typ, oczekiwaną przez ciebie wersję i
wersję, w której agregat faktycznie był. Typowa reakcja to odczytać agregat jeszcze raz
(`GetAggregate`), pokazać go i pozwolić użytkownikowi (albo twojemu kodowi) zdecydować od nowa.

Polecenie dla koszyka, który nie istnieje, dostaje ten sam błąd, z `was` równym
`AggregateVersion.ZERO`: wersja 0 to wersja niczego.

> **Pamiętaj:** wersję do następnego `Command` bierz z najnowszej odpowiedzi.
> `SuccessResponse(cartId, AggregateVersion(4))` znaczy „koszyk ma teraz wersję 4”, więc następne
> polecenie oczekuje wersji 4.

> **Pod maską:** wersja jest sprawdzana dwa razy: przez właściciela w pamięci i jeszcze raz przez
> funkcję PostgreSQL, która zapisuje zdarzenia; ta druga wyłapuje też zapisy, których właściciel nie
> widzi. Zobacz [ARCHITECTURE.md, sekcja 4.2](../ARCHITECTURE.md).

## ConcurrentCommand: „obojętnie, jaka jest teraz wersja”

Rodzinna lista zakupów na lodówce: dopisujesz na dole „mleko”. Nie obchodzi cię, co inni dopisali od
twojego ostatniego spojrzenia; mleko i tak jest potrzebne.

<!-- snippet: shoppingcart/Commands.scala#concurrently -->
```scala
// No expected version: the framework uses the current one and retries on a concurrent change.
case class AddItemConcurrently(userId: UserId, aggregateId: AggregateId, name: String,
                               idempotencyId: Option[ClientKey] = None)
  extends ConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[ClientKey]
```

`ConcurrentCommand` ma identyfikator agregatu, ale nie ma oczekiwanej wersji. Twój handler działa na
koszyku takim, jaki jest w tej chwili. Jeśli w trakcie jego działania zostaną zapisane zdarzenia
innego polecenia, biblioteka wyrzuca twój wynik i uruchamia handler jeszcze raz, na nowszym stanie.
Dostajesz jedną odpowiedź: sukces, z wersją, na której wylądowały twoje zdarzenia.

<!-- snippet: cart/CommandKindsSpec.scala#concurrent -->
```scala
val milk = commandBus ? AddItemConcurrently(alice, cartId, "milk")
// answers SuccessResponse(cartId, AggregateVersion(n)), n being the version the milk landed on
```

Jak bardzo biblioteka się stara?

- **Polecenia trafiające do tego samego właściciela** rozstrzygają kolizje, ustawiając się w
  kolejce: te, które się zderzyły, czekają na swoją kolej i wykonują się ponownie, po jednym, a
  czekanie nigdy nie liczy się jako nieudana próba. Pomiar: każde z 10 000 poleceń wysłanych naraz do jednego agregatu zakończyło
  się sukcesem, a ostatnie dostało odpowiedź po 3,8 s (pojedynczy przebieg na jednej maszynie
  deweloperskiej, 17/10/2026, [`docs/perf/load-M5.md`](../perf/load-M5.md)).
- **Kolizje z zapisami, których właściciel nie widzi** (z zapisami właściciela tego samego agregatu
  na innym węźle, rozdział 17, albo z zapisem wykonanym poza biblioteką), są ponawiane: najwyżej
  dziesięć prób w sumie, z przerwą 10 ms przed drugą, podwajaną aż do 500 ms, i z losowym
  składnikiem, żeby rywale się rozproszyli. Po dziesiątej dostajesz
  `AggregateConcurrentModificationError`.

> **Ostrzeżenie:** handler `ConcurrentCommand` może się wykonać więcej niż raz dla jednego polecenia.
> Niech nie ma efektów ubocznych: żadnych e-maili, żadnych płatności, żadnych wywołań, które coś
> zmieniają gdzie indziej. To dobra rada dla każdego handlera, bo wynik każdego z nich może zostać
> wyrzucony (kolizja, przekroczony limit czasu). Miejsce efektów ubocznych jest w sagach i
> projekcjach, które reagują na zdarzenia, gdy te są już zapisane (rozdziały 8 i 12).

### Którego użyć?

- Decyzja zależy od tego, co widział użytkownik („masło, bo jest chleb”, „zapłać kwotę, którą mi
  pokazano”): użyj `Command`.
- Zmiana ma sens bez względu na to, co jeszcze się wydarzyło („dodaj mleko”, „dolicz jeszcze jedną
  wizytę”): użyj `ConcurrentCommand`.

O tym, jak zostanie obsłużona kolizja, decyduje rodzaj polecenia; nie ma ustawienia, które by to
zmieniało.

## Samodzielny wybór identyfikatora

Pomyśl o papierowym formularzu zamówienia z nadrukowanym numerem w rogu. Jeśli przez pomyłkę wyślesz
go dwa razy, sklep zobaczy dwa razy ten sam numer i zrealizuje jedno zamówienie. `FirstCommand` może
nieść taki numer: identyfikator agregatu, który tworzy.

1. Poproś szynę poleceń o nowy identyfikator komunikatem `ReserveAggregateId`. Odpowiedzią jest
   `AggregateId` z tej samej puli, z której korzysta sama szyna.
2. Zanim cokolwiek wyślesz, zachowaj identyfikator razem z żądaniem (w kliencie, w swoim dzienniku
   żądań).
3. Wyślij `FirstCommand`, które podaje ten identyfikator w `chosenAggregateId`.
4. Jeśli odpowiedź w ogóle nie przyszła, wyślij dokładnie to samo polecenie jeszcze raz.

Domena testowa ma takie polecenie:

<!-- snippet: shoppingcart/Commands.scala#createWithId -->
```scala
/** Creates a cart under an id the caller reserved beforehand, so a retry cannot create a second cart. */
case class CreateShoppingCartWithId(idempotencyId: Option[ClientKey], userId: UserId, cartId: AggregateId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[ClientKey] {
  override def chosenAggregateId: Option[AggregateId] = Some(cartId)
}
```

<!-- snippet: cart/CommandKindsSpec.scala#reserve -->
```scala
val created: Future[CustomCommandResponse[_]] = for {
  cartId   <- (commandBus ? ReserveAggregateId).mapTo[AggregateId]
  response <- (commandBus ? CreateShoppingCartWithId(None, alice, cartId, "Groceries")).mapTo[CustomCommandResponse[_]]
} yield response   // SuccessResponse(cartId, AggregateVersion(1))
```

Wysłane drugi raz, to polecenie nie utworzy drugiego koszyka. Koszyk już istnieje, więc kopia dostaje
odpowiedź
`AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart], AggregateVersion.ZERO, AggregateVersion(1))`:
„miało tu nie być niczego, a jest wersja 1”. Z kluczem idempotencji (następna sekcja) kopia dostaje
zamiast tego dokładnie ten sam `SuccessResponse`, co jest bardziej przyjazne.

### Jakie identyfikatory można wybrać?

Bierz je z `ReserveAggregateId`, a nie będziesz musieć się nad tym zastanawiać. Jeśli wybierasz
identyfikatory inaczej, szyna poleceń sprawdza je, zanim cokolwiek się wykona, i odpowiada
`FailureResponse` dla identyfikatora, który:

- nie jest dodatni (zero lub mniej; ujemne identyfikatory są zarezerwowane dla agregatów
  efemerycznych, żyjących w pamięci, [rozdział 20](20-ephemeral-aggregates.md)),
- nie jest mniejszy niż 2^53 (9 007 199 254 740 992), czyli największa liczba całkowita, jaką klient
  w JavaScripcie odczytuje dokładnie,
- należy do zakresu innej instalacji.

*Instalacja* to jedno wdrożenie twojej aplikacji z własną bazą danych: na przykład jedno dla Europy i
jedno dla Azji. Każda ustawia `reactivecqrs.installation-number` (domyślnie 0), a instalacja *n*
tworzy identyfikatory od *n* × 10^12 aż do miejsca, w którym zaczyna się instalacja *n* + 1, więc
identyfikatory dwóch instalacji nigdy się nie zderzą, jeśli ich dane kiedyś się spotkają. Każdy węzeł
jednej instalacji ustawia ten sam numer:

```
# application.conf of installation 3: its ids start at 3 000 000 000 000
reactivecqrs.installation-number = 3
```

Identyfikatory poniżej 10^12, czyli z zakresu, w którym żyją wszystkie identyfikatory utworzone przed
wersją 1.0, są dozwolone w każdej instalacji.

> **Pod maską:** zakresy są zdefiniowane w `IdRange` (`core/.../uid/IdRange.scala`). Zobacz
> [ARCHITECTURE.md, sekcja 13.5](../ARCHITECTURE.md).

## Polecenia idempotentne: bezpieczne, nawet gdy wyślesz je dwa razy

Klikasz „Zapłać” w sklepie internetowym, a strona się zawiesza. Klikasz jeszcze raz. Czy płacisz
podwójnie? Dobry sklep nadaje obu kliknięciom ten sam numer zamówienia, a drugie kliknięcie zastaje
zamówienie już złożone.

Polecenie, które działa raz na klucz, niezależnie od tego, ile razy zostanie wysłane (i ile kopii
dotrze w tej samej chwili), nazywamy *idempotentnym*. Takie polecenie tworzysz, dołączając trait
`IdempotentCommand` i nadając mu `idempotencyId`:

```scala
trait IdempotentCommand[IID <: CommandIdempotencyId] {
  val idempotencyId: Option[IID]
}
```

Kluczem może być dowolna klasa z `asDbKey: String`. Domena testowa ma klucz dla klientów:

<!-- snippet: shoppingcart/Commands.scala#clientKey -->
```scala
/** A key chosen by the caller, the way a client retrying a request would send it. */
case class ClientKey(value: String) extends CommandIdempotencyId {
  override def asDbKey: String = value
}
```

a biblioteka ma klucz dla sag: `SagaStep(sagaId, step)`, zapisywany jako `"sagaId|step"`. Sagi
(rozdział 12) dają poleceniu każdego kroku ten krok jako klucz, więc saga wznowiona po awarii nie
może zastosować kroku dwa razy.

Oto śliwka dodana dwa razy z tym samym kluczem:

<!-- snippet: cart/CommandKindsSpec.scala#plum -->
```scala
val key = Some(ClientKey(java.util.UUID.randomUUID().toString))  // one key per user action
val addPlum = AddItemWithReceipt(key, alice, cartId, AggregateVersion(1), "plum")

val first  = commandBus ? addPlum  // CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))
val second = commandBus ? addPlum  // the very same answer; there is one plum in the cart
```

Odpowiedź pierwszej kopii zostaje zapisana w tej samej transakcji co jej zdarzenia. Każda późniejsza
kopia, a także każda, która ściga się z pierwszą, dostaje tę zapisaną odpowiedź, a jej własne
zdarzenia nigdy nie zostają zapisane. Działa to także po restarcie: odpowiedzi są przechowywane w
PostgreSQL. Klucz `None` znaczy „tym razem bez idempotencji”, jak w
`CreateShoppingCart(None, alice, "Groceries")` z przykładów.

Zasady:

- **Klucze są globalne**, wspólne dla wszystkich typów agregatów. Używaj świeżego UUID dla każdej
  akcji użytkownika, nigdy czegoś w rodzaju `"cart-1"`: `"cart-1"` innego typu dostałoby zapisaną
  odpowiedź twojego koszyka. Klucz może mieć do 256 znaków.
- **Odpowiedź zostaje zachowana tylko wtedy, gdy polecenie zapisało zdarzenia.** Kopia polecenia
  odrzuconego (`FailureResponse`) albo takiego, które nie wytworzyło żadnych zdarzeń, po prostu
  wykonuje się ponownie.
- **Odpowiedzi są domyślnie przechowywane przez 30 dni**, a każda szyna poleceń co godzinę usuwa
  starsze. Kopia, która przyjdzie później, wykona się jako nowe polecenie. Inny okres ustawisz przy
  tworzeniu magazynu odpowiedzi:

  <!-- snippet: cart/CommandKindsSpec.scala#retention -->
  ```scala
  val responses = new PostgresCommandResponseState(mpjsons, typesNames, retention = 7.days).initSchema()
  ```

- **Dodatkowa informacja w zapisanej odpowiedzi** (zobacz niżej „Sukces z dodatkiem”) musi być klasą
  bez parametrów typu, na przykład case class taką jak `ItemReceipt`.

> **Ostrzeżenie:** idempotencja potrzebuje tabeli odpowiedzi i jej unikalnego indeksu, które tworzy
> `PostgresCommandResponseState.initSchema()`. Zapomnij o `initSchema()`, a każde polecenie
> idempotentne zakończy się błędem.

> **Wskazówka:** przy tworzeniu połącz obie sztuczki: zarezerwuj identyfikator i daj poleceniu
> tworzącemu klucz, jak w `CreateShoppingCartWithId(Some(ClientKey(...)), alice, cartId, "Groceries")`.
> Dwie ścigające się kopie, nawet wysłane przez dwa różne węzły, obie dostaną
> `SuccessResponse(cartId, AggregateVersion(1))`.

## Co może odpowiedzieć handler

Handler polecenia to funkcja, która z bieżącego stanu robi wynik. Oto możliwe wyniki, od tych
codziennych po bardziej nietypowe.

### Jedno zdarzenie albo kilka

`CommandSuccess(ItemAdded(c.name))` zapisuje jedno zdarzenie. Handler może też zwrócić kilka, a te
dostaną kolejne wersje:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#addItems -->
```scala
case c: AddItems => CommandSuccess(c.names.map(ItemAdded))
```

Na `AddItems(alice, cartId, AggregateVersion(1), Seq("apple", "pear"))` przychodzi odpowiedź
`SuccessResponse(cartId, AggregateVersion(3))`. Zdarzenia jednego polecenia są zapisywane razem albo
wcale: jeśli drugiego nie da się zapisać, pierwsze też nie zostanie zapisane.

### Sukces z dodatkiem

Czasem wywołujący potrzebuje czegoś więcej niż nowej wersji: pokwitowania, wyliczonej liczby,
wygenerowanego kodu. Przekaż to jako drugi argument. Pokwitowanie w domenie testowej to osobna case
class, `ItemReceipt(itemName: String)`:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#receipt -->
```scala
case c: AddItemWithReceipt => CommandSuccess(ItemAdded(c.name), ItemReceipt(c.name))
```

Odpowiedzią jest wtedy `CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))`
zamiast zwykłego `SuccessResponse`.

### Nie

Reguła mówi „nie”: zwróć `CommandFailure` z komunikatem. Nic nie zostaje zapisane, a wywołujący
dostaje ten komunikat z powrotem. Twój koszyk z rozdziału 3 pilnuje w ten sposób swojej zasady
domowej:

<!-- snippet: e1/Carts.scala#addItemRule -->
```scala
case c: AddItem =>
  if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
  else CommandSuccess(ItemAdded(c.name))
```

Odpowiedź to `FailureResponse(List("Cannot have more than 5 items in your cart"))`. Używaj tego do
reguł biznesowych; wyjątki zostaw na sytuacje, w których coś naprawdę się zepsuło.

### Nic do zrobienia

Niektóre polecenia sprawdzają stan i nie znajdują nic do zmiany: cykliczna kontrola albo prośba,
która jest już spełniona. Zwróć sukces bez zdarzeń:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#checkCart -->
```scala
case _: CheckCart => CommandSuccess(Seq.empty[Event[ShoppingCart]])
```

Odpowiedź to `SuccessResponse(cartId, AggregateVersion(1))` z bieżącą wersją koszyka, a nic nie
zostaje zapisane: nie ma zdarzenia ani nowej wersji, a do bazy danych nie trafia nawet jedno
polecenie SQL dotyczące koszyka. Zestaw testów wysyła 10 000 takich poleceń i sprawdza dokładnie to.

### Później: handlery asynchroniczne

Handler, który musi na coś poczekać, na przykład na usługę stanów magazynowych, zwraca
`AsyncCommandResult` opakowujący `Future` z jednym z powyższych wyników. Tak właśnie jest napisany
handler `AddItem` w domenie testowej. Oto wariant, który najpierw pyta twoją własną usługę
magazynową:

<!-- snippet: cart/CommandKindsSpec.scala#stock -->
```scala
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

/** Your own service, not part of the library. */
trait Stock {
  def available(item: String): Future[Boolean]
}

class StockCheckingCarts(stock: Stock) extends ShoppingCartAggregateContext {
  // a bounded pool of your own for the follow-up work
  private implicit val stockPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  override def commandHandlers = cart => {
    case c: AddItem =>
      AsyncCommandResult(stock.available(c.name).map {
        case true  => CommandSuccess(ItemAdded(c.name))
        case false => CommandFailure(c.name + " is out of stock")
      })
    case other => super.commandHandlers(cart)(other)
  }
}
```

Twój handler kończy działanie w chwili, gdy ma `Future`, co zwalnia jego wątek; właściciel odbierze
wynik, gdy ten będzie gotowy. W `AggregateContext` możesz nawet zwrócić sam `Future`: kontekst
zamieni go za ciebie na `AsyncCommandResult` i właśnie na tym opiera się `createShoppingCart` z
domeny testowej.

## Wszystkie odpowiedzi w skrócie

Cokolwiek się stanie, polecenie dostaje dokładnie jedną odpowiedź:

| Odpowiedź | Co się stało | Co zrobić |
|---|---|---|
| `SuccessResponse(id, version)` | Zapisane albo nie było nic do zapisania. `version` to bieżąca wersja agregatu. | Działaj dalej; `version` użyj w następnym `Command`. |
| `CustomSuccessResponse(id, version, info)` | To samo, razem z `info` zwróconym przez twój handler. | Odczytaj `info`. |
| `FailureResponse(reasons)` | Twój handler powiedział „nie”; albo szyna odrzuciła polecenie przed jego wykonaniem: wybrany identyfikator jest niedozwolony, pierwsze zdarzenie nowego agregatu nie wskazuje przestrzeni (space) (rozdział 6) albo, w klastrze z agregatami shardowanymi, polecenie jest za duże, by przesłać je między węzłami (rozdział 17). | Pokaż powody. |
| `AggregateConcurrentModificationError(id, type, expected, was)` | Agregat nie jest w oczekiwanej wersji: ktoś go zmienił, nie istnieje (`was` to 0) albo wybrany identyfikator jest już zajęty. `ConcurrentCommand` dostaje ten błąd dopiero po dziesięciu próbach. | Odczytaj agregat ponownie i zdecyduj od nowa. |
| `CommandRejected(id, reason)` | Polecenie się nie wykonało: na ten agregat czeka już zbyt wiele poleceń (przetwarzanie szeregowe, niżej) albo jego zdarzenia są klasy, której nie każdy węzeł klastra potrafi jeszcze odczytać (rozdział 17). | Wyślij je ponownie nieco później. |
| `CommandHandlingError(command, errorId, commandId)` | Twój handler rzucił wyjątek albo nie miał `case` dla tego polecenia; działał dłużej niż limit czasu; nie było agregatu, na którym mógłby działać (usunięty albo, w przypadku `ConcurrentCommand`, nieistniejący); agregatu nie dało się odczytać z magazynu zdarzeń; albo szyna nie miała identyfikatorów. | Poszukaj `errorId` w logu. |
| `EventHandlingError(event, errorId, commandId)` | Zapis się nie udał: handler zdarzenia rzucił wyjątek, zdarzenie było za duże albo agregat ma za dużo zdarzeń (rozdział 6), albo zawiodła baza danych. Nic nie zostało zapisane. | Poszukaj `errorId` w logu. |

Na komunikat, który w ogóle nie jest poleceniem, nie przychodzi żadna z tych odpowiedzi: szyna kończy
`Future` twojego zapytania błędem `IllegalArgumentException`.

> **Wskazówka:** pokaż `errorId` osobie, która zgłasza problem. Biblioteka zapisuje ten sam
> identyfikator w swoim logu razem z przyczyną (na przykład
> „Error handling command AddItem, errorId: [...]”), więc dział wsparcia znajdzie go w kilka sekund.

## Wolne handlery

Większości handlerów wystarczają mikrosekundy. Niektóre muszą jednak wywołać wolną usługę albo dużo
policzyć. Wtedy liczą się dwie rzeczy: gdzie handler działa i jak długo może trwać.

### Gdzie działają handlery

Handlery poleceń działają na własnej puli wątków, `reactivecqrs.handler-dispatcher`, z 32 wątkami
(według `reference.conf` biblioteki). Szyny, projekcje i sagi działają gdzie indziej, więc wolny
handler spowalnia tylko polecenia. Ale handler, który *trzyma* swój wątek (długie obliczenia,
`Thread.sleep`, blokujące wywołanie HTTP albo JDBC), zajmuje jeden z tych 32, a gdy zajęte są
wszystkie 32, czeka każde polecenie na tym węźle.

Pomiar z 17/10/2026 (pojedynczy przebieg na jednej maszynie deweloperskiej z 12 wątkami procesora i
PostgreSQL na tej samej maszynie, [`docs/perf/heavy-M5.md`](../perf/heavy-M5.md)): na 36 poleceń,
których handlery trzymały wątek po 5 sekund każdy, wszystkie odpowiedzi przyszły po 10,0 s, bo naraz
działały 32, a w tym czasie lekkie polecenie dla innego agregatu czekało 5,4 s. Gdy tę samą pracę
wykonywała własna pula aplikacji, a handler zwracał `AsyncCommandResult`, ostatnia odpowiedź przyszła
po 5,0 s, a lekkie polecenie zajęło 3 ms.

Rozmiar puli ustawisz w swoim `application.conf`:

```
reactivecqrs.handler-dispatcher.thread-pool-executor.fixed-pool-size = 64
```

Więcej wątków przyda się handlerom, które czekają na wejście/wyjście; handlerom, które liczą,
wystarczy mniej więcej tyle wątków, ile procesor ma rdzeni.

### Limit czasu polecenia

Każdy kontekst ma `commandTimeout`: 60 sekund, chyba że go nadpiszesz. Czas liczy się od chwili, gdy
właściciel agregatu przyjmie polecenie. Gdy minie, wywołujący dostaje `CommandHandlingError`, a
polecenie jest martwe: jeśli handler skończy później, jego wynik zostanie wyrzucony i nic nie zostanie
zapisane. Kontekstowi możesz dać więcej czasu:

<!-- snippet: cart/CommandKindsSpec.scala#patient -->
```scala
class PatientCarts extends ShoppingCartAggregateContext {
  override val commandTimeout: FiniteDuration = 2.minutes
}
```

> **Ostrzeżenie:** twój własny limit czasu zapytania (`ask`) to osobny zegar. Jeśli jest krótszy niż
> limit czasu polecenia, twój `Future` kończy się błędem `AskTimeoutException`, a polecenie toczy się
> dalej i wciąż może zostać zastosowane. Ustaw limit czasu zapytania dłuższy niż limit czasu
> polecenia (jak w przygotowaniu z tego rozdziału), a poleceniom, które klienci ponawiają, daj klucz
> idempotencji.

### Długa praca bez nerwów

Dla handlerów, które pracują minutami, notatki biblioteki o skalowaniu zalecają, zaczynając od
najlepszego rozwiązania ([`docs/SCALING.md`](../SCALING.md), sekcja 8):

1. **Wynieś długą pracę poza polecenie.** Polecenie zapisuje prośbę (jedno zdarzenie); proces roboczy
   albo saga (rozdział 12) wykonuje pracę na własnych wątkach, z ponawianiem i możliwością
   wznowienia; drugie polecenie zapisuje wynik. Agregat pozostaje dostępny, a długiej pracy nie
   dotknie żaden limit czasu ani żadna kolizja.
2. **Jeśli polecenie musi to zrobić samo:** zwróć `AsyncCommandResult`, a pracę wykonaj na własnej,
   ograniczonej puli; podnieś `commandTimeout` powyżej najdłuższego przebiegu; dla tego typu włącz
   przetwarzanie szeregowe (następna sekcja), żeby praca nigdy nie była powtarzana po kolizji; a
   klientom daj klucz idempotencji i odpowiednio długi limit czasu albo od razu odpowiadaj
   „przyjęto”, a wynik pokazuj później przez projekcję.

## Jedno po drugim: przetwarzanie szeregowe

Domyślnie właściciel pozwala poleceniom swojego agregatu działać równolegle, każdemu na stanie, jaki
zastało, i rozstrzyga kolizje dopiero potem: błędem dla `Command`, ponownym uruchomieniem dla
`ConcurrentCommand`. Przy agregacie, który wiele osób zmienia naraz, spokojniej bywa z jedną kolejką,
jak przy okienku na poczcie: każdy czeka na swoją kolej i każdy widzi dokładnie to, co zostawił po
sobie poprzednik.

<!-- snippet: cart/CommandKindsSpec.scala#busy -->
```scala
class BusyCarts extends ShoppingCartAggregateContext {
  override val commandProcessing: CommandProcessing = CommandProcessing.Serial(maxQueuePerAggregate = 100)
}
```

Z `Serial`:

- polecenia jednego agregatu wykonują się po jednym, w kolejności nadejścia; każde rusza, gdy
  poprzednie dostało odpowiedź, więc jego handler widzi najnowszy stan;
- nie mogą kolidować ze sobą, więc handler `ConcurrentCommand` wykonuje się raz. (`Command` z
  nieaktualną oczekiwaną wersją nadal dostaje `AggregateConcurrentModificationError`.)
- za wykonywanym poleceniem czeka najwyżej `maxQueuePerAggregate` poleceń (1 000, jeśli napiszesz
  samo `Serial()`); polecenie, które już się nie mieści, od razu dostaje odpowiedź
  `CommandRejected(cartId, "100 commands of the aggregate wait already")` i można je wysłać ponownie
  później;
- limit czasu polecenia liczy się od jego nadejścia, razem z czasem spędzonym w kolejce. Dziesięć
  poleceń po minucie każde, jedno za drugim, wymaga dla ostatniego z nich limitu czasu dłuższego niż
  dziesięć minut.

Drugi parametr, `onOverflow`, mówi, co zrobić z tym jednym poleceniem za dużo; dziś ma tylko jedną
możliwość: `OverflowPolicy.Reject`.

Pomiar z 17/10/2026 (ta sama maszyna, [`docs/perf/load-M5.md`](../perf/load-M5.md)): 10 000 poleceń
wysłanych naraz do jednego agregatu w trybie szeregowym dało 1 003 sukcesy i 8 997 odrzuceń, a
ostatnia odpowiedź przyszła po 0,6 s. W trybie domyślnym każde z 10 000 poleceń zakończyło się
sukcesem, ale ostatnie czekało 3,8 s.

Wybierz `Serial` dla agregatów, do których wielu klientów pisze jednocześnie, dla handlerów z ciężką
pracą, która nie może wykonać się dwa razy, i wtedy, gdy szybkie „zajęte, spróbuj ponownie” pasuje
twoim klientom bardziej niż długie czekanie.

## Własne wiersze w tej samej transakcji

Czasem polecenie musi zostawić po sobie coś oprócz zdarzeń: bilet do wysłania e-mailem, wiadomość dla
innego systemu, zadanie do wykonania później. Projekcja ([rozdział 8](08-projections.md)) zapisuje
takie rzeczy po zapisaniu zdarzeń, więc może się chwilę spóźnić. **Writer outboxu** zapisuje je w tej
samej transakcji, która zapisuje zdarzenia, więc zapisane zostaje jedno i drugie albo nic. Kontekst
dołącza `WithOutboxWriter` i podaje writer. Koncerty z E6 mają taki writer
(`class ConcertContext extends AggregateContext[Concert] with WithOutboxWriter[Concert]`); zapisuje on
wiersz z biletem dla każdego zarezerwowanego miejsca:

<!-- snippet: e6/Concerts.scala#outboxWriter -->
```scala
// A ticket to email for each booking, in the transaction of its event: both are stored, or neither.
override def outboxWriter: OutboxWriter[Concert] = new OutboxWriter[Concert] {
  override def write(concertId: AggregateId, version: AggregateVersion, events: Seq[EventInfo[Concert]])(implicit session: DBSession): Unit =
    events.foreach(_.event match {
      case SeatBooked(customer) =>
        sql"INSERT INTO e6_tickets (concert_id, version, customer) VALUES (${concertId.asLong}, ${version.asInt}, $customer)".update()
      case _ => ()
    })
}
```

Na co writer może liczyć:

- Dostaje identyfikator agregatu, jego wersję po zdarzeniach, zdarzenia z ich wersjami, użytkownikami
  i czasami (`EventInfo`) oraz sesję transakcji. Zapisuj przez ScalikeJDBC w tej sesji, nigdy we
  własnej transakcji.
- Jeśli rzuci wyjątek, jego wiersze i zdarzenia zostaną wycofane razem, a wywołujący dostanie
  `EventHandlingError`.
- Polecenie, które nie zapisuje zdarzeń, go nie wywołuje; nie wywołuje go też kopia polecenia
  idempotentnego, które zostało już zastosowane, więc każdy wiersz jest zapisywany raz.
- Wiersze są twoje: odczytuje je i usuwa twój własny kod, na przykład zadanie, które wysyła e-maile.
  Gdy przepisanie historii wymazuje dane ([rozdział 15](15-rewriting-history.md)), metoda `rewrite`
  writera zmienia także jego wiersze.

> **Wskazówka:** E6 (`examples/src/main/scala/io/reactivecqrs/examples/e6/`) łączy narzędzia z tego
> rozdziału w jednym programie: koncerty, na które miejsca rezerwuje się po jednym (`Serial`),
> planowane z kluczem wybranym przez klienta (polecenie idempotentne), a każda rezerwacja zapisuje
> swój wiersz z biletem. Uruchomisz go przez
> `sbt "examples/runMain io.reactivecqrs.examples.e6.CommandsSetup"`.

## Poprawianie przeszłości

Czasem zmienić trzeba samą przeszłość: klient prosi o usunięcie swojego nazwiska z każdego koszyka,
jaki kiedykolwiek utworzył. Do tego służą dwa kolejne rodzaje poleceń: `RewriteHistoryCommand` (z
oczekiwaną wersją) i `RewriteHistoryConcurrentCommand` (bez niej). W `eventsTypes` wymieniają klasy
zdarzeń, które przepisują; ich handlery mieszkają w `rewriteHistoryCommandHandlers` kontekstu,
dostają zapisane zdarzenia tych klas i odpowiadają `RewriteCommandSuccess`: przepisanymi zdarzeniami
oraz nowymi zdarzeniami, które odnotowują zmianę. `RewriteCartName` z koszyka zmienia nazwę koszyka
w jego zdarzeniu `ShoppingCartCreated` i dodaje `CartNameRewritten`. To ostre narzędzie, a
[rozdział 15](15-rewriting-history.md) pokazuje, jak się z nim bezpiecznie obchodzić.

## Co już umiesz

- `FirstCommand` tworzy agregat. Identyfikator wybiera szyna, chyba że `chosenAggregateId` wskazuje
  identyfikator zarezerwowany wcześniej przez `ReserveAggregateId`; wtedy ponowne wysłanie polecenia
  tworzącego jest nieszkodliwe.
- `Command` niesie `expectedVersion`; gdy agregat poszedł już dalej, nic nie zostaje zapisane, a ty
  dostajesz `AggregateConcurrentModificationError`.
- `ConcurrentCommand` działa na bieżącej wersji i po kolizji jest uruchamiane ponownie, więc jego
  handler nie może mieć efektów ubocznych.
- `IdempotentCommand` z kluczem sprawia, że każda kopia polecenia dostaje odpowiedź pierwszej kopii;
  klucze są globalne, a odpowiedzi są domyślnie przechowywane przez 30 dni.
- Handlery odpowiadają `CommandSuccess` (jedno zdarzenie, kilka, żadne albo z dodatkową informacją),
  `CommandFailure` lub `AsyncCommandResult`, a każde polecenie kończy się dokładnie jedną odpowiedzią.
- Wolne handlery napotykają 60-sekundowy limit czasu polecenia i 32-wątkową pulę handlerów; długiej
  pracy miejsce jest poza poleceniem.
- `CommandProcessing.Serial` wykonuje polecenia agregatu po jednym i odpowiada `CommandRejected`, gdy
  jego kolejka jest pełna.
- Writer outboxu (`WithOutboxWriter`) zapisuje twoje własne wiersze w transakcji zdarzeń.

Dalej: [rozdział 6](06-events.md) przygląda się z bliska temu, co zostawiają po sobie polecenia:
zdarzeniom.
