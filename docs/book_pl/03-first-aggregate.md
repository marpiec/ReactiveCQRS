# Rozdział 3. Twój pierwszy agregat: koszyk zakupów

Czas napisać kod, który dotyczy *twojego* biznesu, a nie hydrauliki. Pod koniec tego rozdziału będziesz
mieć kompletny agregat koszyka zakupów: stan, który koszyk pamięta, zdarzenia, które go zmieniają,
polecenia, które ludzie do niego wysyłają, zasady, które decydują, i kontekst, który spina to wszystko
w całość. To sama zwykła Scala: żadnych aktorów, żadnej bazy danych, niczego do uruchamiania. Możesz go
nawet wypróbować prostym foldem i na końcu tak właśnie zrobimy. Potem rozdział 4 tchnie w niego życie.

---

## Plan na serwetce

Koszyk ma nazwę i listę produktów. Można utworzyć koszyk, dodać do niego produkt i usunąć z niego
produkt. Jedna zasada domowa: **nie więcej niż pięć produktów w koszyku.**

Zanim napiszesz jakikolwiek kod, wypisz, o co ludzie mogą prosić i co się dzieje, gdy koszyk się zgadza:

| Ktoś prosi (polecenie) | Gdy koszyk się zgodzi, oto co zaszło (zdarzenie) |
|---|---|
| `CreateShoppingCart("Groceries")` | `ShoppingCartCreated("Groceries")` |
| `AddItem(..., "apples")` | `ItemAdded("apples")` |
| `RemoveItem(..., 1)` | `ItemRemoved(1)` |

Zwróć uwagę na gramatykę. Polecenia to rozkazy w trybie rozkazującym („create”, „add”), zdarzenia to
fakty w czasie przeszłym („created”, „added”). Ten drobny nawyk pomoże ci zachować jasność myślenia
przez resztę książki: polecenie wciąż można odrzucić, zdarzenie już się stało.

Koszyk z tego rozdziału to E1, pierwszy przykład książki, w
`examples/src/main/scala/io/reactivecqrs/examples/e1/Carts.scala`, kompilowany i testowany razem
z biblioteką. W swoim projekcie umieść go we własnym pakiecie, na przykład `com.example.shop.cart`,
w plikach zaproponowanych w [rozdziale 2](02-setting-up.md). Koszyk z domeny testowej, w
`testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`, potrafi więcej (cofanie, kopie,
usuwanie), a te dodatki poznasz w [rozdziale 6](06-events.md).

## Krok 1: stan

Co koszyk musi pamiętać? Swoją nazwę i swoje produkty. To jest **stan**, nazywany też **korzeniem
agregatu**: zwykła case class.

<!-- snippet: e1/Carts.scala#state -->
```scala
case class ShoppingCart(name: String, items: Vector[Item])

case class Item(id: Int, name: String)
```

Celowo brakuje tu dwóch rzeczy: id koszyka i jego wersji. Biblioteka trzyma je obok twojego stanu,
nigdy w nim, i podaje ci całą trójkę razem, gdy poprosisz o koszyk (jako
`Aggregate(id, version, aggregateRoot)`, w rozdziale 4).

**Stan ma być niezmienny.** Case classy, `Vector`, `List`, `Map`: tak. `var` albo kolekcja mutowalna:
nigdy. Biblioteka przekazuje tę samą wartość stanu kilku wątkom naraz (twoje handlery poleceń działają
w `Future`, a szyna zdarzeń przekazuje stan projekcjom), a tylko wartości niezmienne przechodzą przez to
bez niespodzianek.

> **Ostrzeżenie:** pakiet i nazwę klasy stanu wybieraj starannie. Jej pełna nazwa,
> `com.example.shop.cart.ShoppingCart`, *jest* nazwą typu agregatu w bazie danych. Jeśli później
> zmienisz nazwę klasy albo ją przeniesiesz, biblioteka zobaczy zupełnie nowy typ agregatu, bez żadnych
> agregatów, a stare będą leżeć w bazie pod starą nazwą.

## Krok 2: zdarzenia

Każda linijka w pamiętniku koszyka to zdarzenie: mała case class, która rozszerza `Event`, z klasą stanu
jako parametrem typu. `Event` i wszystkie pozostałe klasy biblioteki, których używa ten rozdział,
pochodzą z dwóch pakietów API, importowanych na początku pliku:

<!-- snippet: e1/Carts.scala#imports -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
```

Trzy zdarzenia koszyka:

<!-- snippet: e1/Carts.scala#events -->
```scala
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]
```

Po parametrze typu, `Event[ShoppingCart]`, biblioteka poznaje, do którego typu agregatu należy
zdarzenie.

`ShoppingCartCreated` jest wyjątkowe: to zdarzenie, które *zaczyna* koszyk, więc rozszerza `FirstEvent`.
Pierwsze zdarzenie ma jeden obowiązek: musi powiedzieć, do której **przestrzeni** należy nowy agregat.
Przestrzeń to organizacja, jeden z najemców twojej aplikacji; `SpaceId(0)` to sama platforma. Dopóki
twoja aplikacja nie ma własnych organizacji, każde pierwsze zdarzenie podaje `SpaceId(0)`, tak jak
w domenie testowej. Przestrzenie dostaną własny rozdział ([rozdział 16](16-organizations.md)).

> **Pamiętaj:** każde pierwsze zdarzenie musi podać przestrzeń. Nowy agregat, którego pierwsze zdarzenie
> jej nie podaje (`SpaceId.unknown` albo dowolna inna liczba ujemna), zostaje odrzucony: polecenie
> dostaje w odpowiedzi `FailureResponse` z informacją, że nowy agregat potrzebuje przestrzeni, i nic nie
> zostaje zapisane.

Kilka zasad sprawia, że ze zdarzeniami żyje się przyjemnie:

- **Zawsze czas przeszły.** `ItemAdded`, a nie `AddItem`.
- **Fakty, nie instrukcje, i tylko fakty.** `ItemAdded("apples")` nie niesie id nowego produktu: stan
  sam potrafi je wyliczyć (krok 5 pokazuje jak). Zdarzenia mają być małe.
- **Zwykłe dane.** Napisy, liczby, wartości `Option`, kolekcje i inne małe case classy. Zdarzenia są
  zapisywane jako JSON przez bibliotekę mpjsons, która czyta twoje klasy po ich polach.
- **Nazwy są na zawsze.** Zdarzenie jest zapisywane pod nazwą swojej klasy i pod tą nazwą stare
  zdarzenia są odczytywane nawet po latach. Nigdy nie zmieniaj nazwy klasy zdarzenia, które już zapisano,
  nie przenoś jej i nie zmieniaj jej kształtu. Gdy zdarzenie musi się zmienić, dodaj obok nową klasę
  (`ItemAddedV2`) i zachowaj starą; [rozdział 6](06-events.md) pokazuje jak.

> **Pod maską:** oto, co biblioteka zapisuje dla drugiego zdarzenia koszyka: wiersz w tabeli `events`
> z id koszyka, wersją 2, użytkownikiem, czasem, numerem klasy `com.example.shop.cart.ItemAdded` (ze
> słownika `types_names` z rozdziału 2) i JSON-em `{"name":"apples"}`. Szczegóły są w
> [ARCHITECTURE.md, w sekcji 2](../ARCHITECTURE.md).

## Krok 3: polecenia

Polecenia to też case classy. Każde rozszerza jedną z klas poleceń biblioteki, a ta decyduje, jak zostanie
obsłużone:

<!-- snippet: e1/Carts.scala#commands -->
```scala
case class CreateShoppingCart(userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]

case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]

case class RemoveItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                      id: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]
```

- **`FirstCommand`** tworzy nowy agregat. Nie ma `aggregateId`: koszyk jeszcze nie istnieje, a id nadaje
  mu biblioteka.
- **`Command`** zmienia istniejący agregat. Wskazuje agregat (`aggregateId`) i wersję, którą widzisz
  w chwili, gdy postanawiasz je wysłać (`expectedVersion`). To blokowanie optymistyczne
  z [rozdziału 1](01-cqrs-without-the-headache.md): jeśli koszyk od tamtej pory poszedł dalej, polecenie
  zostaje odrzucone i nic się nie zmienia.
- Każde polecenie niesie **`userId`**: kto prosi. Biblioteka zapisuje go przy każdym zdarzeniu, które
  polecenie wytworzy.

Pierwszy parametr typu to agregat, dla którego jest polecenie. Drugi to rodzaj odpowiedzi, jaką dostaje
wywołujący; `CustomCommandResponse[_]` znaczy „dowolna z odpowiedzi biblioteki” i tego używa każde
polecenie w domenie testowej. Używaj go i ty.

> **Wskazówka:** nazwy tych pól nie zależą od ciebie. `userId`, `aggregateId` i `expectedVersion` to
> składowe, które deklarują klasy bazowe, a pola twojej case class je wypełniają. Pisz je dokładnie tak;
> pozostałe pola (`name`, `id`) możesz nazwać, jak chcesz.

> **Pamiętaj:** w domenie testowej `CreateShoppingCart` ma z przodu jeszcze jedno pole,
> `idempotencyId: Option[SagaStep]`, i dodatkowo domieszkuje `IdempotentCommand`. Dzięki temu polecenie
> można bezpiecznie wysłać dwa razy i dlatego testy biblioteki piszą
> `CreateShoppingCart(None, userId, "Groceries")`. Tutaj je pomijamy, żeby pierwszy koszyk był prosty;
> polecenia idempotentne opisuje [rozdział 5](05-command-flavors.md), razem z innymi rodzajami poleceń.

## Krok 4: zasady (handlery poleceń)

**Handler polecenia** patrzy na bieżący stan i na polecenie, po czym decyduje. W ReactiveCQRS handlery
poleceń agregatu to jedna funkcja: dajesz jej bieżący stan, a ona oddaje ci funkcję częściową z poleceń
w wyniki. Mieszka w klasie kontekstu z kroku 6; tutaj pokazujemy ją osobno:

<!-- snippet: e1/Carts.scala#rules -->
```scala
override def commandHandlers = cart => {
  case c: CreateShoppingCart =>
    CommandSuccess(ShoppingCartCreated(c.name))
  case c: AddItem =>
    if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
    else CommandSuccess(ItemAdded(c.name))
  case c: RemoveItem =>
    if (cart.items.exists(_.id == c.id)) CommandSuccess(ItemRemoved(c.id))
    else CommandFailure("There is no item " + c.id + " in the cart")
}
```

Czytaj to jak listę domowych zasad. `cart` to koszyk taki, jaki jest w tej chwili; dla `FirstCommand`,
które nie ma jeszcze koszyka, to pusty stan początkowy z kroku 6. Każdy `case` mówi, co zrobić z jednym
rodzajem polecenia. Każdy przypadek kończy się **wynikiem**:

| Zwracasz | Znaczy to | Wywołujący dostaje (rozdział 4) |
|---|---|---|
| `CommandSuccess(event)` albo `CommandSuccess(Seq(event1, event2))` | Tak: zapisz te zdarzenia. | `SuccessResponse` |
| `CommandSuccess(event, info)` | Tak, a oto coś dla wywołującego. | `CustomSuccessResponse` z `info` |
| `CommandFailure("reason")` albo `CommandFailure(List("reason", "another"))` | Nie, i oto dlaczego. Nic nie zostaje zapisane. | `FailureResponse` |
| `AsyncCommandResult(future)` | Najpierw muszę na coś poczekać; odpowiedzią będzie później jedno z powyższych. | to, co przyniesie future |

Nigdy nie sprawdzasz `expectedVersion` samodzielnie. Biblioteka porównuje ją z wersją koszyka, *zanim*
uruchomi twój handler, a polecenie z nieaktualną wersją nigdy do niego nie dociera.

Czasem zasada musi na coś poczekać: na przykład na cennik albo na usługę stanów magazynowych. Wtedy
zwróć `AsyncCommandResult` opakowujący `Future` z wynikiem. Oto wariant `AddItem` z wymyśloną, twoją
własną usługą magazynową, której `isAvailable` odpowiada `Future[Boolean]` (i z twoim
`ExecutionContext` w zasięgu):

<!-- snippet: e1/TryingItOutSpec.scala#asyncAddItem -->
```scala
case c: AddItem =>
  AsyncCommandResult(stock.isAvailable(c.name).map { available =>
    if (available) CommandSuccess(ItemAdded(c.name))
    else CommandFailure(c.name + " is out of stock")
  })
```

Wewnątrz kontekstu agregatu możesz nawet zwrócić bezpośrednio `Future`: kontekst sam zamieni `Future`
z wynikiem w `AsyncCommandResult`. Dokładnie tak robi `createShoppingCart` z domeny testowej. Więcej
o powolnych handlerach mówi [rozdział 5](05-command-flavors.md).

A co, jeśli do polecenia nie pasuje żaden `case`? Wywołujący dostaje `CommandHandlingError`, a szczegóły
trafiają do logu. Zatem: jeden `case` na każde polecenie agregatu.

> **Ostrzeżenie:** nie wysyłaj e-maili, nie obciążaj kart płatniczych i nie wywołuj innych systemów
> z handlera polecenia. Handler może zostać uruchomiony więcej niż raz dla jednego polecenia (polecenie
> współbieżne po konflikcie jest uruchamiane ponownie, [rozdział 5](05-command-flavors.md)), a jego wynik
> może zostać wyrzucony (w międzyczasie inne polecenie zmieniło koszyk albo handler działał dłużej niż
> limit czasu polecenia). Efekty uboczne należą do etapu *po* zapisaniu zdarzeń: do projekcji i sag,
> rozdziały 8 i 12.

> **Wskazówka:** handlery poleceń działają na własnej puli wątków, `reactivecqrs.handler-dispatcher`
> (rozdział 2), więc powolny handler nigdy nie zatrzyma szyny zdarzeń, projekcji ani sag. Przez cały ten
> czas zajmuje jednak jeden z 32 wątków tej puli, a gdy zajęte są wszystkie, czeka każde polecenie.
> Długie oczekiwanie przenieś na własne wątki za pomocą `AsyncCommandResult`. Co dzieje się w przeciwnym
> razie, pokazuje sekcja 8 w [docs/SCALING.md](../SCALING.md).

## Krok 5: jak zdarzenia zmieniają stan (handlery zdarzeń)

Handler polecenia podjął decyzję; zdarzenie to ta decyzja zapisana czarno na białym. Teraz ktoś musi
zastosować to zdarzenie do stanu. To zadanie **handlerów zdarzeń**, znów jednej funkcji:

<!-- snippet: e1/Carts.scala#handlers -->
```scala
override def eventHandlers = (userId, timestamp, cart) => {
  case e: ShoppingCartCreated =>
    ShoppingCart(e.name, Vector.empty)
  case e: ItemAdded =>
    val nextId = cart.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
    cart.copy(items = cart.items :+ Item(nextId, e.name))
  case e: ItemRemoved =>
    cart.copy(items = cart.items.filterNot(_.id == e.id))
}
```

Dostaje trzy rzeczy: `userId`, czyli użytkownika, którego polecenie zapisało zdarzenie; `timestamp`,
czyli chwilę zapisu (`java.time.Instant`); oraz `cart`, czyli stan przed zdarzeniem. Zwraca stan po
zdarzeniu. Nic więcej handler zdarzenia nigdy nie robi.

Spójrz na `ItemAdded`: id nowego produktu wynika ze stanu (o jeden więcej niż największe dotychczasowe
id). Ponieważ przy każdym odtwarzaniu tego samego pamiętnika handler dostaje ten sam stan, zawsze wylicza
te same identyfikatory. Właśnie takiego myślenia potrzebują handlery zdarzeń, bo uruchamiają się wciąż od
nowa:

1. **gdy polecenie się powiedzie**, żeby wyliczyć nowy stan, zanim cokolwiek zostanie zapisane;
2. **za każdym razem, gdy koszyk jest wczytywany z bazy danych**: biblioteka przepuszcza przez te
   handlery każde zdarzenie koszyka (pamiętnik z rozdziału 1, czytany od góry);
3. **gdy projekcja jest przebudowywana** z zapisanych zdarzeń ([rozdział 10](10-rebuilding-projections.md)).

Stąd trzy złote zasady handlerów zdarzeń:

- **Nigdy nie odmawiaj.** Zdarzenie już się stało; na „nie” był czas w handlerze polecenia. Bez względu
  na stan zwróć nowy stan.
- **Żadnych efektów ubocznych.** Handler, który wysyła e-mail, wyśle go przy każdym odtworzeniu.
- **To samo wejście, to samo wyjście.** Żadnego `Instant.now()`, żadnych liczb losowych, żadnego czytania
  z bazy. Potrzebujesz czasu? Użyj `timestamp`, który przy każdym odtworzeniu jest taki sam.

Handler zdarzenia, który rzuca wyjątek, to zły dzień. Podczas wykonywania polecenia sprawia, że polecenie
kończy się błędem `EventHandlingError` i nic nie zostaje zapisane. Podczas odtwarzania jest gorzej:
koszyka w ogóle nie da się wczytać, dopóki nie naprawisz handlera.

> **Pod maską:** zwrócenie `null` z handlera zdarzenia oznacza agregat jako usunięty (tak robi
> `ShoppingCartDeleted` z domeny testowej), a zdarzenia cofnięcia i kopii wymagają w handlerach
> szczególnej uwagi. Oba tematy należą do [rozdziału 6](06-events.md). Jak biblioteka uruchamia handlery,
> opisuje [sekcja 2.6 w ARCHITECTURE.md](../ARCHITECTURE.md).

## Krok 6: kontekst, który spina całość

Ostatni element to **kontekst agregatu**: jedna klasa na typ agregatu, która przekazuje bibliotece
wszystko z kroków od 1 do 5. Oto cały koszyk:

<!-- snippet: e1/Carts.scala#context -->
```scala
class ShoppingCartContext extends AggregateContext[ShoppingCart] {

  override val version: Int = 1

  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector.empty)

  override def commandHandlers = cart => {
    case c: CreateShoppingCart =>
      CommandSuccess(ShoppingCartCreated(c.name))
    case c: AddItem =>
      if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
      else CommandSuccess(ItemAdded(c.name))
    case c: RemoveItem =>
      if (cart.items.exists(_.id == c.id)) CommandSuccess(ItemRemoved(c.id))
      else CommandFailure("There is no item " + c.id + " in the cart")
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case e: ShoppingCartCreated =>
      ShoppingCart(e.name, Vector.empty)
    case e: ItemAdded =>
      val nextId = cart.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
      cart.copy(items = cart.items :+ Item(nextId, e.name))
    case e: ItemRemoved =>
      cart.copy(items = cart.items.filterNot(_.id == e.id))
  }
}
```

Cztery składowe, cztery zadania:

- **`initialAggregateRoot`** to czysta kartka przed pierwszym zdarzeniem: stan, który widzi handler
  `FirstCommand`, i stan, od którego wychodzi handler pierwszego zdarzenia. Wystarczy pusty koszyk bez
  nazwy.
- **`version`** to wersja *zachowania* tego kontekstu, a nie żadnego koszyka. Zacznij od 1. Podnieś ją,
  gdy zmienisz to, co handlery zdarzeń robią ze zdarzeniami: zapisane migawki starego zachowania są wtedy
  ignorowane ([rozdział 13](13-snapshots.md)), a projekcje zarządzane, które czytają stany tego typu,
  zostają przebudowane ([rozdział 10](10-rebuilding-projections.md)).
- **`commandHandlers`** i **`eventHandlers`** to kroki 4 i 5.

Domena testowa trzyma swoje handlery w dwóch obiektach ze zwykłymi funkcjami, `CommandsHandlers`
i `EventsHandlers`, i wywołuje je z kontekstu; w małym agregacie pisanie ich bezpośrednio w kontekście,
jak powyżej, jest równie dobre.

Wszystko inne w `AggregateContext` ma rozsądną wartość domyślną, a kolejne rozdziały włączają to, gdy
będzie potrzebne:

| Składowa | Domyślnie | Gdzie ją spotkasz |
|---|---|---|
| `commandTimeout` | 60 sekund | [rozdział 5](05-command-flavors.md) |
| `commandProcessing` | `CommandProcessing.Concurrent` (po jednym naraz to `Serial`) | [rozdział 5](05-command-flavors.md) |
| `snapshots` | `None` | [rozdział 13](13-snapshots.md) |
| `squash` | `None` | [rozdział 14](14-squash.md) |
| `rewriteHistoryCommandHandlers` | każde polecenie przepisania historii kończy się błędem | [rozdział 15](15-rewriting-history.md) |
| `placement` | `Placement.Local` | [rozdział 17](17-cluster.md) |
| `writtenFrom` | puste | [rozdział 17](17-cluster.md) |

> **Pamiętaj:** domyślnie jeden agregat mieści najwyżej 10 000 zdarzeń (rozdział 2). Niech agregaty będą
> małe i skupione na jednej rzeczy, jak pojedynczy koszyk, a nie „wszystkie koszyki klienta”. Rozdziały
> 13 i 14 pokazują, co robić, gdy historia mimo to się wydłuża.

## Wypróbuj to foldem

A teraz najlepsze: handlery to zwykłe funkcje, więc możesz je wypróbować bez uruchamiania czegokolwiek.
Odtworzenie pamiętnika to fold po zdarzeniach:

<!-- snippet: e1/TryingItOutSpec.scala#fold -->
```scala
import java.time.Instant
import io.reactivecqrs.api.id.UserId

val context = new ShoppingCartContext
val alice = UserId(1)
val now = Instant.now()

val diary = List(ShoppingCartCreated("Groceries"), ItemAdded("apples"), ItemAdded("oranges"), ItemRemoved(1))

val cart = diary.foldLeft(context.initialAggregateRoot) { (state, event) =>
  context.eventHandlers(alice, now, state)(event)
}
// cart: ShoppingCart(Groceries,Vector(Item(2,oranges)))
```

Ten fold to w miniaturze to, co biblioteka robi za każdym razem, gdy wczytuje koszyk z bazy danych.
A prośba o decyzję to jedno wywołanie funkcji:

<!-- snippet: e1/TryingItOutSpec.scala#decision -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId

val decision = context.commandHandlers(cart)(AddItem(alice, AggregateId(1001), AggregateVersion(4), "pears"))
// decision: CommandSuccess(List(ItemAdded(pears)),NoInfo())
```

`NoInfo()` na końcu to „brak dodatkowych informacji” zwykłego `CommandSuccess`, mała case class samej
biblioteki, `io.reactivecqrs.api.NoInfo`. Na zwykły sukces wywołujący dostaje odpowiedź
`SuccessResponse`; polecenie, które odpowiada czymś więcej (rozdział 5), umieszcza w tym miejscu swoją
informację.

Zauważ, że wersja w poleceniu nie miała tu znaczenia: jej sprawdzanie to zadanie biblioteki, nie
handlera, a bezpośrednie wywołanie handlera omija to sprawdzenie. [Rozdział 19](19-testing.md) zamienia
te jednolinijkowce w porządne testy.

## Cztery praktyczne zasady

Wszystko w tym rozdziale sprowadza się do czterech nawyków:

1. **Zdarzenia to fakty.** W czasie przeszłym, niezmienne, małe, a raz zapisane nigdy nie zmieniają nazwy.
2. **Handlery poleceń decydują.** Patrzą na stan, mówią „tak” zdarzeniami albo „nie” z podaniem powodu
   i trzymają ręce z dala od świata zewnętrznego.
3. **Handlery zdarzeń słuchają.** Nigdy nie odmawiają, nigdy nie mają efektów ubocznych i dla tych samych
   zdarzeń za każdym razem dają ten sam stan.
4. **Stan powstaje z odtworzenia zdarzeń.** Pamiętnik jest prawdą; stan to jego obrazek, który w każdej
   chwili można narysować od nowa.

## Co już umiesz

- Agregat to cztery rodzaje kodu: **stan** (niezmienna case class), **zdarzenia**, **polecenia**
  i **kontekst**.
- `FirstEvent` zaczyna agregat i musi wskazać jego **przestrzeń**; `SpaceId(0)` to platforma.
- `FirstCommand` tworzy agregat; `Command` go zmienia i niesie `expectedVersion` na potrzeby blokowania
  optymistycznego.
- **Handlery poleceń** zwracają `CommandSuccess` ze zdarzeniami, `CommandFailure` z powodami albo
  `AsyncCommandResult`, gdy muszą poczekać.
- **Handlery zdarzeń** przyjmują `(userId, timestamp, state)` i zdarzenie, a zwracają nowy stan; muszą
  być czyste, bo działają przy każdym odtworzeniu.
- **`AggregateContext`** spina to wszystko za pomocą `initialAggregateRoot`, `version`,
  `commandHandlers` i `eventHandlers`.
- Nazwy klas stanu i zdarzeń są zapisywane w bazie danych, więc wybieraj je starannie.

Dalej: [rozdział 4](04-first-command.md) podłącza koszyk do działającego systemu i wysyła mu pierwsze
polecenie.
