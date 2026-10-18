# Rozdział 6. Zdarzenia: jedyne źródło prawdy

Polecenia przychodzą i odchodzą, zdarzenia zostają. Każdy koszyk, jaki kiedykolwiek pokażesz, każdy raport
i każda projekcja powstają ze zdarzeń zapisanych w PostgreSQL, więc warto je dobrze poznać. Pod koniec tego
rozdziału będziesz wiedzieć, co dostają twoje handlery zdarzeń i jakich zasad muszą się trzymać, jakie
szczególne rodzaje zdarzeń rozumie biblioteka (pierwsze, cofnięcia, duplikacji i trwałego usunięcia), jak
usunąć agregat delikatnie albo na dobre, jak zapisać, kto działał i dlaczego, jak zmienić zapisaną już klasę
zdarzenia, niczego nie psując, i jakie limity dbają o zdrowie agregatów.

Przykłady korzystają dalej z przygotowania z rozdziału 5: `commandBus` to szyna poleceń koszyków, a `alice`
i `bob` to `UserId(1)` i `UserId(2)`. Bloki, które coś wysyłają, działają w `EventsSpec` (obok
`CommandKindsSpec` z rozdziału 5), a ten spec sprawdza, co każdy z nich dostaje w odpowiedzi.

---

## Handlery zdarzeń z bliska

Twój koszyk z rozdziału 3 ma handlery dla trzech zdarzeń. Koszyk z domeny testowej, jego większy brat,
obsługuje ich kilka więcej. Oto pełna lista z `ShoppingCartAggregateContext`:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#eventHandlers -->
```scala
override def eventHandlers = (userId, timestamp, shoppingCart) => {
  case e: ShoppingCartDuplicated => shoppingCart // the state that arrives is already the copy
  case e: ShoppingCartCreated => shoppingCartCreated(e)
  case e: ItemAdded => itemAdded(shoppingCart, e)
  case e: ItemRemoved => itemRemoved(shoppingCart, e)
  case e: ShoppingCartDeleted => null
  case e: CartNameRewritten => cartNameRewritten(shoppingCart, e) // shoppingCart.copy(name = e.name)
  case e: CartSquashed => ShoppingCart(e.name, e.items)
}
```

Ten rozdział wyjaśnia usuwanie, cofanie i kopiowanie; przepisywanie należy do rozdziału 15, a zdarzenie
squasha do rozdziału 14. Najpierw sam handler. Dostaje trzy rzeczy i oddaje jedną:

- `userId`: kto wysłał polecenie, które zapisało zdarzenie (`userId` tego polecenia);
- `timestamp`: kiedy zdarzenie zostało zapisane. Wszystkie zdarzenia jednego polecenia mają tę samą chwilę,
  przyciętą do mikrosekund, i przy każdym odtworzeniu zdarzenia odczytuje się ona dokładnie tak samo;
- stan *przed* zdarzeniem;
- a zwraca stan *po* zdarzeniu: nową wartość zbudowaną za pomocą `copy`, nigdy zmienioną starą.

Koszyk nie potrzebuje dwóch pierwszych, ale wiele agregatów tak. Wyobraź sobie wypożyczenie z biblioteki:
książkę trzeba oddać dwa tygodnie po wypożyczeniu, a wypożyczającym jest ten, kto ją wypożyczył:

<!-- snippet: cart/Loans.scala#loan -->
```scala
import java.time.Instant
import java.time.temporal.ChronoUnit
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{SpaceId, UserId}

case class Loan(title: String, borrower: UserId, due: Instant, returned: Boolean)

case class LoanStarted(title: String) extends FirstEvent[Loan] {
  def spaceId: SpaceId = SpaceId(0)
}
case class LoanExtended(days: Int) extends Event[Loan]
case class BookReturned() extends Event[Loan]
```

a w kontekście wypożyczenia handlery zdarzeń:

<!-- snippet: cart/Loans.scala#loanHandlers -->
```scala
override def eventHandlers = (userId, timestamp, loan) => {
  case e: LoanStarted  => Loan(e.title, borrower = userId, due = timestamp.plus(14, ChronoUnit.DAYS), returned = false)
  case e: LoanExtended => loan.copy(due = loan.due.plus(e.days.toLong, ChronoUnit.DAYS))
  case _: BookReturned => loan.copy(returned = true)
}
```

Zwróć uwagę, czego tu *nie ma*: `Instant.now()`. Gdyby handler pytał zegar, termin zwrotu wychodziłby inny
za każdym razem, gdy wypożyczenie jest czytane z magazynu. Parametr `timestamp` to chwila, w której
wypożyczenie naprawdę się zaczęło, i dziś, i za pięć lat.

### Gdzie działają twoje handlery i dlaczego nigdy nie mogą zawieść

Ten sam kod handlera działa w większej liczbie miejsc, niż można by się spodziewać:

1. **Zanim zdarzenia zostaną zapisane**: raz na każde nowe zdarzenie, na kopii stanu. Jeśli handler rzuci
   wyjątek, polecenie dostaje `EventHandlingError` i nic nie zostaje zapisane.
2. **Za każdym razem, gdy agregat jest czytany z magazynu**: gdy startuje jego właściciel (przy pierwszym
   użyciu, po restarcie, po uśpieniu z powodu bezczynności) i przy każdym spojrzeniu w przeszłość
   (rozdział 7).
3. **Gdy projekcja jest przebudowywana** z zapisanych zdarzeń (rozdział 10).

Dlatego twoje handlery zdarzeń muszą:

- **obsługiwać każde zdarzenie, jakie agregat kiedykolwiek zapisał**, także zdarzenia starych klas, dopóki
  takie zdarzenia istnieją;
- **nigdy nie rzucać wyjątków.** Zdarzenie to fakt: już się wydarzyło. Sprawdzenia i odmowy należą do
  handlera polecenia, który wciąż może powiedzieć „nie”;
- **być czyste**: to samo zdarzenie na tym samym stanie zawsze daje ten sam wynik. Żadnego zegara, żadnych
  liczb losowych, żadnych wywołań bazy danych ani usług;
- **zwracać wartości niezmienne** (case class, `Vector`, `Map`). Stan jest przekazywany handlerom poleceń
  i subskrybentom na innych wątkach.

Oto najbardziej kuszący błąd:

```scala
case e: ItemAdded =>
  // Don't! Checks belong in the command handler. If the limit changes one day, the carts stored
  // before the change still have to replay.
  if (shoppingCart.items.size > 5) throw new IllegalStateException("Cart full")
  itemAdded(shoppingCart, e)
```

> **Ostrzeżenie:** handler, który na zapisanym zdarzeniu rzuca wyjątek albo nie ma dla niego przypadku,
> sprawia, że tego agregatu nie da się odczytać. Jego właściciel odpowiada na polecenia
> `CommandHandlingError`, a na zapytania `Failure(AggregateRestoreFailedException)`, dopóki kod nie zostanie
> poprawiony; przy kolejnym żądaniu próbuje od nowa. Jeden zapomniany `case` może zablokować każdy koszyk,
> w którym jest to zdarzenie.

## FirstEvent: każdy pamiętnik ma pierwszą stronę

Pierwszym zdarzeniem nowego agregatu musi być `FirstEvent`. Poza tym, że jest pierwszą stroną pamiętnika,
mówi, do której *przestrzeni* należy agregat, czyli w jakiej organizacji (najemcy, firmie klienta) żyje.
Koszyk żyje w samej platformie, w przestrzeni 0:

<!-- snippet: shoppingcart/Events.scala#created -->
```scala
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}
```

W aplikacji z wieloma organizacjami przestrzeń zwykle przychodzi z polecenia, a podać ją może wprost pole
case class (to przykład, w domenie testowej go nie ma):

<!-- snippet: cart/EventsSpec.scala#cartOpened -->
```scala
case class CartOpened(spaceId: SpaceId, name: String) extends FirstEvent[ShoppingCart]
```

Jeśli to pominiesz, możliwe są dwa kłopoty:

- Pierwsze zdarzenie z ujemną przestrzenią (taką jak `SpaceId.unknown`, czyli -1) zostaje odrzucone:
  polecenie dostaje `FailureResponse` z informacją, że nowy agregat potrzebuje przestrzeni, i nic nie zostaje
  zapisane. Gdy agregat należy do platformy, a nie do organizacji, użyj `SpaceId(0)`.
- Zwykłe `Event` jako pierwsze zdarzenie nowego agregatu kończy się przy zapisie błędem
  `EventHandlingError`: magazyn nie wie, do której przestrzeni należy agregat.

Przestrzenie mają cały własny rozdział: [rozdział 16](16-organizations.md).

## Delikatne usuwanie

Koszyk można usunąć. Jego handler polecenia zapisuje `ShoppingCartDeleted()`, a handler zdarzenia zwraca
`null`:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#deletedHandler -->
```scala
case e: ShoppingCartDeleted => null
```

Stan `null` oznacza „usunięty”. Od tej chwili:

- `GetAggregate(cartId)` odpowiada `Aggregate`, którego `aggregateRoot` to `None`;
- kolejne polecenia dla koszyka są odrzucane z błędem `CommandHandlingError` (w logu pojawia się
  informacja, że agregat „is already deleted”);
- nic nie zostaje wymazane. Cały pamiętnik nadal istnieje, projekcje widzą `ShoppingCartDeleted` i usuwają
  koszyk ze swoich list, a odczyty z rozdziału 7 nadal pokazują koszyk takim, jaki był wcześniej.

To *miękkie* usunięcie i zwykle właśnie ono jest dobrym wyborem: historia zostaje, a nikt nie może już
zmienić koszyka.

## Cofnięcie: Ctrl+Z dla agregatu

Oto mały dramat z zestawu testów. Alicja tworzy koszyk „Zakupy” i dodaje jabłka; Bob dodaje pomarańcze,
a potem usuwa pozycję 1, którą niestety są jabłka:

```
version 1  ShoppingCartCreated(Groceries)   Alice
version 2  ItemAdded(apples)                Alice
version 3  ItemAdded(oranges)               Bob
version 4  ItemRemoved(1)                   Bob, by mistake
```

Alicja naciska w koszyku przycisk cofania. Koszyk ma do tego polecenie,

<!-- snippet: shoppingcart/Commands.scala#undo -->
```scala
case class UndoShoppingCartChange(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                                  stepsToUndo: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]
```

zdarzenie cofnięcia,

<!-- snippet: shoppingcart/Events.scala#undone -->
```scala
case class ShoppingCartChangesUndone(eventsCount: Int) extends UndoEvent[ShoppingCart]
```

i przypadek w handlerach poleceń, który zamienia jedno w drugie:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#undoHandler -->
```scala
case c: UndoShoppingCartChange => CommandSuccess(ShoppingCartChangesUndone(c.stepsToUndo))
```

`UndoEvent` ma pole `eventsCount`: liczbę najnowszych zdarzeń, które anuluje.

<!-- snippet: cart/EventsSpec.scala#undo -->
```scala
val undone = commandBus ? UndoShoppingCartChange(alice, cartId, AggregateVersion(4), stepsToUndo = 1)
// SuccessResponse(cartId, AggregateVersion(5)); the cart holds apples and oranges again
```

Co warto wiedzieć o cofaniu:

- **Cofnięcie też jest zdarzeniem.** Wersja idzie naprzód, do 5, nigdy wstecz. Usunięcie zostaje
  w pamiętniku, oznaczone jako anulowane od wersji 5, więc odczyt wersji 4 (rozdział 7) nadal pokazuje
  koszyk bez jabłek, bo taki wtedy był.
- **Anuluje najnowsze zdarzenia, które wciąż obowiązują.** Kolejne cofnięcie sięgnęłoby dalej wstecz
  i anulowałoby wersję 3, czyli pomarańcze. Cofnięcie nigdy nie anuluje innego cofnięcia i nie ma ponawiania
  (redo).
- **Twoje handlery zdarzeń nie potrzebują przypadku dla zdarzenia cofnięcia.** Cofaniem zajmuje się
  biblioteka: nigdy nie przekazuje zdarzenia cofnięcia twoim handlerom, a po zatwierdzeniu sama ustala
  prawdziwy stan, czytając agregat z powrotem z magazynu i pomijając anulowane zdarzenia. (Przed wersją 1.0
  brak takiego przypadku kończył polecenie błędem `EventHandlingError`, problem F148; przypadek, który
  został z tamtych czasów, nie szkodzi, po prostu nigdy nie jest wywoływany.)
- **Cofnięcie nie może sięgnąć poniżej punktu squasha** (rozdział 14); takie polecenie dostaje
  `EventHandlingError`.
- **Projekcje widzą zdarzenie cofnięcia jak każde inne.** Listenery, które budują swoje dane ze zdarzeń,
  muszą same cofnąć swoją pracę; listenery, które dostają stan agregatu, dostają razem z nim poprawiony stan
  (rozdział 8).

> **Pod maską:** anulowane zdarzenia są zapisywane w tabeli `noop_events`, ze skutkiem od wersji cofnięcia,
> a migawka agregatu jest wyrzucana. Zobacz [ARCHITECTURE.md, sekcje 2.6 i 4.2](../ARCHITECTURE.md).

## Duplikacja: nowy agregat, który zaczyna życie jako kopia

„Zapisz jako...” w edytorze tekstu: lista zakupów z zeszłego tygodnia staje się początkiem listy na ten
tydzień. Koszyk też to potrafi, dzięki poleceniu

<!-- snippet: shoppingcart/Commands.scala#duplicate -->
```scala
case class DuplicateShoppingCart(userId: UserId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]
```

i zdarzeniu duplikacji:

<!-- snippet: shoppingcart/Events.scala#duplicated -->
```scala
case class ShoppingCartDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends DuplicationEvent[ShoppingCart]
```

Handler polecenia koszyka zapisuje to zdarzenie,

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#duplicateHandler -->
```scala
case c: DuplicateShoppingCart =>
  CommandSuccess(ShoppingCartDuplicated(SpaceId(0), c.baseAggregateId, c.baseAggregateVersion))
```

a handler zdarzenia oddaje bez zmian stan, który dostaje:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#duplicatedHandler -->
```scala
case e: ShoppingCartDuplicated => shoppingCart // the state that arrives is already the copy
```

Wracając do naszej historii: Alicja kopiuje koszyk „Zakupy” taki, jaki był w wersji 3:

<!-- snippet: cart/EventsSpec.scala#duplicate -->
```scala
val copied = commandBus ? DuplicateShoppingCart(alice, cartId, AggregateVersion(3))
// SuccessResponse(copyId, AggregateVersion(1)): a new cart with apples and oranges
```

Kopia jest osobnym koszykiem, w wersji 1, i żyje własnym życiem: dodaj do niej pomidory, a staną się jej
pozycją 3, podczas gdy koszyk „Zakupy” nigdy ich nie zobaczy.

Co warto wiedzieć o duplikacji:

- **`DuplicationEvent` jest `FirstEvent`**, więc podaje przestrzeń kopii, która może być inna niż przestrzeń
  agregatu bazowego.
- **Handler zdarzenia zwraca stan bez zmian.** Gdy biblioteka czyta kopię, najpierw stosuje zdarzenia
  agregatu bazowego aż do skopiowanej wersji, więc stan, który dostaje twój handler, już jest kopią.
- **Kopia dzieli z agregatem bazowym historię do tej wersji, a nie kopiuje jej.** Cokolwiek stanie się
  potem z agregatem bazowym (nowe zdarzenia, cofnięcia), kopii nie dotyczy. Dotyczy jej za to przepisanie
  historii agregatu bazowego poniżej tej wersji, bo tę historię mają wspólną (rozdział 15); a trwałe
  usunięcie agregatu bazowego najpierw daje kopii jej własne pierwsze zdarzenie (następna sekcja).
- **Kopia nie może zacząć się poniżej punktu squasha agregatu bazowego** (rozdział 14) **ani powyżej jego
  wersji**: kopia „w wersji 10” koszyka, który jest w wersji 5, zostaje odrzucona z błędem
  `EventHandlingError`. Wersję bierz z odpowiedzi na `GetAggregate`.

## Trwałe usunięcie: gdy dane naprawdę muszą zniknąć

Miękkie usunięcie zachowuje wszystko i zwykle właśnie tego chcesz. Czasem jednak dane muszą naprawdę
zniknąć. Zajmuje się tym `PermanentDeleteEvent`, w dwóch krokach:

1. Zdarzenie zostaje zapisane i dostarczone jak każde inne, a polecenie od razu dostaje odpowiedź
   `SuccessResponse`.
2. Gdy każdy subskrybent potwierdzi to zdarzenie (a więc każda projekcja będzie już mieć okazję usunąć
   własną kopię danych), biblioteka usuwa agregat na dobre: jego zdarzenia i jego wiersz, czekające wiersze
   outboxu, migawkę, zarchiwizowane zdarzenia (rozdział 14) i zapisy postępu projekcji dotyczące tego
   agregatu.

Koszyk w domenie testowej nie ma trwałego usunięcia. Własny kontekst doda je w kilku linijkach; spec tego
rozdziału uruchamia taki:

<!-- snippet: cart/EventsSpec.scala#erase -->
```scala
case class ShoppingCartErased() extends PermanentDeleteEvent[ShoppingCart]

case class EraseShoppingCart(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[ShoppingCart, CustomCommandResponse[_]]

class ErasableCarts extends ShoppingCartAggregateContext {
  override def commandHandlers = cart => {
    case _: EraseShoppingCart => CommandSuccess(ShoppingCartErased())
    case other => super.commandHandlers(cart)(other)
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case _: ShoppingCartErased => cart
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }
}
```

Potem koszyk wygląda przy odczycie tak, jakby nigdy nie istniał: `GetAggregate` odpowiada wersją 0 bez
stanu, a `GetEventsForAggregate` (rozdział 7) pustą listą.

Co warto wiedzieć o trwałym usunięciu:

- **Czeka, dopóki któraś projekcja ma dla tego agregatu zapisaną awarię** (rozdział 9). Gdy awaria zostanie
  usunięta, następne dostarczenie zdarzenia przeprowadzi usunięcie.
- **Kopie są pod opieką.** Jeśli z tego agregatu powstały duplikaty, każdy z nich najpierw dostaje własne
  pierwsze zdarzenie, utworzone ze zdarzenia squasha danego typu (rozdział 14), więc kopie zachowują swój
  stan. Typ bez polityki squasha tego nie potrafi: usunięcie wtedy czeka, a log mówi dlaczego. Koszyk ma
  politykę squasha.
- **Tabele twoich projekcji są twoje.** Biblioteka ich nie dotyka: usuń ich wiersze, gdy twoje projekcje
  zobaczą to zdarzenie. Zapisane odpowiedzi idempotentnych poleceń agregatu znikają razem z nim; odpowiedzi
  zapisane przez starsze wersje biblioteki, które nie wskazują swojego agregatu, wygasają wraz z końcem
  okresu przechowywania (domyślnie 30 dni).

> **Wskazówka:** jeśli potrzebujesz obu rodzajów usuwania, zaplanuj to z góry. Polecenie nie może działać na
> agregacie, którego stan to `null`, więc koszyka usuniętego miękko tak, jak robi to nasz koszyk, nie da się
> już wymazać poleceniem. Jeśli kiedyś może zajść potrzeba wymazania miękko usuniętych agregatów, trzymaj
> miękkie usunięcie jako flagę w stanie (`deleted = true`), a inne polecenia odrzucaj samodzielnie w swoich
> handlerach poleceń.

## Kto to zrobił i dlaczego: koperta metadanych

Paczka i jej etykieta: paczka to to, co wysłano, a etykieta mówi, kto ją nadał, dla kogo i pod jakim numerem
zamówienia. Zdarzenia też mają etykietę.

Każde zdarzenie jest zapisywane z `userId` polecenia. Często potrzeba więcej: asystent (człowiek, agent AI,
integracja) działał w imieniu Boba; żądanie przyszło przez publiczne API; całość dotyczy zgłoszenia nr 12
w pomocy technicznej. Polecenie z domieszką `WithMetadata` daje każdemu zapisanemu przez siebie zdarzeniu
kopertę `EventMetadata`:

<!-- snippet: shoppingcart/Commands.scala#onBehalf -->
```scala
/** Adds an item for someone else: its event carries a metadata envelope. */
case class AddItemOnBehalf(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String,
                           metadata: EventMetadata)
  extends Command[ShoppingCart, CustomCommandResponse[_]] with WithMetadata
```

| Pole `EventMetadata` | Co zawiera |
|---|---|
| `actor` | kto działał, w formacie, jaki lubi twoja aplikacja: `"user:5"`, `"agent:planner"` |
| `correlationId` | łączy wszystko, co zrobiono w ramach jednego żądania albo zgłoszenia |
| `causationId` | co to spowodowało: komunikat, inne zdarzenie |
| `traceContext` | kontekst śledzenia W3C, do śledzenia rozproszonego |
| `spaceId` | organizacja |
| `custom` | twoja własna `Map[String, String]` |

Wymagany jest tylko `actor`:

<!-- snippet: cart/EventsSpec.scala#onBehalf -->
```scala
val onBehalf = EventMetadata("agent:planner", correlationId = Some("case 12"), custom = Map("channel" -> "api"))

val added = commandBus ? AddItemOnBehalf(bob, cartId, AggregateVersion(1), "apple", onBehalf)
// SuccessResponse(cartId, AggregateVersion(2)); the ItemAdded event carries the envelope
```

Gdzie pojawia się ta etykieta?

- **Nie w handlerach zdarzeń.** Te dalej dostają swoje `(userId, timestamp, state)`, więc stan nigdy nie
  zależy od etykiety.
- **W projekcjach**, przy każdym zdarzeniu, jako `EventInfo.metadata`, niezależnie od tego, czy zdarzenie
  przychodzi na bieżąco, czy jest dostarczane ponownie później (rozdział 8).
- **W odczytach historii** z rozdziału 7.

Zdarzenie zapisane bez koperty (przez polecenie bez `WithMetadata`) odczytuje się jako
`EventMetadata.ofUser(userId)`, czyli kopertę, której aktorem jest `"user:"`, a po nim identyfikator
użytkownika.

## Zmiana zdarzenia, które jest już zapisane

Oprawiony pamiętnik: strony 12 nie wydrukujesz na nowo, ale możesz na nowej stronie zapisać, że od teraz
pewne rzeczy notuje się inaczej.

Zdarzenia są zapisywane jako JSON pod pełną nazwą swojej klasy. Zmień klasę (zmień nazwę pola, dodaj albo
usuń pole, zmień typ, zmień nazwę klasy albo przenieś ją do innego pakietu), a zapisany JSON przestanie do
niej pasować. Zależnie od zmiany starych zdarzeń nie da się odczytać, a razem z nimi każdego agregatu, który
ma choć jedno z nich; albo nowe pole po cichu wróci puste (zero, `null` albo `None`, nigdy wartość domyślna
wpisana w klasie).

Zasada jest więc krótka: **nigdy nie zmieniaj kształtu zapisanej klasy zdarzenia.** Dodaj nową klasę,
a starą zostaw. Powiedzmy, że pewnego dnia pozycje koszyka dostają ilość:

```scala
@deprecated("Written before items had quantities; use ItemAddedV2", "2.0")
case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemAddedV2(name: String, quantity: Int) extends Event[ShoppingCart]
```

(Koszyk z domeny testowej nie ma ilości; tak by je dostał.) Potem:

1. Handlery poleceń od teraz zapisują tylko `ItemAddedV2`.
2. Handlery zdarzeń obsługują oba, dopóki istnieją zapisane zdarzenia `ItemAdded`:

   ```scala
   case e: ItemAdded   => addItem(shoppingCart, e.name, quantity = 1)
   case e: ItemAddedV2 => addItem(shoppingCart, e.name, e.quantity)
   ```

3. Projekcje też obsługują oba (rozdział 8).

> **Ostrzeżenie:** gdy klaster jest aktualizowany po jednym węźle, węzeł, który wciąż działa na starej
> wersji, nie odczyta nowej klasy. Nie zapisuj jej, dopóki nie potrafi tego każdy węzeł: właśnie do tego
> służy `writtenFrom` kontekstu ([rozdział 17](17-cluster.md)).

### Sprawdź przed aktualizacją

`StoredDataCheck` odczytuje każdą zapisaną wartość JSON za pomocą klas z wersji, którą zamierzasz wdrożyć,
i dla każdej tabeli i klasy raportuje, ile wierszy odczytał i których nie dało się odczytać. Uruchom go ze
swojej aplikacji (która ma wszystkie twoje klasy), podłączonej do kopii bazy produkcyjnej:

<!-- snippet: cart/EventsSpec.scala#check -->
```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val report = StoredDataCheck.run(mpjsons, typesNames)
println(report.text)
if (report.unreadable > 0) println("Not yet: some stored data cannot be read by this version")
```

Czyta zdarzenia, zarchiwizowane zdarzenia, zlecenia sag i zapisane odpowiedzi na polecenia, a do tego
wszystkie tabele projekcji, które wymienisz (`StoredDataCheck.ProjectionTable[YourDocument]("name")`).
Niczego nie zmienia, tylko czyta. Tabeli, której twoja baza nie ma, na przykład tabeli sag w aplikacji bez
sag, nie czyta, a raport ją wymienia. Raport liczy też agregaty bez przestrzeni oraz zdarzenia według ich
starej wersji typu (zobacz [ARCHITECTURE.md, sekcja 2.3](../ARCHITECTURE.md)).

### Kiedy można pozbyć się starej klasy?

<!-- snippet: cart/EventsSpec.scala#usersOf -->
```scala
val users = StoredDataCheck.usersOf(classOf[ItemAdded].getName)
println(s"${users.events} stored and ${users.archived} archived ItemAdded events")
users.aggregates.foreach(u => println(s"aggregate ${u.aggregateId}: ${u.events} stored, ${u.archived} archived"))
```

Gdy oba liczniki wynoszą zero, nic już tej klasy nie używa i może ona zniknąć z twojego kodu. `usersOf` czyta
obie tabele w całości, bo nie ma indeksu po klasie, więc uruchamiaj go na kopii albo wtedy, gdy baza ma mało
pracy. Stare zdarzenia nie znikają same; jednym ze sposobów, by odeszły, jest squash (rozdział 14).

## Limity, które dbają o zdrowie agregatów

Dwa limity chronią przed agregatami, które wymknęły się spod kontroli. Oba są parametrami konstruktora
`PostgresEventStoreState`, więc dotyczą każdego typu agregatu, który korzysta z tego magazynu:

| Parametr | Domyślnie | Po przekroczeniu |
|---|---|---|
| `aggregateVersionLimit` | 10 000 zdarzeń na agregat | Polecenie dostaje `EventHandlingError`; w logu pojawia się `TooManyEventsException`. |
| `eventSizeLimit` | 100 000 znaków JSON na zdarzenie | Polecenie dostaje `EventHandlingError`; w logu pojawia się `EventTooLargeException`. |
| `eventSizeLimitPerClassName` | brak | Limit dla klasy zdarzenia zamiast `eventSizeLimit`, wyższy albo niższy. |

Zdarzenia jednego polecenia są zapisywane razem albo wcale, więc przez jedno zdarzenie za dużo albo jedno
zbyt duże nie udaje się całe polecenie. Limity ustawia się tak:

<!-- snippet: cart/EventsSpec.scala#limits -->
```scala
val eventStore = new PostgresEventStoreState(mpjsons, typesNames,
  aggregateVersionLimit = 20000,                                       // carts with very long lives
  eventSizeLimitPerClassName = Map(classOf[ItemAdded].getName -> 1000)  // an item name never needs more
).initSchema()
```

Zanim je podniesiesz, zastanów się, po co w ogóle są. Gdy startuje właściciel agregatu, czytane jest każde
zdarzenie tego agregatu, chyba że ma on migawkę: w pomiarze z 17/10/2026 odczyt 50 000 zdarzeń trwał 392 ms,
a odczyt z migawki 12 ms (jedno uruchomienie na jednej maszynie deweloperskiej,
[`docs/perf/load-M5.md`](../perf/load-M5.md)). Duże zdarzenia kosztują proporcjonalnie więcej pamięci
i miejsca na dysku.

> **Wskazówka:** gdy agregat zbliża się do limitu, zastanów się, czy podzielić go wzdłuż naturalnej granicy
> (jeden na miesiąc, jeden na wersję dokumentu), włączyć migawki ([rozdział 13](13-snapshots.md)), czy
> zwinąć starą historię squashem ([rozdział 14](14-squash.md)). Przy dużych dokumentach trzymaj plik gdzie
> indziej, a w zdarzeniu umieszczaj tylko odwołanie do niego ([`docs/SCALING.md`](../SCALING.md),
> sekcja 7).

## Co już umiesz

- Handlery zdarzeń dostają użytkownika, czas zapisania zdarzenia i stan sprzed niego, a zwracają nowy stan;
  są czyste, obsługują każde zapisane zdarzenie i nigdy nie rzucają wyjątków.
- Nowy agregat zaczyna się od `FirstEvent`, które podaje jego przestrzeń; `SpaceId(0)` to platforma.
- Zwrócenie `null` delikatnie usuwa agregat i zachowuje jego historię; `PermanentDeleteEvent` usuwa go na
  dobre, gdy każdy subskrybent zobaczy to zdarzenie.
- `UndoEvent` anuluje najnowsze zdarzenia, które wciąż obowiązują, i nie potrzebuje przypadku w twoich
  handlerach; `DuplicationEvent` zaczyna nowy agregat jako kopię innego w danej wersji, a jego przypadek
  zwraca stan, który dostaje, bo to już jest kopia.
- Polecenie z domieszką `WithMetadata` opatruje swoje zdarzenia kopertą `EventMetadata`, którą widzą
  projekcje i odczyty historii.
- Zapisana klasa zdarzenia nigdy nie zmienia kształtu: dodaj klasę `V2`, obsłuż obie i przed aktualizacją
  sprawdź zapisane dane przez `StoredDataCheck`.
- Agregat mieści najwyżej 10 000 zdarzeń, a każde z nich ma najwyżej 100 000 znaków, chyba że ustawisz inne
  limity magazynu zdarzeń.

Dalej: [rozdział 7](07-reading-the-past.md) otwiera pamiętnik i czyta przeszłość.
