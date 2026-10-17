# Rozdział 6. Wydarzenia: Twoje jedyne źródło prawdy

Polecenia przychodzą i odchodzą, zdarzenia pozostają. Każdy wózek, który kiedykolwiek pokażesz, każdy raport i każda projekcja
jest tworzony na podstawie zdarzeń przechowywanych w PostgreSQL, dlatego warto je dobrze poznać. Do końca tego
w tym rozdziale dowiesz się, co otrzymują Twoi obsługujący zdarzenia i jakich zasad muszą przestrzegać,
rodzaje zdarzeń, które biblioteka rozumie (pierwsze, cofnięcie, duplikacja i trwałe usunięcie), jak
jak usunąć agregację delikatnie lub na stałe, jak zarejestrować, kto i dlaczego działał, jak zmienić zdarzenie
klasa po jej zapisaniu bez uszkodzenia czegokolwiek oraz ograniczenia, które utrzymują agregaty w zdrowiu.

Przykłady są kontynuowane w oparciu o konfigurację z rozdziału 5: `commandBus` to magistrala poleceń wózków, a
`alicja` i `bob` to `UserId(1)` i `UserId(2)`.

---

## Obsługujące zdarzenia z bliska

W rozdziale 3 napisałeś procedury obsługi zdarzeń dla trzech zdarzeń. Koszyk domeny testowej, większy brat domeny
Twój, obsługuje kilka kolejnych. Oto pełna lista z `ShoppingCartAggregateContext`:

```scala
nadpisz def eventHandlers = (userId, timestamp, shoppingCart) => {
  przypadek e: Duplikat koszyka => koszyk
  przypadek e: Utworzono koszyk => utworzono koszyk(e)
  przypadek e: Dodano przedmiot => dodano przedmiot (koszyk, e)
  przypadek e: PrzedmiotUsunięty => przedmiotUsunięty(Koszyk, e)
  przypadek e: Koszyk usunięty => null
  przypadek e: ShoppingCartChangesUndone => shoppingCart
  przypadek e: CartNameRewritten => cartNameRewritten(shoppingCart, e)
  przypadek e: CartSquashed => ShoppingCart(e.name, e.items)
}
```

W tym rozdziale wyjaśniono usuwanie, cofanie i kopiowanie; przepisywanie jest rozdziałem 15, a
Rozdział 14 wydarzenia squasha. Po pierwsze, sam obsługujący. Otrzymuje trzy rzeczy i zwraca jedną:

- `userId`: kto wysłał polecenie, które zapisało zdarzenie (`userId` polecenia);
- `timestamp`: kiedy zdarzenie zostało zapisane. Wszystkie zdarzenia jednego polecenia współdzielą jeden moment, skrócony do
  mikrosekundach i odczytuje dokładnie tę samą wartość za każdym razem, gdy zdarzenie jest odtwarzane;
- stan *przed* zdarzeniem;
- i zwraca stan *po* zdarzeniu: nową wartość, utworzoną za pomocą `copy`, nigdy niezmienioną.

Wózek nie potrzebuje pierwszych dwóch, ale wiele agregatów tak. Wyobraź sobie wypożyczenie biblioteczne, gdzie
książkę należy zwrócić po dwóch tygodniach od momentu wypożyczenia, a wypożyczającym jest osoba, która ją wypożyczyła:

```scala
importuj java.time.Instant
import java.time.temporal.ChronoUnit
importuj io.reactivecqrs.api._
import io.reactivecqrs.api.id.{SpaceId, UserId}

klasa przypadku Pożyczka(tytuł: String, pożyczkobiorca: UserId, należność: Natychmiast, zwrócona: Wartość logiczna)

klasa przypadku LoanStarted(tytuł: String) rozszerza FirstEvent[Pożyczka] {
  def spaceId: SpaceId = SpaceId(0)
}
klasa przypadku LoanExtended(dni: Int) rozszerza Event[Pożyczka]
klasa przypadku BookReturned() rozszerza Event[Pożyczka]

// w AggregateContext pożyczki
nadpisz def eventHandlers = (userId, timestamp, loan) => {
  przypadek e: LoanStarted => Loan(e.title, loaner = userId, due = timestamp.plus(14, ChronoUnit.DAYS), returned = false)
  przypadek e: LoanExtended => loan.copy(due = loan.due.plus(e.days.toLong, ChronoUnit.DAYS))
  przypadek _: BookReturned => pożyczka.kopia(zwrócona = prawda)
}
```

Zauważ, czego *nie* tam jest: `Instant.now()`. Gdyby obsługujący zapytał zegar, nadszedłby termin
za każdym razem, gdy pożyczka jest odczytywana z magazynu, wartość jest inna. Parametr `timestamp` to
moment, w którym pożyczka naprawdę się rozpoczęła, dziś i za pięć lat.

### Gdzie biegną Twoi opiekunowie i dlaczego nigdy nie mogą zawieść

Ten sam kod obsługi działa w większej liczbie miejsc, niż można by się spodziewać:

1. **Przed zapisaniem zdarzeń**, raz dla każdego nowego zdarzenia, na kopii stanu. Jeśli program obsługi zgłosi wyjątek,
   polecenie otrzymuje `EventHandlingError` i nic nie zostaje zapisane.
2. **Za każdym razem, gdy agregat jest odczytywany ze sklepu**: gdy jego właściciel rozpoczyna działanie (przy pierwszym użyciu, po
   ponownego uruchomienia, po uśpieniu z powodu bezczynności) i za każdym spojrzeniem w przeszłość (rozdział 7).
3. **Gdy projekcja jest budowana na nowo** z zapisanych zdarzeń (rozdział 10).

Dlatego Twoje procedury obsługi zdarzeń muszą:

- **obsługuje każde zdarzenie, które kiedykolwiek zostało zapisane**, wliczając stare klasy, tak długo, jak takie zdarzenia będą przechowywane
  istnieć;
- **nigdy nie rzucaj.** Wydarzenie jest faktem; już się wydarzyło. Sprawdzenia i odmowy należą do
  program obsługi poleceń, który nadal może powiedzieć „nie”;
- **bądź czysty**: to samo zdarzenie w tym samym stanie zawsze daje ten sam wynik. Bez zegara, bez losowości.
  numery, brak baz danych i połączeń serwisowych;
- **zwraca niezmienne wartości** (klasy przypadków, `Wektor`, `Mapa`). Stan jest przekazywany do polecenia
  osoby obsługujące wątki i subskrybenci w innych wątkach.

Oto najbardziej kuszący błąd:

```scala
przypadek e: Dodano element =>
  // Nie! Kontrole powinny być przeprowadzane w programie obsługi poleceń. Jeśli limit zmieni się pewnego dnia, przechowywane koszyki
  // przed zmianą trzeba będzie jeszcze odtworzyć.
  jeśli (shoppingCart.items.size > 5) rzuć nowy IllegalStateException("Koszyk pełny")
  itemAdded(koszyk, e)
```

> **Ostrzeżenie:** obiekt obsługujący zdarzenie, który je zgłasza lub nie ma dla niego przypadku, powoduje, że zbiorcze zdarzenie jest
> nieczytelny. Jego właściciel odpowiada na polecenia kodem `CommandHandlingError`, a na zapytania kodem
> `Failure(AggregateRestoreFailedException)` do momentu naprawienia kodu; następnie spróbuje ponownie z późniejszym
> prośba. Jeden zapomniany `przypadek` może zablokować każdy wózek, który zawiera to zdarzenie.

## FirstEvent: każdy pamiętnik ma pierwszą stronę

Pierwszym zdarzeniem nowego agregatu musi być „FirstEvent”. Poza tym, że jest to pierwsza strona dziennika,
mówi, do której *przestrzeni* należy agregat: organizacji (najemcy, firmy klienta)
mieszka w. Wózek mieszka w samej platformie, przestrzeń 0:

```scala
klasa przypadku ShoppingCartCreated(nazwa: String) rozszerza FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}
```

W aplikacji z wieloma organizacjami spacja zwykle pochodzi z polecenia i przypadku
pole klasy może zapewnić to bezpośrednio (przykład, nie w domenie testowej):

```scala
klasa przypadku CartOpened(spaceId: SpaceId, nazwa: String) rozszerza FirstEvent[ShoppingCart]
```

Jeśli to pominiesz, możesz popełnić dwa błędy:

- Pierwsze zdarzenie z ujemną spacją (takie jak `SpaceId.unknown`, które ma wartość -1) jest odrzucane:
  polecenie otrzymuje `FailureResponse` informujący, że nowy agregat wymaga spacji i nic nie jest przechowywane.
  Użyj `SpaceId(0)`, jeśli agregat należy do platformy, a nie do organizacji.
- Zwykłe `Wydarzenie` jako pierwsze zdarzenie nowego agregatu kończy się niepowodzeniem podczas przechowywania,
  `EventHandlingError`: sklep nie wie, gdzie należy umieścić agregat.

Przestrzenie mają swój własny rozdział: [rozdział 16](16-organizations.md).

## Usuwanie, delikatny sposób

Koszyk można usunąć. Jego program obsługi poleceń przechowuje `ShoppingCartDeleted()`, a program obsługi zdarzeń
zwraca `null`:

```scala
przypadek e: Koszyk usunięty => null
```

Stan „null” oznacza „usunięty”. Od tego momentu:

- `GetAggregate(cartId)` odpowiada wartością `Aggregate`, której `aggregateRoot` ma wartość `None`;
- dalsze polecenia dla wózka są odrzucane z błędem `CommandHandlingError` (w dzienniku jest napisane, że agregat
  "jest już usunięty");
- nic nie jest usuwane. Cały dziennik nadal tam jest, projekcje widzą „ShoppingCartDeleted” i usuwają
  wózek z ich list, a odczyty rozdziału 7 nadal pokazują wózek w takim stanie, w jakim był wcześniej.

To jest *miękkie* usunięcie i w większości przypadków jest to właściwy wybór: zachowujesz historię i
nikt już nie może zmienić koszyka.

## Cofnij: Ctrl+Z, aby wykonać agregację

Oto mały dramat z zestawu testowego. Alicja tworzy wózek z zakupami i dodaje jabłka; Bob
dodaje pomarańcze, a następnie usuwa element 1, którym niestety są jabłka:

```
wersja 1 ShoppingCartCreated(Groceries) Alice
wersja 2 Dodano element (jabłka) Alicja
wersja 3 Dodano element (pomarańcze) Bob
wersja 4 Element usunięty(1) Bob, przez pomyłkę
```

Alicja naciska przycisk cofania na wózku. Wózek ma polecenie i zdarzenie dla niego:

```scala
klasa przypadku UndoShoppingCartChange(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                                  stepsToUndo: Int) rozszerza Command[ShoppingCart, CustomCommandResponse[_]]

klasa przypadku ShoppingCartChangesUndone(eventsCount: Int) rozszerza UndoEvent[ShoppingCart]

// w commandHandlers
przypadek c: UndoShoppingCartChange => CommandSuccess(ShoppingCartChangesUndone(c.stepsToUndo))
```

Zdarzenie `UndoEvent` ma parametr `eventsCount`, który informuje, ile najnowszych zdarzeń zostało anulowanych.

```scala
commandBus ? UndoShoppingCartChange(alice, cartId, AggregateVersion(4), stepsToUndo = 1)
// SuccessResponse(cartId, AggregateVersion(5)); w koszyku znów są jabłka i pomarańcze
```

Co warto wiedzieć o funkcji cofania:

- **Cofnięcie to też zdarzenie.** Wersja przesuwa się do przodu o 5, nigdy do tyłu. Usunięcie pozostaje w
  dziennik, oznaczony jako anulowany od wersji 5, więc czytanie wersji 4 (rozdział 7) nadal pokazuje
  wózek bez jabłek, bo tak to wtedy było.
- **Anuluje najnowsze wydarzenia, które nadal obowiązują.** Cofnij jeszcze raz, a przejdziesz dalej
  Powrót: anuluje wersję 3, pomarańcze. Cofnięcia nigdy się nie anulują i nie ma możliwości powtórzenia.
- **Twój moduł obsługi zdarzeń nadal potrzebuje przypadku dla zdarzenia cofania**, zwracając stan bez zmian:

  ```scala
  przypadek e: ShoppingCartChangesUndone => shoppingCart // biblioteka wykonuje cofanie zmian
  ```

  Biblioteka przepuszcza każde nowe zdarzenie przez Twoje moduły obsługi przed jego zapisaniem, w tym zdarzenia cofania,
  a brakujący przypadek spowoduje błąd polecenia z błędem `EventHandlingError`. Po zatwierdzeniu działa
  sam stan rzeczywisty, poprzez odczytanie agregatu z magazynu i pominięcie anulowanych
  wydarzenia.
- **Cofnięcie nie może osiągnąć wartości poniżej punktu zgniecenia** (rozdział 14); takie polecenie otrzymuje
  `EventHandlingError`.
- **Projekcje widzą zdarzenie cofania jak każde inne.** Słuchacze, którzy tworzą zdarzenia na podstawie zdarzeń, muszą cofnąć zdarzenie
  ich własna praca; słuchacze, którzy odbierają stan agregatu, otrzymują wraz z nim poprawiony stan
  (rozdział 8).

> **Pod maską:** anulowane zdarzenia są rejestrowane w tabeli `noop_events` i obowiązują od
> wersja cofania, a migawka agregatu zostaje odrzucona. Zobacz
> [ARCHITECTURE.md, sekcje 2.6 i 4.2](../ARCHITECTURE.md).

## Duplikacja: nowy agregat, który zaczyna się jako kopia

„Zapisz jako...” w edytorze tekstu: lista zakupów z poprzedniego tygodnia staje się początkiem listy zakupów z bieżącego tygodnia.
Wózek może to zrobić:

```scala
klasa przypadku DuplicateShoppingCart(userId: UserId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  rozszerza FirstCommand[ShoppingCart, CustomCommandResponse[_]]

klasa przypadku ShoppingCartDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  rozszerza DuplicationEvent[ShoppingCart]

// w commandHandlers
przypadek c: DuplikatKoszyka Zakupowego =>
  CommandSuccess(ShoppingCartDuplicated(SpaceId(0), c.baseAggregateId, c.baseAggregateVersion))

// w eventHandlers: stan, który przychodzi, jest już kopią, więc oddaj go bez zmian
przypadek e: Duplikat koszyka => koszyk
```

Kontynuując opowieść, Alicja kopiuje wózek z zakupami, taki jaki był w wersji 3:

```scala
commandBus ? DuplicateShoppingCart(alicja, identyfikator koszyka, łączna wersja(3))
// SuccessResponse(copyId, AggregateVersion(1)): nowy koszyk z jabłkami i pomarańczami
```

Kopia jest osobnym koszykiem w wersji 1 i żyje własnym życiem: dodaj do niej pomidory, a one...
stają się przedmiotem nr 3, podczas gdy wózek z zakupami nigdy ich nie widzi.

Co warto wiedzieć o duplikacji:

- **Zdarzenie „DuplicationEvent” jest zdarzeniem „FirstEvent”**, dlatego nadaje nazwę przestrzeni kopii, która może się różnić od
  bazy.
- **Obsługa zdarzeń zwraca stan bez zmian.** Gdy biblioteka odczytuje kopię, najpierw stosuje
  zdarzenia bazy aż do skopiowanej wersji, więc stan, który otrzymuje Twój program obsługi, jest już kopią.
- **Kopia udostępnia historię bazy aż do danej wersji, nie kopiuje jej.** Cokolwiek się stanie
  do bazy później (nowe zdarzenia, cofnięcia) nie zmienia kopii. Przepisanie historii bazy
  poniżej znajduje się ta wersja, ponieważ ją udostępniają (rozdział 15); i trwałe usunięcie bazy w pierwszej kolejności
  nadaje kopii pierwsze własne wydarzenie (następna sekcja).
- **Kopia nie może zaczynać się poniżej punktu zgniecenia podstawy** (rozdział 14), **ani powyżej wersji podstawy**:
  Kopia „w wersji 10” koszyka w wersji 5 jest odrzucana z błędem `EventHandlingError`. Weź wersję
  z odpowiedzi `GetAggregate`.

## Trwałe usunięcie: kiedy dane naprawdę muszą zniknąć

Miękkie usunięcie zachowuje wszystko, co zazwyczaj jest pożądane. Czasami jednak dane muszą zostać usunięte.
naprawdę zniknąć. `PermanentDeleteEvent` robi to w dwóch krokach:

1. Zdarzenie jest przechowywane i dostarczane jak każde inne, a na polecenie odpowiadamy `SuccessResponse`
   od razu.
2. Gdy każdy subskrybent potwierdził to zdarzenie, każda projekcja miała szansę na usunięcie swojego
   własną kopię danych, biblioteka usuwa agregat na dobre: ​​jego zdarzenia i wiersz,
   wiersze oczekującej skrzynki nadawczej, jej migawka, zarchiwizowane zdarzenia (rozdział 14) i postęp projekcji
   rekordy dla niego.

Koszyk w domenie testowej nie ma możliwości trwałego usunięcia; dodanie go zajmuje kilka linijek (domena testowa)
`PermanentDeleteSpec` robi to samo dla małego agregatu „memo”):

```scala
klasa przypadku ShoppingCartErased() rozszerza PermanentDeleteEvent[ShoppingCart]

klasa przypadku EraseShoppingCart(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  rozszerza polecenie [Koszyk, OdpowiedźNiestandardowegoPolecenia[_]]

// w commandHandlers
przypadek _: EraseShoppingCart => CommandSuccess(ShoppingCartErased())
// w EventHandlerach
przypadek _: KoszykWymazany => Koszyk
```

Potem koszyk wygląda tak, jakby nigdy nie istniał: „GetAggregate” odpowiada wersją 0 i nie
stan i `GetEventsForAggregate` (rozdział 7) z pustą listą.

Co warto wiedzieć o trwałym usunięciu:

- **Czeka, aż projekcja zarejestruje błąd dla agregatu** (rozdział 9). Gdy
  Po usunięciu awarii kolejne dostarczenie zdarzenia spowoduje wykonanie operacji usunięcia.
- **Kopie są pod opieką.** Jeśli z niego zduplikowano inne agregaty, każdy z nich otrzymuje najpierw
  pierwsze własne zdarzenie, utworzone za pomocą zdarzenia squasha danego typu (rozdział 14), dzięki czemu kopie zachowują swoje
  stan. Typ bez polityki squash nie może tego zrobić: usunięcie czeka, a dziennik podaje przyczynę.
  Wózek ma politykę squasha.
- **Twoje tabele projekcji są Twoje.** Biblioteka ich nie dotyka: usuń ich wiersze, gdy
  Twoje projekcje widzą zdarzenie. Zapisane odpowiedzi na idempotentne polecenia agregatu są zgodne z
  odpowiedzi przechowywane w starszych wersjach biblioteki, które nie nadają nazwy swojemu agregatowi, wygasają wraz z
  okres ich przechowywania (domyślnie 30 dni).

> **Wskazówka:** zaplanuj z wyprzedzeniem, jeśli potrzebujesz obu rodzajów usuwania. Polecenie nie może zostać uruchomione na agregacie, którego
> stan jest `null`, więc wózek, który został usunięty w sposób miękki, nie może już zostać usunięty poleceniem. Jeśli
> może być konieczne późniejsze usunięcie agregatów usuniętych miękko, należy zachować opcję usunięcia miękko jako flagę w stanie
> (`deleted = true`) i samodzielnie odrzucaj inne polecenia w swoich procedurach obsługi poleceń.

## Kto to zrobił i dlaczego: koperta metadanych

Przesyłka i jej etykieta wysyłkowa: przesyłka to to, co zostało wysłane, etykieta podaje, kto ją wysłał i dla kogo
i pod jakim numerem zamówienia. Wydarzenia również mają etykietę.

Każde zdarzenie jest przechowywane z identyfikatorem użytkownika polecenia. Często potrzeba czegoś więcej: asystenta (osoby,
Agent AI, integracja) działał w imieniu Boba; żądanie wpłynęło przez publiczne API; należy do
sprawa pomocy technicznej nr 12. Polecenie, które łączy `WithMetadata`, nadaje każdemu zdarzeniu, które przechowuje,
Koperta `EventMetadata`:

```scala
/** Dodaje element dla kogoś innego: jego zdarzenie niesie kopertę metadanych. */
klasa przypadku AddItemOnBehalf(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, nazwa: String,
                           metadane: EventMetadata)
  rozszerza Command[ShoppingCart, CustomCommandResponse[_]] o WithMetadata
```

| Pole `EventMetadata` | Co zawiera |
|---|---|
| `actor` | kto działał, napisane tak, jak chce tego Twoja aplikacja: `"user:5"`, `"agent:planner"` |
| `correlationId` | łączy wszystko, co zostało zrobione dla jednego żądania lub przypadku |
| `causationId` | co to spowodowało: wiadomość, inne zdarzenie |
| `traceContext` | kontekst śledzenia W3C do śledzenia rozproszonego |
| `spaceId` | organizacja |
| `custom` | własna `Map[String, String]` |

Wymagany jest tylko `aktor`:

```scala
val onBehalf = EventMetadata("agent:planner", correlationId = Some("case 12"), custom = Map("channel" -> "api"))

commandBus ? AddItemOnBehalf(bob, cartId, AggregateVersion(1), "apple", onBehalf)
// SuccessResponse(cartId, AggregateVersion(2)); zdarzenie ItemAdded niesie kopertę
```

Gdzie wyświetla się etykieta?

- **Nie w obsłudze zdarzeń.** Zachowują one swoje `(userId, timestamp, state)`, więc stan nigdy nie zależy
  na etykiecie.
- **W projekcjach**, przy każdym zdarzeniu, jako `EventInfo.metadata`, niezależnie od tego, czy zdarzenie jest dostępne na żywo, czy też nie.
  dostarczone ponownie później (rozdział 8).
- **W historii czytamy** rozdział 7.

Zdarzenie zapisane bez koperty (za pomocą polecenia bez `WithMetadata`) odczytuje się jako
`EventMetadata.ofUser(userId)`, koperta, której aktorem jest `"user:"` po którym następuje identyfikator użytkownika.

## Zmiana zdarzenia po jego zapisaniu

Dziennik w oprawie: nie można przedrukować strony 12, ale można napisać na nowej stronie, że od tej pory wszystko będzie się działo.
są napisane inaczej.

Zdarzenia są przechowywane w formacie JSON pod pełną nazwą klasy. Zmień klasę (zmień nazwę pola, dodaj
lub usunąć jeden, zmienić typ, zmienić nazwę klasy lub przenieść ją do innego pakietu) i zapisanego JSON-a nie
już nie pasuje. W zależności od zmiany, stare wydarzenia nie są odczytywane, a wraz z nimi każdy agregat
który zawiera jeden; lub nowe pole po cichu wraca puste (zero, `null` lub `None`, nigdy wartość domyślna
wartość zapisana w klasie).

Zasada jest więc krótka: **nigdy nie zmieniaj kształtu klasy zdarzeń przechowywanych**. Dodaj nową klasę i zachowaj
stary. Powiedzmy, że pewnego dnia elementy koszyka otrzymają określoną ilość:

```scala
@deprecated("Napisano zanim elementy miały ilości; użyj ItemAddedV2", "2.0")
klasa przypadku ItemAdded(nazwa: String) rozszerza Event[ShoppingCart]

klasa przypadku ItemAddedV2 (nazwa: String, ilość: Int) rozszerza Event[ShoppingCart]
```

(Koszyk domeny testowej nie zawiera żadnych ilości; w ten sposób można je uzyskać.) Następnie:

1. Od tej pory procedury obsługi poleceń będą zapisywać tylko `ItemAddedV2`.
2. Obsługujące zdarzenia obsługują obydwa, dopóki istnieją zapisane zdarzenia `ItemAdded`:

   ```scala
   przypadek e: Dodano przedmiot => dodaj przedmiot (koszyk, e.name, ilość = 1)
   przypadek e: ItemAddedV2 => addItem(shoppingCart, e.name, e.quantity)
   ```

3. Projekcje również zajmują się obydwoma zagadnieniami (rozdział 8).

> **Ostrzeżenie:** w klastrze, w którym aktualizowano jeden węzeł na raz, węzeł, który nadal korzysta ze starej wersji, nie może
> przeczytaj nową klasę. Nie zapisuj jej, dopóki każdy węzeł nie będzie mógł: `writtenFrom` kontekstu jest tworzony dla
> dokładnie to ([rozdział 17](17-cluster.md)).

### Sprawdź przed aktualizacją

`StoredDataCheck` odczytuje każdą zapisaną wartość JSON z klasami wersji, którą chcesz odczytać
Wdraża i raportuje, dla każdej tabeli i klasy, ile wierszy odczytał i których nie mógł odczytać. Uruchom go
z Twojej aplikacji (która ma wszystkie Twoje klasy), połączonej z kopią produkcji:

```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val report = StoredDataCheck.run(mpjsons, typesNames)
println(tekst raportu)
if (report.unreadable > 0) println("Jeszcze nie: niektóre zapisane dane nie mogą zostać odczytane przez tę wersję")
```

Odczytuje zdarzenia, zdarzenia zarchiwizowane, rozkazy sagi i zapisane odpowiedzi na polecenia, a także
wszystkie tabele projekcji, które wymienisz (`StoredDataCheck.ProjectionTable[YourDocument]("name")`). To tylko
czyta. Jego raport zlicza również agregaty bez spacji i zdarzenia według ich starej wersji typu (patrz
[ARCHITEKTURA.md, sekcja 2.3](../ARCHITEKTURA.md)).

### Kiedy będzie można opuścić starą klasę?

```scala
val users = StoredDataCheck.usersOf(classOf[ItemAdded].getName)
println(s"${users.events} zapisanych i ${users.archived} zarchiwizowano zdarzeń ItemAdded")
users.aggregates.foreach(u => println(s"aggregate ${u.aggregateId}: ${u.events} zapisane, ${u.archived} zarchiwizowane"))
```

Gdy oba liczniki wynoszą zero, klasa nie jest już używana i można opuścić kod. `usersOf`
odczytuje obie tabele w całości, ponieważ nie ma indeksu według klasy, więc uruchamia się go na kopii lub gdy baza danych jest
Cisza. Stare wydarzenia nie znikają same z siebie; zgniecenie (rozdział 14) to jeden ze sposobów, w jaki odchodzą.

## Limity, które utrzymują agregaty w zdrowiu

Dwa limity chronią przed niekontrolowanymi agregatami. Oba są parametrami konstruktora
`PostgresEventStoreState`, więc mają zastosowanie do każdego typu agregatu, który korzysta z tego magazynu:

| Parametr | Domyślny | Poza nim |
|---|---|---|
| `aggregateVersionLimit` | 10 000 zdarzeń na agregat | Polecenie zwraca `EventHandlingError`; w dzienniku występuje nazwa `TooManyEventsException`. |
| `eventSizeLimit` | 100 000 znaków JSON na zdarzenie | Polecenie zwraca `EventHandlingError`; w dzienniku występuje nazwa `EventTooLargeException`. |
| `eventSizeLimitPerClassName` | brak | Limit na klasę zdarzenia, w miejsce `eventSizeLimit`, wyższy lub niższy. |

Ponieważ zdarzenia jednego polecenia są przechowywane razem lub wcale, jedno zdarzenie jest za duże lub za duże
powoduje niepowodzenie całego polecenia. Ustawienie ich wygląda następująco:

```scala
val eventStore = nowy PostgresEventStoreState(mpjsons, typesNames,
  aggregateVersionLimit = 20000, // wózki o bardzo długiej żywotności
  eventSizeLimitPerClassName = Map(classOf[ItemAdded].getName -> 1000) // nazwa przedmiotu nigdy nie potrzebuje więcej
).initSchema()
```

Zanim je podniesiesz, zastanów się, dlaczego tam są. Każde zdarzenie w agregacie jest odczytywane, gdy jego
właściciel rozpoczyna, chyba że ma migawkę: zmierzono 17.10.2026, odczyt 50 000 zdarzeń zajął 392 ms i
odczyt z migawki 12 ms (pojedyncze uruchomienie na jednej maszynie programistycznej,
[`docs/perf/load-M5.md`](../perf/load-M5.md)). Duże zdarzenia proporcjonalnie obciążają pamięć i dysk.

> **Wskazówka:** gdy agregat zbliża się do limitu, zapytaj, czy należy go podzielić przez liczbę naturalną
> granica (jedna na miesiąc, jedna na wersję dokumentu), włącz migawki ([rozdział 13](13-snapshots.md)),
> lub złóż starą historię za pomocą squasha ([rozdział 14](14-squash.md)). W przypadku dużych dokumentów przechowuj
> plik w innym miejscu i umieść tylko odniesienie w zdarzeniu ([`docs/SCALING.md`](../SCALING.md),
> sekcja 7).

## Czego się nauczyłeś

- Obsługujące zdarzenia odbierają użytkownika, czas zapisania zdarzenia i stan przed nim, a następnie zwracają
  nowy stan; są czyste, obsługują każde zapisane zdarzenie i nigdy nie zgłaszają wyjątków.
- Nowy agregat rozpoczyna się od `FirstEvent`, który nadaje nazwę swojej przestrzeni; `SpaceId(0)` to platforma.
- Zwrócenie `null` powoduje delikatne usunięcie agregatu, zachowując jego historię; `PermanentDeleteEvent` usuwa
  zostanie na zawsze, gdy każdy subskrybent ją zobaczy.
- Zdarzenie `UndoEvent` anuluje najnowsze zdarzenia, które wciąż obowiązują, a zdarzenie `DuplicationEvent` rozpoczyna nowe
  agreguj jako kopię innej wersji; twoje procedury obsługi zwracają stan niezmieniony dla obu.
- Polecenie, które łączy `WithMetadata` z etykietami zdarzeń za pomocą koperty `EventMetadata`,
  projekcje i historia odczytuje patrz.
- Klasa zdarzeń przechowywanych nigdy nie zmienia kształtu: dodaj klasę `V2`, obsłuż obie i sprawdź przechowywane dane
  z `StoredDataCheck` przed uaktualnieniem.
- Agregat może zawierać maksymalnie 10 000 zdarzeń, każde o długości maksymalnie 100 000 znaków, chyba że ustawisz
  w przeciwnym razie obowiązują ograniczenia sklepu z wydarzeniami.

Następnie: [rozdział 7](07-reading-the-past.md) otwiera pamiętnik i czyta przeszłość.