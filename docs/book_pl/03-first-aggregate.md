# Rozdział 3. Twój pierwszy agregat: koszyk zakupowy

Czas napisać kod, który będzie dotyczył *Twojego* biznesu, a nie hydrauliki. Do końca tego rozdziału
będziesz mieć kompletny zbiór koszyka zakupowego: stan, jaki utrzymuje, zdarzenia, które go zmieniają,
polecenia, które ludzie wysyłają, zasady, które decydują, i kontekst, który to wszystko łączy. To jest
Wszystko w czystej Scali: bez aktorów, bez bazy danych, bez niczego na początek. Możesz nawet wypróbować to za pomocą prostego
złożymy, i tak zrobimy na końcu. Rozdział 4. ożywia to.

---

## Plan na serwetce

Koszyk ma nazwę i listę produktów. Użytkownicy mogą utworzyć koszyk, dodać do niego produkt i usunąć go.
przedmiot z niego. Jedna zasada domowa: **nie więcej niż pięć przedmiotów na koszyk.**

Zanim napiszesz jakikolwiek kod, wypisz, o co ludzie mogą prosić i co się stanie, gdy koszyk odpowie twierdząco:

| Ktoś pyta (polecenie) | Jeśli wózek się zgadza, to się wydarzyło (wydarzenie) |
|---|---|
| `CreateShoppingCart("Artykuły spożywcze")` | `ShoppingCartCreated("Artykuły spożywcze")` |
| `AddItem(..., "jabłka")` | `ItemAdded("jabłka")` |
| `UsuńElement(..., 1)` | `ElementUsunięto(1)` |

Zwróć uwagę na gramatykę. Polecenia to rozkazy w trybie rozkazującym („utwórz”, „dodaj”); zdarzenia to fakty w
czas przeszły („stworzony”, „dodany”). Ten mały nawyk pozwala ci zachować spokojną głowę przez resztę dnia.
książka: polecenie można nadal odrzucić, zdarzenie już nastąpiło.

Wszystko umieścimy w jednym pakiecie, `com.example.shop.cart`, w plikach sugerowanych w
[rozdział 2](02-setting-up.md). Prawdziwym porównaniem jest domena testowa w tym repozytorium,
`testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`. Może wykonywać więcej czynności (cofanie, kopiowanie,
usunięcie), a tych statystów spotkasz w [rozdziale 6](06-events.md).

## Krok 1: stan

Co wózek musi zapamiętać? Swoją nazwę i przedmioty. To jest **stan**, zwany również
**korzeń agregatu**: zwykła klasa przypadków.

```scala
pakiet com.example.shop.cart

klasa przypadku ShoppingCart(nazwa: String, elementy: Vector[element])

klasa przypadku Element(id: Int, nazwa: String)
```

Celowo brakuje dwóch rzeczy: identyfikatora koszyka i jego wersji. Biblioteka przechowuje je obok
twojego stanu, nigdy w nim nie będąc, i wręcza ci wszystkie trzy razem, gdy prosisz o wózek (
`Aggregate(id, version, aggregateRoot)`, w rozdziale 4).

**Zachowaj niezmienność stanu.** Klasy przypadków, `Wektor`, `Lista`, `Mapa`: tak. Zmienna `var` lub zmienna
kolekcja: nigdy. Biblioteka przekazuje tę samą wartość stanu kilku wątkom jednocześnie (Twoje polecenie
obsługa zdarzeń jest wykonywana w przyszłości, a magistrala zdarzeń przekazuje stan do projekcji) i wartości niezmienne
są jedynymi, które przetrwały to bez niespodzianek.

> **Ostrzeżenie:** ostrożnie wybieraj pakiet i nazwę klasy stanu. Jej pełna nazwa,
> `com.example.shop.cart.ShoppingCart`, *jest* nazwą typu agregatu w bazie danych. Zmień nazwę
> lub przenieś klasę później, a biblioteka zobaczy zupełnie nowy typ agregatu bez agregatów
> je, podczas gdy stare znajdują się w bazie danych pod starą nazwą.

## Krok 2: wydarzenia

Każdy wiersz w dzienniku wózka to zdarzenie: mała klasa przypadku rozszerzająca `Event` ze stanem
klasa jako parametr typu.

```scala
pakiet com.example.shop.cart

importuj io.reactivecqrs.api._
importuj io.reactivecqrs.api.id.SpaceId

klasa przypadku ShoppingCartCreated(nazwa: String) rozszerza FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}

klasa przypadku ItemAdded(nazwa: String) rozszerza Event[ShoppingCart]

klasa przypadku ItemRemoved(id: Int) rozszerza Event[ShoppingCart]
```

Parametr typu `Event[ShoppingCart]` określa, jaki typ agregatu ma dane zdarzenie
należy do.

`ShoppingCartCreated` jest zdarzeniem szczególnym: jest to zdarzenie, które *uruchamia* koszyk, więc rozszerza
`FirstEvent`. Pierwsze zdarzenie ma jedno zadanie: musi wskazywać, do której **przestrzeni** należy nowy agregat.
Przestrzeń to organizacja, najemca Twojej aplikacji; `SpaceId(0)` to sama platforma.
Dopóki Twoja aplikacja nie będzie miała własnych organizacji, każde pierwsze zdarzenie będzie zawierało „SpaceId(0)”, ponieważ
domeny testowej. Przestrzenie mają własny rozdział ([rozdział 16](16-organizations.md)).

> **Pamiętaj:** każde pierwsze zdarzenie musi dawać spację. Nowy agregat, którego pierwsze zdarzenie nie daje żadnej spacji,
> (`SpaceId.unknown` lub jakakolwiek inna liczba ujemna) jest odrzucane: na polecenie odpowiada się
> `FailureResponse` informuje, że nowy agregat wymaga spacji i nic nie jest przechowywane.

Oto kilka zasad, dzięki którym wydarzenia staną się przyjemne:

- **Czas przeszły, zawsze.** `ItemAdded`, nie `AddItem`.
- **Fakty, nie instrukcje i tylko fakty.** `ItemAdded("apples")` nie przenosi nowego
  Identyfikator elementu: stan może to ustalić samodzielnie (krok 5 pokazuje jak). Zachowaj małe zdarzenia.
- **Zwykłe dane.** Ciągi znaków, liczby, `Opcje`, kolekcje i inne klasy o małym rozmiarze. Zdarzenia
  są przechowywane jako JSON przez mpjsons, który odczytuje klasy według ich pól.
- **Nazwy są wieczne.** Zdarzenie jest przechowywane pod nazwą swojej klasy, a stare zdarzenia są odczytywane
  pod tą samą nazwą, lata później. Nigdy nie zmieniaj nazwy, nie przenoś ani nie zmieniaj kształtu klasy zdarzeń, która została zapisana.
  Gdy zdarzenie musi zostać zmienione, dodaj obok niego nową klasę (`ItemAddedV2`) i zachowaj starą;
  [Rozdział 6](06-events.md) pokazuje jak.

> **Pod maską:** oto, co biblioteka przechowuje dla drugiego zdarzenia koszyka: wiersz w
> tabela `events` z identyfikatorem koszyka, wersją 2, użytkownikiem, czasem i numerem klasy
> `com.example.shop.cart.ItemAdded` (ze słownika `types_names` w rozdziale 2) i JSON
> `{"name":"apples"}`. Szczegóły znajdują się w pliku [ARCHITECTURE.md, sekcja 2](../ARCHITECTURE.md).

## Krok 3: polecenia

Polecenia to również klasy przypadków. Każde z nich rozszerza jedną z klas poleceń biblioteki, która decyduje
jak to jest obsługiwane:

```scala
pakiet com.example.shop.cart

importuj io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}

klasa przypadku CreateShoppingCart(userId: UserId, name: String)
  rozszerza FirstCommand[ShoppingCart, CustomCommandResponse[_]]

klasa przypadku AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   nazwa: String) rozszerza Command[ShoppingCart, CustomCommandResponse[_]]

klasa przypadku RemoveItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                      id: Int) rozszerza Command[ShoppingCart, CustomCommandResponse[_]]
```

- **`FirstCommand`** tworzy nowy agregat. Nie ma on `aggregateId`: koszyk nie istnieje.
  a jednak biblioteka nadaje mu identyfikator.
- Polecenie **`Command`** zmienia istniejący agregat. Nadaje nazwę agregatowi (`aggregateId`) i
  wersja, którą przeglądałeś, kiedy zdecydowałeś się ją wysłać (`expectedVersion`). To jest optymistyczna
  blokowanie [rozdziału 1](01-cqrs-without-the-headache.md): jeśli wózek od tego czasu się przesunął,
  polecenie zostało odrzucone i nic się nie zmieniło.
- Każde polecenie zawiera **`userId`**: kto pyta. Biblioteka przechowuje go z każdym zdarzeniem.
  polecenie produkuje.

Pierwszym parametrem typu jest agregat, dla którego przeznaczone jest polecenie. Drugim jest rodzaj odpowiedzi,
osoba dzwoniąca otrzymuje; `CustomCommandResponse[_]` oznacza „dowolną odpowiedź biblioteki” i to jest to, co każdy
Polecenie w domenie testowej używa. Użyj go również.

> **Wskazówka:** nazwy pól nie zależą od Ciebie. `userId`, `aggregateId` i `expectedVersion` są
> członkowie deklarują klasy bazowe, a pola klasy przypadku je wypełniają. Wpisz je dokładnie
> więc wszystkie inne pola (`name`, `id`) możesz nazwać.

> **Pamiętaj:** w domenie testowej `CreateShoppingCart` ma z przodu jeszcze jedno pole,
> `idempotencyId: Option[SagaStep]`, a także dodaje `IdempotentCommand`. To sprawia, że ​​polecenie
> bezpiecznie wysłać dwa razy, dlatego w specyfikacji biblioteki jest napisane `CreateShoppingCart(None, userId,
> „Artykuły spożywcze”). Pomijamy to tutaj, aby zachować prostotę pierwszego koszyka; polecenia idempotentne są dostępne
> [rozdział 5](05-command-flavors.md), z innymi rodzajami poleceń.

## Krok 4: reguły (obsługa poleceń)

**Obsługa poleceń** analizuje aktualny stan i polecenie i podejmuje decyzję. W ReactiveCQRS
obsługa poleceń agregatu to jedna funkcja: podaj mu aktualny stan, a on ci go poda
z powrotem częściową funkcję z poleceń do wyników. Znajduje się ona w klasie kontekstu z kroku 6; tutaj
jest samodzielna:

```scala
nadpisz def commandHandlers = cart => {
  przypadek c: UtwórzKoszykZakupowy =>
    CommandSuccess(ShoppingCartCreated(c.name))
  przypadek c: Dodaj element =>
    if (cart.items.size >= 5) CommandFailure("Nie możesz mieć więcej niż 5 produktów w koszyku")
    w przeciwnym razie CommandSuccess(ItemAdded(c.name))
  przypadek c: Usuń element =>
    jeśli (cart.items.exists(_.id == c.id)) CommandSuccess(ItemRemoved(c.id))
    else CommandFailure("W koszyku nie ma żadnego przedmiotu " + c.id + "")
}
```

Przeczytaj to jak listę zasad domowych. `cart` to wózek taki, jaki jest teraz; w przypadku `FirstCommand`,
który nie ma jeszcze koszyka, jest to pusty stan początkowy kroku 6. Każdy „przypadek” mówi, co z nim zrobić
jeden rodzaj polecenia. Każdy przypadek kończy się **wynikiem**:

| Wracasz | To znaczy | Dzwoniący otrzymuje (rozdział 4) |
|---|---|---|
| `CommandSuccess(event)` lub `CommandSuccess(Seq(event1, event2))` | Tak: zapisz te zdarzenia. | `SuccessResponse` |
| `CommandSuccess(event, info)` | Tak, a oto coś dla osoby wywołującej. | `CustomSuccessResponse` zawierające `info` |
| `CommandFailure("reason")` lub `CommandFailure(List("reason", "another"))` | Nie, i to jest powód. Nic nie jest zapisywane. | ​​`FailureResponse` |
| `AsyncCommandResult(future)` | Najpierw muszę na coś poczekać; odpowiedź brzmi: jedno z powyższych, później. | cokolwiek przyniesie przyszłość |

Nigdy nie sprawdzasz samodzielnie `expectedVersion`. Biblioteka porównuje ją z wersją w koszyku.
*zanim* zostanie uruchomiony Twój moduł obsługi, a polecenie ze starą wersją nigdy do niego nie dotrze.

Czasami reguła musi poczekać: na przykład na cennik lub usługę magazynową. Następnie zwróć
`AsyncCommandResult` wokół `Future` wyniku. Oto wariant `AddItem` z
wyimaginowana usługa giełdowa, której `isAvailable` odpowiada na `Future[Boolean]` (i z
`ExecutionContext` Twojego zakresu):

```scala
przypadek c: Dodaj element =>
  AsyncCommandResult(stock.isAvailable(c.name).map { available =>
    jeśli (dostępne) CommandSuccess(ItemAdded(c.name))
    else CommandFailure(c.name + " jest niedostępny")
  })
```

W kontekście zbiorczym możesz nawet zwrócić sam `Przyszłość`: kontekst zamienia `Przyszłość`
wyniku do `AsyncCommandResult`. `createShoppingCart` domeny testowej robi to
dokładnie to. [Rozdział 5](05-command-flavors.md) zawiera więcej informacji na temat wolnych procedur obsługi poleceń.

Co się stanie, jeśli żaden `case` nie będzie pasował do polecenia? Wywołujący otrzyma `CommandHandlingError`, a dziennik zostanie
Szczegóły. Tak więc: jeden „przypadek” na każde polecenie agregatu.

> **Ostrzeżenie:** nie wysyłaj wiadomości e-mail, nie obciążaj kart ani nie dzwoń do innych systemów z programu do obsługi poleceń.
> Obsługujący może być uruchomiony więcej niż raz dla jednego polecenia (polecenie współbieżne jest uruchamiane ponownie po
> konflikt, [rozdział 5](05-command-flavors.md)), a jego wynik może zostać odrzucony (inne polecenie
> w międzyczasie zmieniono koszyk lub obsługa trwała dłużej niż limit czasu polecenia). Efekty uboczne
> należą *po* zapisaniu zdarzeń: w projekcjach i sagach, rozdziały 8 i 12.

> **Wskazówka:** procedury obsługi poleceń działają w ramach własnej puli wątków `reactivecqrs.handler-dispatcher`
> (rozdział 2), więc powolny nigdy nie zatrzymuje magistrali zdarzeń, projekcji ani sag. Zawiera jeden z
> jednak cały czas w tej puli są 32 wątki, a gdy każdy wątek jest wstrzymany, każde polecenie czeka. Umieść
> długiego oczekiwania na wątkach własnych za pomocą `AsyncCommandResult`. [docs/SCALING.md](../SCALING.md),
> Sekcja 8 pokazuje, co się stanie w przeciwnym razie.

## Krok 5: w jaki sposób zdarzenia zmieniają stan (obsługa zdarzeń)

Osoba obsługująca polecenie podjęła decyzję; zdarzeniem jest zapisana decyzja. Teraz ktoś musi ją zastosować.
to zdarzenie do stanu. To zadanie **obsługujących zdarzenia**, znowu jedna funkcja:

```scala
nadpisz def eventHandlers = (userId, timestamp, cart) => {
  przypadek e: Utworzono koszyk =>
    Koszyk(e.name, Vector.empty)
  przypadek e: Dodano element =>
    val nextId = cart.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
    cart.copy(elementy = cart.items :+ Element(nextId, e.name))
  przypadek e: Element usunięty =>
    cart.copy(elementy = cart.items.filterNot(_.id == e.id))
}
```

Otrzymuje trzy rzeczy: `userId`, użytkownika, którego polecenie zapisało zdarzenie; `timestamp`, moment
został zapisany (w `java.time.Instant`); i `cart`, stan przed zdarzeniem. Zwraca stan
po zdarzeniu. To wszystko, co kiedykolwiek robi program obsługi zdarzeń.

Spójrz na `ItemAdded`: identyfikator nowego elementu jest obliczany na podstawie stanu (o jeden większy niż największy identyfikator, więc
(daleko). Ponieważ program obsługi otrzymuje ten sam stan za każdym razem, gdy odtwarza ten sam dziennik, zawsze pojawia się
z tymi samymi identyfikatorami. To jest rodzaj myślenia, jakiego potrzebują obsługujące zdarzenia, ponieważ uruchamiają się ponownie i
Ponownie:

1. **gdy polecenie zostanie wykonane pomyślnie**, aby ustalić nowy stan przed zapisaniem czegokolwiek;
2. **za każdym razem, gdy koszyk jest odczytywany z bazy danych**: biblioteka odtwarza każdy z nich
   zdarzenia w wózku poprzez te moduły obsługi (dziennik z rozdziału 1, czytany od góry);
3. **kiedy projekcja jest odbudowywana** na podstawie zapisanych zdarzeń ([rozdział 10](10-rebuilding-projections.md)).

Stąd trzy złote zasady obsługi zdarzeń:

- **Nigdy nie odmawiaj.** Wydarzenie już miało miejsce; moment na „nie” był momentem dla osoby obsługującej polecenia.
  Bez względu na stan, zwróć nowy stan.
- **Brak efektów ubocznych.** Osoba wysyłająca wiadomość e-mail wysyła ją przy każdym odtworzeniu.
- **To samo wejście, to samo wyjście.** Bez `Instant.now()`, bez liczb losowych, bez odczytu z bazy danych.
  Potrzebujesz czasu? Użyj znacznika czasu, który jest taki sam dla każdego odtworzenia.

Obsługa zdarzeń, która generuje błąd, oznacza zły dzień. Podczas wykonywania polecenia powoduje błąd polecenia z błędem.
`EventHandlingError` i nic nie jest zapisywane. Podczas odtwarzania jest gorzej: nie można odczytać koszyka.
nie wracaj, dopóki nie naprawisz obsługi.

> **Pod maską:** zwrócenie `null` przez obsługę zdarzeń oznacza agregat jako usunięty (
> domena testowa `ShoppingCartDeleted` to robi), a zdarzenia cofania i kopiowania wymagają szczególnej uwagi
> handlery. Oba należą do [rozdziału 6](06-events.md). Sposób uruchamiania handlerów przez bibliotekę jest opisany w
> [ARCHITECTURE.md, sekcja 2.6](../ARCHITECTURE.md).

## Krok 6: kontekst, który je łączy

Ostatnim elementem jest **kontekst zbiorczy**: jedna klasa na typ zbiorczy, która przekazuje bibliotece
wszystko od kroków 1 do 5. Oto cały koszyk:

```scala
pakiet com.example.shop.cart

importuj io.reactivecqrs.api._

klasa ShoppingCartAggregateContext rozszerza AggregateContext[ShoppingCart] {

  nadpisz wersję val: Int = 1

  nadpisanie def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector.empty)

  nadpisz def commandHandlers = cart => {
    przypadek c: UtwórzKoszykZakupowy =>
      CommandSuccess(ShoppingCartCreated(c.name))
    przypadek c: Dodaj element =>
      if (cart.items.size >= 5) CommandFailure("Nie możesz mieć więcej niż 5 produktów w koszyku")
      w przeciwnym razie CommandSuccess(ItemAdded(c.name))
    przypadek c: Usuń element =>
      jeśli (cart.items.exists(_.id == c.id)) CommandSuccess(ItemRemoved(c.id))
      else CommandFailure("W koszyku nie ma żadnego przedmiotu " + c.id + "")
  }

  nadpisz def eventHandlers = (userId, timestamp, cart) => {
    przypadek e: Utworzono koszyk =>
      Koszyk(e.name, Vector.empty)
    przypadek e: Dodano element =>
      val nextId = cart.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
      cart.copy(elementy = cart.items :+ Element(nextId, e.name))
    przypadek e: Element usunięty =>
      cart.copy(elementy = cart.items.filterNot(_.id == e.id))
  }
}
```

Czterech członków, cztery zadania:

- **`initialAggregateRoot`** to pusta strona przed pierwszym zdarzeniem: stan `FirstCommand`
  widzi obiekt obsługujący zdarzenie, oraz stan, od którego rozpoczyna się pierwszy obiekt obsługujący zdarzenie. Pusty koszyk bez nazwy będzie
  Do.
- **`wersja`** to wersja *zachowania* tego kontekstu, a nie żadnego koszyka. Zacznij od 1. Podnieś ją.
  gdy zmieniasz sposób, w jaki obsługujące zdarzenia przetwarzają zdarzenia: zapisane migawki starych
  następnie ignorowane są zachowania ([rozdział 13](13-snapshots.md)), a zarządzane projekcje odczytują
  Stany tego typu są przebudowywane ([rozdział 10](10-rebuilding-projections.md)).
- **`commandHandlers`** i **`eventHandlers`** to kroki 4 i 5.

Domena testowa przechowuje swoje procedury obsługi w dwóch obiektach prostych funkcji, `CommandsHandlers` i
`EventsHandlers` i wywołuje je z kontekstu; w przypadku małej grupy zapisz je w linii jako
powyżej jest równie dobre.

Wszystko inne w `AggregateContext` ma sensowną wartość domyślną, a w późniejszych rozdziałach jest ona włączana, gdy
potrzebujesz tego:

| Członek | Domyślny | Gdzie go spotkasz |
|---|---|---|
| `commandTimeout` | 60 sekund | [rozdział 5](05-command-flavors.md) |
| `commandProcessing` | `CommandProcessing.Concurrent` (pojedynczo to `Serial`) | [rozdział 5](05-command-flavors.md) |
| `snapshots` | `Brak` | [rozdział 13](13-snapshots.md) |
| `squash` | `Żaden` | [rozdział 14](14-squash.md) |
| `rewriteHistoryCommandHandlers` | kończy się niepowodzeniem każdego polecenia history-rewrite | [rozdział 15](15-rewriting-history.md) |
| `placement` | `Placement.Local` | [rozdział 17](17-cluster.md) |
| `writtenFrom` | pusty | [rozdział 17](17-cluster.md) |

> **Pamiętaj:** domyślnie jeden agregat przechowuje maksymalnie 10 000 zdarzeń (rozdział 2). Zachowaj agregaty
> małe i skoncentrowane, jak pojedynczy wózek, a nie „wszystkie wózki jednego klienta”. Rozdziały 13 i 14
> pokaż co zrobić gdy historia i tak się wydłuża.

## Wypróbowanie z fałdem

A oto najlepsza część tego wszystkiego: procedury obsługi są zwykłymi funkcjami, więc możesz je wypróbować bez
rozpoczynanie czegokolwiek. Odtwarzanie pamiętnika to zakładka na wydarzenia:

```scala
importuj java.time.Instant
importuj io.reactivecqrs.api.id.UserId

kontekst val = nowy ShoppingCartAggregateContext
val alice = UserId(1)
val now = Instant.now()

val diary = List(ShoppingCartCreated("Artykuły spożywcze"), ItemAdded("jabłka"), ItemAdded("pomarańcze"), ItemRemoved(1))

val cart = diary.foldLeft(context.initialAggregateRoot) { (stan, zdarzenie) =>
  context.eventHandlers(alicja, teraz, stan)(zdarzenie)
}
// koszyk: ShoppingCart(Spożywcze, Wektor(Przedmiot(2, pomarańcze)))
```

Ta zakładka to w miniaturze to, co biblioteka robi za każdym razem, gdy odczytuje kartę z bazy danych.
A prośba o podjęcie decyzji to jedno wywołanie funkcji:

```scala
importuj io.reactivecqrs.api._
importuj io.reactivecqrs.api.id.AggregateId

decyzja val = context.commandHandlers(cart)(AddItem(alice, AggregateId(1001), AggregateVersion(4), "gruszki"))
// decyzja: CommandSuccess(List(ItemAdded(pears)),Nothing())
```

`Nothing()` na końcu to „brak dodatkowych informacji” zwykłego `CommandSuccess`. To jest
własna mała klasa przypadku biblioteki `io.reactivecqrs.api.Nothing`, a nie `Nothing` języka Scala; w pliku, który
importuje `io.reactivecqrs.api._`, nazwa `Nothing` oznacza, że ​​jest to biblioteka.

Należy zauważyć, że wersja w poleceniu nie ma tutaj znaczenia: jej sprawdzenie jest zadaniem biblioteki, a nie
obsługi, a bezpośrednie wywołanie obsługi pomija to sprawdzenie. [Rozdział 19](19-testing.md) zmienia
przełożyć te jednozdaniowe stwierdzenia na odpowiednie testy.

## Cztery praktyczne zasady

Wszystko w tym rozdziale sprowadza się do czterech nawyków:

1. **Wydarzenia są faktami.** Czas przeszły, niezmienny, krótki i nigdy nie zmieniany po zapisaniu.
2. **Operatorzy poleceń podejmują decyzję.** Patrzą na stan, mówią „tak” w przypadku zdarzeń lub „nie” z podaniem powodu,
   i nie wtrącać się do świata zewnętrznego.
3. **Obsługa zdarzeń jest posłuszna.** Nigdy nie odmawia, nigdy nie powoduje efektów ubocznych i zapewnia ten sam stan.
   te same wydarzenia, za każdym razem.
4. **Państwo odbudowuje się poprzez odtwarzanie wydarzeń.** Dziennik jest prawdą, państwo jest obrazem
   który można w każdej chwili narysować na nowo.

## Czego się nauczyłeś

- Agregat składa się z czterech rodzajów kodu: **stan** (niezmienna klasa przypadku), **zdarzenia**,
  **polecenia** i **kontekst**.
- `FirstEvent` uruchamia agregat i musi nadać nazwę jego **przestrzeni**; `SpaceId(0)` to platforma.
- `FirstCommand` tworzy agregat; `Command` zmienia agregat i przenosi `expectedVersion` dla
  optymistyczne blokowanie.
- **Obsługa poleceń** zwraca `CommandSuccess` ze zdarzeniami, `CommandFailure` z powodami lub
  `AsyncCommandResult` gdy muszą czekać.
- **Obsługa zdarzeń** przyjmuje `(userId, timestamp, state)` i zdarzenie, a następnie zwraca nowy stan;
  muszą być czyste, ponieważ działają przy każdym odtworzeniu.
- **`AggregateContext`** łączy go z `initialAggregateRoot`, `version`,
  `commandHandlers` i `eventHandlers`.
- Nazwy klas stanu i zdarzeń są przechowywane w bazie danych, dlatego należy je dobierać ostrożnie.

Dalej: [rozdział 4](04-first-command.md) podłącza wózek do działającego systemu i wysyła mu pierwszy
rozkaz.