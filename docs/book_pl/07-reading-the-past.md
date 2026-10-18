# Rozdział 7. Spojrzenie w przeszłość

Tablica wie tylko to, co jest na niej napisane teraz. Pamiętnik wie wszystko, co kiedykolwiek się wydarzyło.
Ponieważ ReactiveCQRS przechowuje każde zdarzenie, możesz zadawać agregatowi pytania, na które zwykła baza
danych nie odpowie: „co było w tym koszyku wczoraj o 18:00?”, „kto usunął jabłka?”, „jak wyglądał koszyk
przed tamtym cofnięciem?”. Pod koniec tego rozdziału będziesz znać komunikaty, które czytają przeszłość
agregatu, i ich odpowiedzi, a także wiedzieć, jak niezawodnie czytać własne zapisy i co dzieje się
z przeszłością, gdy stara historia zostanie zwinięta.

---

## Sześć pytań do szyny poleceń

O przeszłość pytasz tak samo, jak wysyłasz polecenia: pytasz szynę poleceń danego typu agregatu.

| Komunikat | Pyta o | Dostajesz |
|---|---|---|
| `GetAggregate(id)` | agregat teraz (rozdział 4) | `Try[Aggregate[R]]` |
| `GetEventsForAggregate(id)` | wszystkie zdarzenia | `Try[List[EventInfo[R]]]` |
| `GetEventsForAggregateForVersion(id, version)` | zdarzenia do danej wersji | `Try[List[EventInfo[R]]]` |
| `GetAggregateForVersion(id, version)` | agregat taki, jaki był w danej wersji | `Try[Aggregate[R]]` |
| `GetAggregateAtInstant(id, instant)` | agregat taki, jaki był w danej chwili | `Try[Aggregate[R]]` |
| `GetAggregateMinVersion(id, version, maxMillis)` | agregat, gdy już osiągnie daną wersję | `Try[Aggregate[R]]` |

`R` to klasa stanu agregatu: dla koszyków `ShoppingCart`. Wszystkie sześć komunikatów znajduje się
w `io.reactivecqrs.api`.

> **Pamiętaj:** w odróżnieniu od odpowiedzi na polecenia te odpowiedzi są typu `scala.util.Try`. Nieudany
> odczyt i tak przychodzi jako *udany* `Future`, w którym siedzi `Failure`. Zawsze zaglądaj do środka `Try`.

Przykłady korzystają z przygotowania z rozdziału 5 i koszyka „Zakupy” z rozdziału 6, a uruchamia je
`PastSpec`, który sprawdza, co każdy z nich dostaje w odpowiedzi. Oto historia koszyka z jednym zdarzeniem
więcej: po swoim cofnięciu Alicja sama usunęła pomarańcze.

```
version 1  ShoppingCartCreated(Groceries)   Alice
version 2  ItemAdded(apples)                Alice
version 3  ItemAdded(oranges)               Bob
version 4  ItemRemoved(1)                   Bob, by mistake
version 5  ShoppingCartChangesUndone(1)     Alice: the apples are back
version 6  ItemRemoved(2)                   Alice: no oranges after all
```

Potrzebne są jeszcze dwa importy:

<!-- snippet: cart/PastSpec.scala#imports -->
```scala
import java.time.Instant
import scala.util.{Failure, Success, Try}
```

## Cały pamiętnik: GetEventsForAggregate

<!-- snippet: cart/PastSpec.scala#diary -->
```scala
val diary = (commandBus ? GetEventsForAggregate(cartId)).mapTo[Try[List[EventInfo[ShoppingCart]]]]
diary.foreach {
  case Success(events) =>
    events.foreach(e => println(f"${e.version.asInt}%2d  ${e.event.toString}%-30s  ${e.metadata.actor}"))
  case Failure(error) =>
    println("Cannot read the cart: " + error.getMessage)
}
```

wypisuje:

```
 1  ShoppingCartCreated(Groceries)  user:1
 2  ItemAdded(apples)               user:1
 3  ItemAdded(oranges)              user:2
 4  ItemRemoved(1)                  user:2
 5  ShoppingCartChangesUndone(1)    user:1
 6  ItemRemoved(2)                  user:1
```

Każde `EventInfo` zawiera:

- `version`: wersję zdarzenia;
- `event`: samo zdarzenie;
- `userId`: kto wysłał polecenie, które je zapisało;
- `timestamp`: kiedy zostało zapisane;
- `metadata`: jego kopertę z rozdziału 6, a dla zdarzenia zapisanego bez koperty kopertę, której aktorem
  jest `"user:"` i identyfikator użytkownika (to właśnie widać w ostatniej kolumnie).

Co warto wiedzieć o tej liście:

- **To cały pamiętnik.** Jest w nim anulowane usunięcie (wersja 4) i jest w nim cofnięcie (wersja 5). Nic
  w `EventInfo` nie oznacza zdarzenia jako anulowanego; tę część historii opowiadają zdarzenia cofnięcia.
- **Dla duplikatu lista zaczyna się od zdarzeń agregatu bazowego** aż do skopiowanej wersji, z numerami
  takimi jak w agregacie bazowym, a po nich idą własne zdarzenia duplikatu, od wersji 1.
- **Cała lista przychodzi w jednym komunikacie.** Dla koszyka to żaden kłopot, ale długa historia swoje
  waży: w pomiarze z 17/10/2026 odczyt wszystkich 50 001 zdarzeń jednego agregatu trwał 264 ms, a sterta
  urosła szczytowo do 168 MB (jedno uruchomienie na jednej maszynie deweloperskiej,
  [`docs/perf/load-M5.md`](../perf/load-M5.md)). Przy długich historiach czytaj raczej do wybranej wersji.

## Część pamiętnika: GetEventsForAggregateForVersion

To samo, ale z zatrzymaniem na danej wersji:

<!-- snippet: cart/PastSpec.scala#upTo -->
```scala
val firstThree = commandBus ? GetEventsForAggregateForVersion(cartId, AggregateVersion(3))
// Success(List(...)) with the events of versions 1, 2 and 3
```

## Agregat taki, jaki był: GetAggregateForVersion

Ten komunikat odbudowuje stan taki, jaki był zaraz po danej wersji:

<!-- snippet: cart/PastSpec.scala#atVersion -->
```scala
val atVersion4 = (commandBus ? GetAggregateForVersion(cartId, AggregateVersion(4))).mapTo[Try[Aggregate[ShoppingCart]]]
atVersion4.foreach(println)
```

Odpowiedź to `Success(Aggregate(cartId, AggregateVersion(4), Some(ShoppingCart("Groceries", Vector(Item(2, "oranges"))))))`.
W wersji 4 jabłek już nie było: cofnięcie przyszło później, w wersji 5, więc odczyt wersji 4 pokazuje koszyk
dokładnie takim, jakim zostawił go Bob. Poproś o wersję 5, a dostaniesz z powrotem obie pozycje; poproś
o wersję 6, a zostaną tylko jabłka.

Co warto wiedzieć:

- **Wersja 0** daje odpowiedź `Aggregate(cartId, AggregateVersion.ZERO, None)`: przed pierwszym zdarzeniem
  nic nie istniało.
- **Wersja późniejsza niż najnowsza** daje najnowszą. Gdy to ważne, sprawdź wersję w odpowiedzi.
- **Wersja, w której agregat był usunięty** (jego stan był `null`, rozdział 6), daje `Aggregate`, którego
  `aggregateRoot` to `None`.
- **Z migawkami** (rozdział 13) odczyt zaczyna się od migawki z tej wersji lub wcześniejszej, jeśli taka
  jest, i odtwarza tylko zdarzenia po niej. Odpowiedź jest ta sama, tylko przychodzi szybciej.

## Agregat w danej chwili: GetAggregateAtInstant

„Co było w koszyku, gdy złożono zamówienie, 16/10/2026 o 18:00 UTC?”

<!-- snippet: cart/PastSpec.scala#atInstant -->
```scala
val whenOrdered = (commandBus ? GetAggregateAtInstant(cartId, Instant.parse("2026-10-16T18:00:00Z")))
  .mapTo[Try[Aggregate[ShoppingCart]]]
whenOrdered.foreach {
  case Success(cart)  => println(s"At that moment: version ${cart.version.asInt}, ${cart.aggregateRoot}")
  case Failure(error) => println("Cannot read the cart: " + error.getMessage)
}
```

Biblioteka znajduje najnowszą wersję zapisaną w tej chwili lub wcześniej i czyta agregat w tej wersji,
z cofnięciami takimi, jakie były wtedy: zdarzenie anulowane *po* tej chwili nadal się liczy. Jeśli więc
zamówienie złożono między wersjami 4 i 5, widzisz koszyk bez jabłek, czyli to, co widział klient.

Co warto wiedzieć:

- **Czasy to zapisane znaczniki czasu**: chwile zapisania zdarzeń według zegara węzła, który je zapisał;
  ten sam `timestamp`, który dostają twoje handlery zdarzeń.
- **Chwila sprzed pierwszego zdarzenia** daje wersję 0 i brak stanu.
- **Migawki nie są używane** przy odczytach w danej chwili, bo migawka nie zapamiętuje, kiedy agregat
  osiągnął jej wersję. Czytana jest cała historia aż do tej chwili.

> **Wskazówka:** w klastrze `GetAggregateAtInstant(id, Instant.now)` to wygodny sposób, by odczytać duży
> stan na węźle, na którym jesteś: stan jest tam odtwarzany z magazynu, zamiast być przesyłany z węzła, na
> którym trzyma go właściciel agregatu ([rozdział 17](17-cluster.md)).

## Czytanie własnych zapisów: GetAggregateMinVersion

Zamawiasz kawę i dostajesz paragon z numerem 42. Przy ladzie prosisz o numer 42; jeśli jeszcze nie jest
gotowy, czekasz chwilę, zamiast odejść z numerem 41.

Każde udane polecenie podaje wersję, która z niego powstała: `SuccessResponse(cartId, AggregateVersion(7))`. Odczyt
zrobiony później powinien pokazać co najmniej tę wersję. Na jednym węźle tak jest: właściciel aktualizuje
swój stan, zanim odpowie na polecenie, więc `GetAggregate` wysłane po odpowiedzi już pokazuje zmianę. Nie
zawsze jednak można mieć pewność: czytający może nie być tym, kto wysłał polecenie, polecenie mogło zostać
wysłane bez czekania na odpowiedź albo zmiana może być jeszcze w drodze (następny krok sagi, rozdział 12).
`GetAggregateMinVersion` zamienia „co najmniej wersja 7” w obietnicę:

<!-- snippet: cart/PastSpec.scala#minVersion -->
```scala
val atLeast7 = (commandBus ? GetAggregateMinVersion(cartId, AggregateVersion(7), 2000)).mapTo[Try[Aggregate[ShoppingCart]]]
atLeast7.foreach {
  case Success(cart) =>
    println(s"Up to date: version ${cart.version.asInt}")  // version 7 or later
  case Failure(e: AggregateInIncorrectVersionException) =>
    println(s"Still at version ${e.currentVersion.asInt}, waiting for ${e.requestedVersion.asInt}")
  case Failure(error) =>
    println("Cannot read the cart: " + error.getMessage)
}
```

- Gdy agregat jest w tej wersji lub późniejszej, odpowiedź przychodzi od razu.
- W przeciwnym razie właściciel agregatu wstrzymuje pytanie i odpowiada w chwili, gdy ta wersja zostanie
  zapisana.
- Jeśli wersja nie pojawi się w ciągu `maxMillis` milisekund (tutaj to 2 sekundy), odpowiedzią jest
  `Failure(AggregateInIncorrectVersionException)`, a wyjątek podaje wersję, w której jest agregat, i tę,
  o którą pytasz. Właściciel przetrzymuje najwyżej 1 000 takich pytań, każde najwyżej 5 minut; kolejne
  dostaje taką odpowiedź od razu.

> **Pamiętaj:** limit czasu twojego zapytania (ask) to osobny zegar. Ustaw go dłuższy niż `maxMillis`, bo
> inaczej twój `Future` może zakończyć się błędem, zanim właściciel zdąży odpowiedzieć.

> **Wskazówka:** dobry nawyk w interfejsach użytkownika: zachowaj wersję z odpowiedzi na ostatnie polecenie
> i przekaż ją przy następnym odczycie. Rozdział 8 ma podobne narzędzie dla projekcji, które zawsze są trochę
> w tyle za agregatami.

## Poniżej linii squasha

Długie historie można zwinąć *squashem* ([rozdział 14](14-squash.md)): zdarzenia od wersji 1 do N zastępuje
jedno zdarzenie squasha w wersji N, które przechowuje zbudowany przez nie stan. Wersja i stan agregatu
zostają takie, jakie były. Jego przeszłość jest jednak krótsza:

- `GetEventsForAggregate` zaczyna od zdarzenia squasha, w wersji N;
- odczyty wersji poniżej N (`GetAggregateForVersion`, `GetEventsForAggregateForVersion`) i odczyty w chwili
  sprzed czasu zdarzenia squasha odpowiadają `Failure(AggregateSquashedException)`. Historii, której
  potrzebują, już nie ma, a biblioteka nie będzie zgadywać;
- odczyty wersji N i późniejszych działają jak dotąd.

<!-- snippet: cart/PastSpec.scala#squashed -->
```scala
case Failure(e: AggregateSquashedException) =>
  println(s"The history before version ${e.squashedUpTo.asInt} was folded away")
```

Wyjątek podaje też, o którą wersję pytasz (`requested`), a przy odczycie w danej chwili także, o którą
chwilę (`at`).

### Archiwum

Niektóre zdarzenia są ważne dla audytów nawet po squashu: kto zatwierdził zamówienie, kto co dodał. Polityka
squasha może wskazać klasy zdarzeń do zachowania, a squash przenosi wtedy te zdarzenia do archiwum, zamiast
je usuwać. Czyta je `GetArchivedEvents(id)`:

<!-- snippet: cart/PastSpec.scala#archive -->
```scala
val archived = (commandBus ? GetArchivedEvents(cartId)).mapTo[Try[ArchivedEvents[ShoppingCart]]]
archived.foreach {
  case Success(archive) =>
    archive.events.foreach(a => println(s"${a.info.version.asInt}  ${a.info.event}  archived at ${a.archived}"))
  case Failure(error) =>
    println("Cannot read the archive: " + error.getMessage)
}
```

Zdarzenia przychodzą od najstarszego. Każde `ArchivedEvent` zawiera `EventInfo` zdarzenia (`info`), wersję
cofnięcia, które je anulowało, jeśli takie było (`undoneFrom`), i chwilę archiwizacji (`archived`). Dla
agregatu, który nie ma nic w archiwum, `events` jest po prostu puste. Odtwarzanie, ponowne dostarczenia,
przebudowy i pozostałe odczyty historii nigdy nie czytają archiwum: jest po to, żeby do niego zaglądać.
Rozdział 14 pokazuje, jak wybrać, co do niego trafia.

## Gdy odczyt się nie uda

Warto odróżniać dwa rodzaje niepowodzeń:

- `Failure(AggregateSquashedException)`: to wcale nie błąd, tylko fakt dotyczący historii (opisany wyżej).
- `Failure(AggregateRestoreFailedException)`: agregatu nie dało się odczytać. Przyczyna jest w środku:
  zwykle zapisane zdarzenie, którego twój kod już nie potrafi odczytać (zmieniona albo brakująca klasa
  zdarzenia, rozdział 6), albo kłopot z bazą danych. Biblioteka odnotowuje to też w logu, razem
  z identyfikatorem agregatu.

## Gdzie działają odczyty

Odczyty historyczne (`GetEventsForAggregate`, `GetEventsForAggregateForVersion`, `GetAggregateForVersion`,
`GetAggregateAtInstant` i `GetArchivedEvents`) w ogóle nie angażują właściciela agregatu. Szyna poleceń
czyta magazyn w tle, na węźle, do którego się zwracasz, w puli wątków, której biblioteka używa do pracy
z bazą danych. Dzięki temu te odczyty nie budzą uśpionego agregatu, nie czekają w kolejce za jego
poleceniami i widzą dokładnie to, co zostało zatwierdzone.

Z `GetAggregate` i `GetAggregateMinVersion` jest inaczej: odpowiada na nie właściciel agregatu,
uruchamiany w razie potrzeby, bo te komunikaty dotyczą teraźniejszości agregatu.

> **Wskazówka:** te odczyty dotyczą jednego agregatu naraz, co pasuje do ekranów audytu, paneli „co się
> zmieniło?”, narzędzi pomocy technicznej i debugowania. Pytania o wiele agregatów („każdy koszyk
> z pomarańczami”) należą do projekcji: o nich jest następny rozdział.

> **Pod maską:** historyczne odczyty stanów i zdarzeń przechodzą przez moduł odtwarzania,
> `AggregateRestorer`, który ukrywa łańcuchy duplikatów, cofnięte zdarzenia i migawki; właściciel też go
> używa, by przy starcie odczytać swój agregat. Archiwum jest czytane wprost z jego tabeli. Zobacz
> [ARCHITECTURE.md, sekcja 4.3](../ARCHITECTURE.md).

## Co już umiesz

- Szyna poleceń odpowiada na pytania o przeszłość agregatu, a odpowiedzi to `Try`, więc zawsze zaglądaj do
  środka.
- `GetEventsForAggregate` daje cały pamiętnik jako listę `EventInfo` z wersją, użytkownikiem, czasem
  i metadanymi; `GetEventsForAggregateForVersion` zatrzymuje się na danej wersji.
- `GetAggregateForVersion` i `GetAggregateAtInstant` odbudowują stan taki, jaki był w danej wersji lub
  chwili, z cofnięciami takimi, jakie były wtedy.
- `GetAggregateMinVersion` czeka, aż agregat osiągnie daną wersję, albo po `maxMillis` kończy się
  wyjątkiem `AggregateInIncorrectVersionException`: to narzędzie do czytania własnych zapisów.
- Poniżej punktu squasha odczyty historii kończą się wyjątkiem `AggregateSquashedException`; zdarzenia
  zachowane przez politykę squasha nadal można czytać przez `GetArchivedEvents`.
- Odczyty historyczne pochodzą z magazynu, działają na węźle, który pyta, i nie budzą właściciela agregatu.

Dalej: [rozdział 8](08-projections.md) zamienia zdarzenia w widoki, o które można pytać.
