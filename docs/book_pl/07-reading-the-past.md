# Rozdział 7. Spojrzenie wstecz w czasie

Tablica wie tylko to, co jest na niej teraz napisane. Pamiętnik wie wszystko, co się kiedykolwiek wydarzyło. Od
ReactiveCQRS przechowuje każde zdarzenie, dzięki czemu możesz zadać pytania zbiorcze, na które normalna baza danych nie jest w stanie odpowiedzieć:
„Co było w tym wózku wczoraj o 18:00?”, „Kto zabrał jabłka?”, „Jak wyglądał wózek?”
przed tym cofnięciem?". Do końca tego rozdziału poznasz komunikaty odczytujące agregat
przeszłość, co ona odpowiada, jak niezawodnie czytać własne teksty i co się dzieje z przeszłością, gdy
stara historia została złożona.

---

## Sześć pytań do magistrali poleceń

Pytasz o przeszłość w ten sam sposób, w jaki wysyłasz polecenia: pytasz magistralę poleceń typu agregatu.

| Wiadomość | Pyta | Otrzymujesz |
|---|---|---|
| `GetAggregate(id)` | teraz agregat (rozdział 4) | `Try[Aggregate[R]]` |
| `GetEventsForAggregate(id)` | każde zdarzenie | `Try[List[EventInfo[R]]]` |
| `GetEventsForAggregateForVersion(id, version)` | zdarzenia do wersji | `Try[List[EventInfo[R]]]` |
| `GetAggregateForVersion(id, version)` | agregat taki, jaki był w danej wersji | `Try[Aggregate[R]]` |
| `GetAggregateAtInstant(id, instant)` | agregat taki, jaki był w danym momencie | `Try[Aggregate[R]]` |
| `GetAggregateMinVersion(id, version, maxMillis)` | agregat po osiągnięciu wersji | `Try[Aggregate[R]]` |

`R` to klasa stanu agregatu: `ShoppingCart` dla koszyków. Wszystkie sześć znajduje się w `io.reactivecqrs.api`.

> **Pamiętaj:** w przeciwieństwie do odpowiedzi na polecenia, te odpowiedzi to `scala.util.Try`. Odczyt, który się nie powiódł
> nadal pojawia się jako *udana* `Przyszłość`, trzymając w sobie `Porażkę`. Zawsze zaglądaj do środka `Try`.

W przykładach wykorzystano konfigurację z rozdziału 5 i wózek z zakupami z rozdziału 6. Jego historia, z jeszcze jednym
wydarzenie: po tym, jak Alicja cofnęła swoje rzeczy, sama zabrała pomarańcze.

```
wersja 1 ShoppingCartCreated(Groceries) Alice
wersja 2 Dodano element (jabłka) Alicja
wersja 3 Dodano element (pomarańcze) Bob
wersja 4 Element usunięty(1) Bob, przez pomyłkę
wersja 5 ShoppingCartChangesUndone(1) Alicja: jabłka wróciły
wersja 6 ItemRemoved(2) Alicja: jednak nie ma pomarańczy
```

Potrzebne są jeszcze dwa importy:

```scala
importuj java.time.Instant
import scala.util.{Niepowodzenie, Sukces, Próba}
```

## Cały dziennik: GetEventsForAggregate

```scala
(commandBus ? GetEventsForAggregate(cartId)).mapTo[Try[Lista[Informacje o zdarzeniu[Koszyk]]]].foreach {
  przypadek Sukces(wydarzenia) =>
    events.foreach(e => println(f"${e.version.asInt}%2d ${e.event.toString}%-30s ${e.metadata.actor}"))
  przypadek Błąd (błąd) =>
    println("Nie można odczytać koszyka: " + error.getMessage)
}
```

wydruki:

```
 1 Utworzono koszyk (artykuły spożywcze) użytkownik: 1
 2 Dodano elementy (jabłka) użytkownik: 1
 3 Dodano element (pomarańcze) użytkownik: 2
 4 elementyUsunięto(1) użytkownik:2
 5 cofniętych zmian w koszyku (1) użytkownik:1
 6 ElementówUsunięto(2) użytkownik:1
```

Każdy element `EventInfo` zawiera:

- `wersja`: wersja wydarzenia;
- `event`: samo wydarzenie;
- `userId`: kto wysłał polecenie, które je zapisało;
- `timestamp`: kiedy został zapisany;
- `metadane`: jego koperta z rozdziału 6 lub, w przypadku zdarzenia przechowywanego bez niej, koperta, której
  aktorem jest `"user:"` i identyfikator użytkownika (to jest to, co pokazuje ostatnia kolumna).

Co warto wiedzieć o tej liście:

- **To jest pełny dziennik.** Znajduje się w nim anulowane usunięcie (wersja 4), a także cofnięcie (wersja
  5) Nic w `EventInfo` nie oznacza, że ​​zdarzenie zostało anulowane; zdarzenia cofania informują, że część zdarzenia
     historia.
- **W przypadku duplikatu lista zaczyna się od wydarzeń w bazie** aż do skopiowanej wersji, ponumerowanej jako
  znajdują się w bazie, a za nimi następują wydarzenia duplikatu z wersji 1.
- **To przychodzi w jednej wiadomości.** To nie jest problem dla wózka, ale długa historia ma znaczenie: mierzona na
  17.10.2026 odczytanie wszystkich 50 001 zdarzeń z jednego agregatu zajęło 264 ms, a wielkość sterty osiągnęła szczyt 168 MB
  (pojedyncze uruchomienie na jednej maszynie programistycznej, [`docs/perf/load-M5.md`](../perf/load-M5.md)). Przez długi czas
  historie, zamiast tego przeczytaj wersję docelową.

## Część dziennika: GetEventsForAggregateForVersion

To samo, zatrzymując się na wersji:

```scala
commandBus ? GetEventsForAggregateForVersion(cartId, AggregateVersion(3))
// Success(List(...)) ze zdarzeniami wersji 1, 2 i 3
```

## Agregat w obecnej postaci: GetAggregateForVersion

Ten odtwarza stan, jaki istniał tuż po danej wersji:

```scala
(commandBus ? GetAggregateForVersion(cartId, AggregateVersion(4))).mapTo[Try[Aggregate[ShoppingCart]]].foreach(println)
```

Odpowiedź brzmi `Success(Aggregate(cartId, AggregateVersion(4), Some(ShoppingCart("Groceries",
Vector(Item(2, "pomarańcze")))))). W wersji 4 jabłka zniknęły: cofnięcie nastąpiło później, w wersji
5, więc odczyt wersji 4 pokazuje wózek dokładnie tak, jak zostawił go Bob. Poproś o wersję 5, a otrzymasz oba.
przedmioty z powrotem; poproś o wersję 6, a pozostaną tylko jabłka.

Co warto wiedzieć:

- **Wersja 0** odpowiada `Aggregate(cartId, AggregateVersion.ZERO, None)`: przed pierwszym zdarzeniem,
  nic nie istniało.
- **Wersja późniejsza niż najnowsza** daje najnowszą wersję. Sprawdź wersję w odpowiedzi, kiedy
  sprawy.
- **Wersja, w której usunięto agregat** (jego stan był `null`, rozdział 6) daje
  `Aggregate`, którego `aggregateRoot` ma wartość `None`.
- **W przypadku migawek** (rozdział 13) odczyt rozpoczyna się od migawki w wersji lub przed nią, gdy
  Jest jeden i odtwarza tylko wydarzenia następujące po nim. Odpowiedź jest ta sama; tylko pojawia się szybciej.

## Agregat w danym momencie: GetAggregateAtInstant

„Co znajdowało się w koszyku w momencie składania zamówienia, o godzinie 18:00 UTC, 16.10.2026?”

```scala
(commandBus ? GetAggregateAtInstant(cartId, Instant.parse("2026-10-16T18:00:00Z")))
  .mapTo[Try[Aggregate[ShoppingCart]]]
  .foreach {
    przypadek Success(cart) => println(s"W tym momencie: wersja ${cart.version.asInt}, ${cart.aggregateRoot}")
    przypadek Failure(error) => println("Nie można odczytać koszyka: " + error.getMessage)
  }
```

Biblioteka wyszukuje najnowszą wersję zapisaną w tym momencie lub wcześniej i odczytuje agregat w tym momencie.
ta wersja, z cofnięciami, jak wtedy: zdarzenie anulowane *po* chwili nadal się liczy. Więc
jeśli zamówienie zostało złożone pomiędzy wersjami 4 i 5, zobaczysz koszyk bez jabłek, czyli tego, co
klient widział.

Co warto wiedzieć:

- **Czasy są zapisanymi znacznikami czasu**: kiedy zdarzenia zostały zapisane, według zegara węzła
  które je zapisały; ten sam `znacznik czasu`, który otrzymują programy obsługi zdarzeń.
- **Chwila przed pierwszym zdarzeniem** podaje wersję 0 i brak stanu.
- **Migawki nie są używane** do natychmiastowego odczytu, ponieważ migawka nie rejestruje momentu, w którym jej wersja
  został osiągnięty. Przeczytaj całą historię aż do tego momentu.

> **Wskazówka:** w klastrze `GetAggregateAtInstant(id, Instant.now)` to wygodny sposób odczytu dużego stanu
> na węźle, na którym się znajdujesz: jest on tam przywracany ze sklepu, zamiast być wysyłany z
> węzeł, którego właściciel go posiada ([rozdział 17](17-cluster.md)).

## Czytanie własnych zapisów: GetAggregateMinVersion

Zamawiasz kawę i dostajesz paragon z numerem 42. Przy ladzie prosisz o numer 42, jeśli nie jest
Jeśli jesteś już gotowy, poczekaj chwilę, zamiast odejść z numerem 41.

Każde pomyślne polecenie podaje wersję, która została wygenerowana: `SuccessResponse(cartId,
AggregateVersion(7))`. Późniejszy odczyt powinien pokazać co najmniej tę wersję. Na jednym węźle tak się dzieje:
właściciel aktualizuje swój stan przed udzieleniem odpowiedzi na polecenie, więc polecenie `GetAggregate` jest wysyłane po otrzymaniu odpowiedzi
pokazuje zmianę. Ale nie zawsze można mieć pewność: czytelnik może nie być tym, kto wysłał polecenie,
polecenie mogło zostać wysłane bez czekania na odpowiedź lub zmiana może być jeszcze w drodze
(kolejny krok sagi, rozdział 12). `GetAggregateMinVersion` zamienia „przynajmniej wersję 7” w obietnicę:

```scala
(commandBus ? GetAggregateMinVersion(cartId, AggregateVersion(7), 2000)).mapTo[Try[Aggregate[ShoppingCart]]].foreach {
  przypadek Sukces (koszyk) =>
    println(s"Aktualna: wersja ${cart.version.asInt}") // wersja 7 lub nowsza
  przypadek Failure(e: AggregateInIncorrectVersionException) =>
    println(s"Nadal w wersji ${e.currentVersion.asInt}, czekam na ${e.requestedVersion.asInt}")
  przypadek Błąd (błąd) =>
    println("Nie można odczytać koszyka: " + error.getMessage)
}
```

- Gdy agregat osiągnie tę wersję lub nowszą, odpowiedź pojawi się natychmiast.
- W przeciwnym wypadku właściciel agregatu przechowuje pytanie i odpowiedzi w momencie zapisania danej wersji.
- Jeżeli wersja nie pojawi się w ciągu `maxMillis` milisekund (tutaj 2 sekund), odpowiedź brzmi
  `Failure(AggregateInIncorrectVersionException)`, który informuje o wersji agregatu i
  ten o który prosiłeś.

> **Pamiętaj:** Twój limit czasu na żądanie to osobny zegar. Ustaw go na dłużej niż `maxMillis`, albo
> `Future` może się nie powieść, zanim właściciel zdąży odpowiedzieć.

> **Wskazówka:** dobry nawyk dla interfejsów użytkownika: zachowaj wersję z odpowiedzi ostatniego polecenia i
> przekaż to do następnego czytania. Rozdział 8 zawiera podobne narzędzie do projekcji, które zawsze działają trochę
> za agregatami.

## Poniżej linii squasha

Długie historie można złożyć za pomocą *squash* ([rozdział 14](14-squash.md)): wydarzeń z
wersje od 1 do N są zastępowane pojedynczym zdarzeniem squasha w wersji N, która przechowuje zbudowany przez nich stan.
Wersja i stan agregatu pozostają takie same. Jego przeszłość jest jednak krótsza:

- `GetEventsForAggregate` rozpoczyna się od zdarzenia squash w wersji N;
- odczytuje wersję poniżej N (`GetAggregateForVersion`, `GetEventsForAggregateForVersion`) i odczytuje
  na chwilę przed czasem zdarzenia squasha odpowiedz `Failure(AggregateSquashedException)`.
  potrzebna im historia już nie istnieje, a biblioteka nie będzie tego zgadywać;
- odczyty wersji N i nowszych działają jak dotychczas.

```scala
przypadek Failure(e: AggregateSquashedException) =>
  println(s"Historia sprzed wersji ${e.squashedUpTo.asInt} została złożona")
```

Wyjątek określa również, o którą wersję prosiłeś (`requested`) i umożliwia natychmiastowe odczytanie:
która chwila (`w`).

### Archiwum

Niektóre wydarzenia mają znaczenie dla audytów nawet po zgnieceniu: kto zatwierdził zamówienie, kto co dodał. Zgniecenie
polityka może nadać nazwę klasom zdarzeń, które mają zostać zachowane, a następnie program Squash przeniesie te zdarzenia do archiwum
zamiast je usuwać. `GetArchivedEvents(id)` odczytuje to:

```scala
(commandBus ? GetArchivedEvents(cartId)).mapTo[Try[ArchivedEvents[ShoppingCart]]].foreach {
  przypadek Sukces (archiwum) =>
    archive.events.foreach(a => println(s"${a.info.version.asInt} ${a.info.event} zarchiwizowano w ${a.archived}"))
  przypadek Błąd (błąd) =>
    println("Nie można odczytać archiwum: " + error.getMessage)
}
```

Zdarzenia są wyświetlane od najstarszych. Każde „ArchivedEvent” zawiera „EventInfo” (`info`) zdarzenia, wersję
cofnięcia, które anulowało operację, jeśli tak się stało (`undoneFrom`), oraz daty archiwizacji (`archived`).
Agregat bez niczego zarchiwizowanego, `wydarzenia` są po prostu puste. Przywraca, dostarcza ponownie, odbudowuje i
Inne historie nigdy nie czytają archiwum: istnieje ono po to, by je przeglądać. Rozdział 14 pokazuje, jak wybierać
co się w tym kryje.

## Gdy odczyt się nie powiedzie

Warto rozróżnić dwie porażki:

- `Failure(AggregateSquashedException)`: wcale nie jest to błąd, a jedynie fakt historyczny (powyżej).
- `Failure(AggregateRestoreFailedException)`: nie można odczytać agregatu. Przyczyna leży w:
  zwykle jest to zapisane zdarzenie, którego kod nie może już odczytać (zmieniona lub brakująca klasa zdarzenia,
  rozdział 6) lub problem z bazą danych. Biblioteka również to loguje, podając identyfikator agregatu.

## Gdzie odbywają się odczyty

Odczyty historyczne (`GetEventsForAggregate`, `GetEventsForAggregateForVersion`,
`GetAggregateForVersion`, `GetAggregateAtInstant` i `GetArchivedEvents`) nie obejmują
Właściciel agregatu w ogóle. Magistrala poleceń odczytuje magazyn w tle, na węźle, o który pytałeś,
w puli wątków biblioteki do pracy z bazą danych. Dzięki temu nie budzą uśpionego agregatu, nie
poczekaj na jego polecenia i zobacz dokładnie, co zostało zatwierdzone.

`GetAggregate` i `GetAggregateMinVersion` różnią się: właściciel agregatu na nie odpowiada,
rozpoczynając je, jeśli zajdzie taka potrzeba, ponieważ dotyczą one teraźniejszości zbiorczej.

> **Wskazówka:** te odczyty dotyczą pojedynczego agregatu na raz, co jest przydatne na ekranach audytu: „co się zmieniło?”
> panele, narzędzia wsparcia i debugowanie. Pytania dotyczące wielu agregatów („każdy koszyk z pomarańczami w
> to") należy do projekcji: to jest następny rozdział.

> **Pod maską:** historyczne odczyty stanów i zdarzeń przechodzą przez moduł przywracania,
> `AggregateRestorer`, który ukrywa łańcuchy duplikacji, niewykonane zdarzenia i migawki; używa go właściciel
> również, aby odczytać jego agregat podczas uruchamiania. Archiwum jest odczytywane bezpośrednio z tabeli. Zobacz
> [ARCHITECTURE.md, sekcja 4.3](../ARCHITECTURE.md).

## Czego się nauczyłeś

- Szyna poleceń odpowiada na pytania dotyczące przeszłości agregatu, a odpowiedziami są `Try`, więc
  zawsze patrz do środka.
- `GetEventsForAggregate` zwraca cały dziennik jako `EventInfo` z wersją, użytkownikiem, czasem i
  metadane; `GetEventsForAggregateForVersion` zatrzymuje się na wersji.
- `GetAggregateForVersion` i `GetAggregateAtInstant` odbudowują stan, jaki był w danej wersji lub
  chwili, z tym że wówczas zdarzały się cofnięcia.
- `GetAggregateMinVersion` czeka, aż agregat osiągnie wersję lub zakończy się niepowodzeniem
  `AggregateInIncorrectVersionException` po `maxMillis`: narzędzie do odczytywania własnych zapisów.
- Poniżej punktu squasha odczyt historii kończy się niepowodzeniem z wyjątkiem `AggregateSquashedException`; zdarzenia przechowywane przez
  zasady squasha pozostają czytelne dzięki `GetArchivedEvents`.
- Historyczne odczyty pochodzą ze sklepu, z węzła pytającego, bez budzenia właściciela agregatu.

Następnie: [rozdział 8](08-projections.md) przekształca zdarzenia w widoki, które ludzie mogą przeszukiwać.