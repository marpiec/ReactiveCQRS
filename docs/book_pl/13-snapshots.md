# Rozdział 13. Utrzymywanie szybkości agregatów za pomocą migawek

Pod koniec tego rozdziału będziesz wiedział, dlaczego agregat z długą historią budzi się powoli,
jak jeden wiersz w kontekście sprawia, że ​​jest on znów szybki, gdy biblioteka wyrzuca migawkę i
Co powstrzymuje agregat przed nieustannym wzrostem? Migawki są najdelikatniejszą cechą w tej części
książka: nie zmieniają niczego w historii i możesz je wyłączyć w dowolnym momencie.

---

## Dlaczego długie historie stają się powolne

Powrót do dziennika z rozdziału 1. Aby poznać aktualną listę zakupów, przeczytaj dziennik od góry
i odtworzyć każdy wers. Za jedną stronę, która zajmie chwilę. Za pamiętnik tysiąca stron, będziesz
najpierw chcę filiżankę kawy.

Właściciel agregatu robi dokładnie to samo. Podczas gdy agregat jest zajęty, jego właściciel utrzymuje stan w
pamięć, a polecenia są szybkie. Ale za każdym razem, gdy właściciel uruchamia, **przywraca** agregat,
odtwarzając każde z wydarzeń:

- w pierwszej wiadomości dla agregatu po uruchomieniu aplikacji;
- po zatrzymaniu właściciela z powodu bezczynności, aby zwolnić pamięć: domyślnie węzeł zatrzymuje właścicieli
  poza 200 ostatnio używanymi, gdy są bezczynne przez 2 minuty, a każdy właściciel jest bezczynny przez
  12 godzin;
- po przekazaniu do innego węzła klastra (rozdział 17);
- po tym jak coś poszło nie tak i stan trzeba było odczytać ponownie ze sklepu.

Koszt rośnie wraz z historią. Pomiar z dnia 17.10.2026 r. (obciążenie ławki – bieg M5, jeden bieg na jednym
maszyna: Windows 10, 12 wątków procesora, OpenJDK 17 z pamięcią 2 GB, PostgreSQL 17.4 na tym samym komputerze
maszyna), nowy właściciel przywrócił łącznie 50 000 zdarzeń, każde z notatką składającą się z 200 znaków,
392 ms, a w międzyczasie rozmiar stosu wzrósł z 17 do 52 MB. Polecenie po przywróceniu zajęło 3 ms.

Dzięki temu polecenia nie stają się wolniejsze wraz ze wzrostem historii: po przywróceniu właściciel zachowuje stan, a nie
Wydarzenia. To przebudzenie staje się wolniejsze. I chociaż właściciel odnawia, to wciąż jest jeden z
blokowanie 32 wątków dyspozytora i połączenia z bazą danych, dzięki czemu każdy inny właściciel węzła czeka
jeszcze trochę dłużej.

## Zakładka z notatką

Teraz wyobraź sobie zakładkę w dzienniku na stronie 200 z karteczką samoprzylepną: „w tym momencie lista jest
chleb, jajka i kawa”. Następnym razem, gdy otworzysz pamiętnik na zakładce, przeczytaj notatkę i odtwórz ją.
tylko linijki po nim.

To jest **migawka**: stan agregatu w danej wersji, zapisany tak, aby można było rozpocząć przywracanie
i przeczytaj tylko wydarzenia następujące po nim.

Dwie rzeczy, których migawka nie jest:

- **Nie jest częścią historii.** Wydarzenia pozostają prawdą. Wyrzuć każdą migawkę, a nic nie zostanie.
  Utracone: przywraca tylko odczyt wszystkich zdarzeń. Dlatego migawka nazywa się *jednorazowa*.
- **Nie jest to squash.** squash (rozdział 14) zastępuje stare wydarzenia jednym wydarzeniem. Migawka zmienia
  nic w historii i nic w wersjach.

## Włączanie migawek

Jeden wiersz w kontekście:

```scala
import io.reactivecqrs.api.SnapshotPolicy

klasa ShoppingCartAggregateContext rozszerza AggregateContext[ShoppingCart] {
  // ... handlerów i reszty, jak w rozdziale 3

  nadpisanie migawek val: Option[SnapshotPolicy] = Some(SnapshotPolicy())
}
```

`snapshots` domyślnie ma wartość `None`. `SnapshotPolicy` ma trzy parametry, wszystkie z wartościami domyślnymi:

| Parametr | Domyślny | Znaczenie |
| --- | --- | --- |
| `every` | `200` | Właściciel zapisuje migawkę zaraz po zatwierdzeniu, która przenosi wersję agregatu do następnej wielokrotności `every`: 200, 400, 600 itd. |
| `onPassivation` | `true` | Właściciel zapisuje go również wtedy, gdy agregat jest zatrzymany (pasywowany), jeśli od ostatniej migawki nastąpił ruch. |
| `schemaVersion` | `1` | Wywołuje wyjątek, gdy klasa stanu zmienia kształt (patrz poniżej). |

Kilka rzeczy, które warto wiedzieć o pisaniu:

- **Jedna migawka na agregat.** Najnowsza zastępuje poprzednią; tabela `snapshots` przechowuje jedną migawkę.
  wiersz na agregat.
- **Najważniejsza jest migawka pasywacji.** Właściciel zostaje zatrzymany, gdy jest bezczynny, gdy jest przekazywany
  do innego węzła lub gdy zapytasz `PassivateAggregate`. Następny właściciel przywróci prawa
  z tej migawki: często nie ma w ogóle zdarzeń do odczytania.
- **Zapis nigdy nie przeszkadza.** Migawka przechodzi w krótką transakcję po
  Zatwierdzenie polecenia. Jeśli się nie powiedzie, zostanie ono tylko zarejestrowane, a późniejsze przywrócenie po prostu odczyta więcej zdarzeń.

> **Wskazówka:** `SnapshotSpec` włącza migawki dla koszyków z `SnapshotPolicy(every = 3)`, więc
> migawka pojawia się po kilku poleceniach. Użyj małego `every`, takiego jak to, aby je obejrzeć w teście;
> zachowaj ustawienia domyślne w produkcji.

## Oglądanie jak to działa

Mały eksperyment z `SnapshotPolicy(every = 3)` w kontekście koszyka:

1. Utwórz koszyk za pomocą `CreateShoppingCart`: wersja 1.
2. Dodaj dwa elementy za pomocą `AddItem`: wersje 2 i 3. Zatwierdzenie, które osiągnie wersję 3, również zapisuje
   migawka w wersji 3.
3. Spójrz na tabelę: `SELECT aggregate_id, version FROM snapshots` pokazuje jeden wiersz dla koszyka,
   w wersji 3.
4. Uśpij właściciela:

   ```scala
   import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate

   wózki ! PassivateAggregate(cartId)
   ```

   Agregat nie uległ zmianie od wersji 3, dlatego nie jest tworzona żadna nowa migawka.
5. Wyślij kolejne polecenie. Nowy właściciel uruchamia migawkę, odczytuje ją i nie znajduje po niej żadnego zdarzenia.
   powtórna rozgrywka.

W produkcji można obserwować to samo za pomocą metryk (rozdział 18): każde przywracanie jest mierzone czasowo
jako `reactivecqrs.restore`, ze znacznikiem `from` ustawionym na `snapshot` lub `events`.

## Co przywracanie robi z migawką

Przywracanie wykorzystuje migawkę agregatu, gdy:

- zostało napisane w aktualnej `wersji` kontekstu;
- został napisany przy użyciu aktualnej wersji polityki `schemaVersion`;
- w przypadku starszej wersji, jest ona równa lub starsza od tej wersji.

Następnie odczytuje tylko zdarzenia agregatu po migawce. W przeciwnym razie (brak migawki, jedno z drugiego)
wersja, której nie da się odczytać) odczytuje każde zdarzenie dokładnie tak samo, jak bez migawek. Migawka
nigdy nie powoduje niepowodzenia przywracania; przywracanie, którego nie można odczytać, jest rejestrowane jako ostrzeżenie i pomijane.

Te same zasady obowiązują także poza granicami właściciela:

- **Odczyty z przeszłości** (rozdział 7): `GetAggregateForVersion` może rozpocząć się od migawki w wersji lub niższej
  wersja, o którą prosiłeś. `GetAggregateAtInstant` nigdy jej nie używa, ponieważ migawka nie rejestruje
  kiedy osiągnięto jego wersję.
- **Kompilacje projekcji** (rozdział 10) przywracają potrzebne stany w ten sam sposób, dzięki czemu migawki stają się szybsze
  przebudować projekcje, które również otrzymują państwa.

`SnapshotSpec` sprawdza, czy jest to niewidoczne: w przypadku losowych historii koszyków z funkcją cofania i duplikowania,
Przywracanie za pomocą migawek daje ten sam stan, co pełne odtworzenie, w każdej wersji. Przywraca również
koszyk 100 000 zdarzeń z jednej migawki bez odczytywania ani jednego zdarzenia, a po kolejnych 150 zdarzeniach
odczytuje tylko te 150.

## Kiedy migawki są wyrzucane

Historia może się zmienić w ramach migawki, a wtedy migawka będzie leżeć. Tak więc każda zmiana, która
można popełnić jeden błąd usuwając go w ramach własnej transakcji zmiany:

| Zmiana | Usunięto migawki |
| --- | --- |
| cofnięcie (rozdział 6) | agregat |
| przepisanie historii (rozdział 15) | całości i jej duplikatów |
| squash (rozdział 14) | agregatu, gdy znajduje się w punkcie squasha lub poniżej; ze zdarzeniem squasha, które może zmienić stan, agregatu i duplikatów, które go odczytują |
| trwałe usunięcie (rozdział 6) | agregatu |

Dwie inne zmiany pozostawiają migawkę w tabeli, ale czynią ją bezużyteczną: nowa „wersja”
kontekst i nową „wersję schematu” polityki. Stara migawka jest następnie ignorowana, a
Następna migawka agregatu zastępuje go.

> **Pod maską:** każda z tych zmian powoduje również *generowanie historii* agregatu,
> licznik w tabeli `aggregates`. Przywrócenie odczytuje generację sprzed historii, a
> migawka jest zapisywana tylko wtedy, gdy agregat jest przechowywany w wersji migawki lub nowszej i w
> pokolenie, w którym odczytano jego stan. Zatem właściciel nadal posiada stan sprzed zmiany
> nie można tego zapisać jako migawki (D30, F119). Pełna historia:
> [ARCHITECTURE.md, sekcja 4.3](../ARCHITECTURE.md).

## Zmiana klasy stanu lub obsługi zdarzeń

Migawka przechowuje stan jako JSON Twojej zagregowanej klasy głównej, tutaj „ShoppingCart”. Dwa rodzaje
zmiana powoduje, że stare migawki stają się błędne bez wiedzy biblioteki:

- **Klasa główna zmienia kształt** (pole zostaje dodane, usunięte lub nazwane): stara migawka może nie zostać odczytana
  z powrotem jako nowa klasa. Wywołaj `schemaVersion`, a stare migawki zostaną zignorowane:

  ```scala
  nadpisanie migawek val: Option[SnapshotPolicy] = Some(SnapshotPolicy(schemaVersion = 2))
  ```

- **Obsługa zdarzeń zmienia sposób, w jaki przetwarza zdarzenia**: stare migawki przechowują stany utworzone przez
  stary handler. Podnieś `wersję` kontekstu. To również sprawi, że stare migawki staną się bezużyteczne i
  odbudowuje zarządzane projekcje, które odbierają stany agregatu (rozdział 10), które zawierają
  stany dokonane również przez starego handlera.

> **Ostrzeżenie:** zapomnienie o podniesieniu któregokolwiek z nich powoduje przywrócenie stanu, w którym obecny kod by się znajdował
> nie tworzyć. Nic nie zawodzi: agregat jest po cichu błędny, a jego kolejna migawka przenosi błąd
> wł. W razie wątpliwości podnieś `schemaVersion`: kosztuje to jedno pełne odtworzenie na agregat, raz.

## Jak długa może być historia?

Agregat nie może rosnąć w nieskończoność. Magazyn zdarzeń odmawia przechowywania zdarzeń po
`aggregateVersionLimit`, domyślnie 10 000. Polecenie, którego zdarzenia przekroczyłyby limit, kończy się niepowodzeniem:
Wywołujący otrzymuje błąd `EventHandlingError`, a log nazywa `TooManyEventsException`. Limit jest wartownikiem
przed niekontrolowanymi agregatami, takimi jak błąd dodający zdarzenie w pętli lub jeden agregat, który powinien
Tak naprawdę było ich kilka.

Jest to parametr magazynu zdarzeń, więc dotyczy każdego typu agregatu przechowywanego w magazynie.
pozwól na dłuższe historie, zbuduj sklep z większą historią:

```scala
val eventStore = new PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000).initSchema()
```

(`SnapshotSpec` używa `aggregateVersionLimit = 200000` dla swojego koszyka zawierającego 100 000 zdarzeń.)

> **Pamiętaj:** limit obejmuje wersje, a squash nie obniża wersji (rozdział 14).
> zgnieciony agregat nadal zatrzymuje się na granicy. Migawki pozwalają na szybkie odtworzenie długiej historii;
> nie pozwól na to.

## Kiedy je włączyć

`docs/SCALING.md` (sekcja 3) zaleca migawki dla każdego typu, którego agregaty rosną powyżej kilku
Tysiąc zdarzeń. Pomiary pokazują dlaczego. Czasy przywracania własnego agregatu ławki, zdarzenia
Notatka 200-znakowa, zmierzona 17.10.2026 r. w przebiegu „baseline-M4” (jeden przebieg na maszynie o nazwie
powyżej, z zniesionym limitem na jednostkę łączną):

| Wydarzenia | Pełna powtórka | Z migawki |
| --- | --- | --- |
| 1 000 | 16 ms | 5 ms |
| 10 000 | 58 ms | 2 ms |
| 100 000 | 356 ms | 2 ms |
| 500 000 | 2 088 ms | 1 ms |

`load-M5` tego samego dnia daje 392 ms na pełne odtworzenie 50 000 zdarzeń w porównaniu z 12 ms od
Migawka. To pojedyncze przebiegi na jednej maszynie: dobre do porównania, nie są to dane dotyczące pojemności dla Twojej maszyny.

Na podstawie tych liczb `docs/SCALING.md` szacuje, że jeden węzeł może odtworzyć około 80 agregatów
50 000 zdarzeń na sekundę bez migawek i około 2 700 z nimi (szacunkowo, nie mierzono: 32
wątków podzielonych przez 0,392 s i przez 0,012 s). W przypadku krótkich agregatów wystarczy kilka milisekund.
ratować.

Ile kosztują migawki:

- jeden dodatkowy zapis co każde zdarzenie `every` i jeden, gdy właściciel zostanie zatrzymany;
- jeden wiersz na agregat, przechowujący cały stan w formacie JSON. Duży stan tworzy dużą migawkę: zachowaj
  duże dokumenty z zbiorczych stanów (rozdział 18).

A co pomaga oprócz migawek: zaprojektuj agregaty tak, aby były krótkie (podziel je naturalnym
granica, np. jedna na miesiąc lub jedna na wersję dokumentu), zmiażdż starą historię (rozdział 14),
i nigdy nie proś od razu o całą historię długiego zbioru (rozdział 18).

## Czego się nauczyłeś

- Właściciel przywraca swój agregat, odtwarzając każde zdarzenie za każdym razem, gdy się ono zaczyna; przywracane dane rosną wraz z
  historia, polecenia nie.
- Migawka to stan w danej wersji; przywrócenie odczytuje ją i tylko zdarzenia następujące po niej.
  jednorazowe i nigdy nie będące częścią historii.
- `override val snapshots = Some(SnapshotPolicy())` włącza je: migawka co 200 zdarzeń i
  jeden, gdy właściciel zostaje zatrzymany.
- Cofnij, przepisz historię, zmiksuj i usuń trwale migawki, które zostały wykonane nieprawidłowo; nowa
  kontekst `version` lub `schemaVersion` powoduje ignorowanie starych.
- Podnieś `schemaVersion`, gdy klasa stanu zmienia kształt, a `version` kontekstu, gdy
  Obsługa zdarzeń zmienia sposób, w jaki zdarzenia są traktowane.
- `aggregateVersionLimit` (domyślnie 10 000) ogranicza każdą wersję agregatu, a squash nie
  obniż to.

Następnie: [rozdział 14](14-squash.md) raz na zawsze odrzuca starą historię.