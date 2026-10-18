# Rozdział 13. Szybkie agregaty dzięki migawkom

Pod koniec tego rozdziału będziesz wiedzieć, dlaczego agregat z długą historią powoli się budzi, jak jedna
linijka w jego kontekście znów go przyspiesza, kiedy biblioteka wyrzuca migawkę i co powstrzymuje agregat
przed rośnięciem bez końca. Migawki to najłagodniejsza funkcja w tej części książki: niczego nie zmieniają
w twojej historii i w każdej chwili możesz je z powrotem wyłączyć.

---

## Dlaczego długa historia spowalnia

Wróćmy do pamiętnika z rozdziału 1. Żeby poznać aktualną listę zakupów, czytasz pamiętnik od początku
i przechodzisz przez każdą linijkę. Przy jednej stronie to chwila. Przy pamiętniku na tysiąc stron najpierw
przyda się kubek kawy.

Właściciel agregatu robi dokładnie to samo. Dopóki agregat jest w użyciu, jego właściciel trzyma stan
w pamięci i polecenia są szybkie. Ale za każdym razem, gdy właściciel startuje, **odtwarza** agregat,
stosując po kolei każde z jego zdarzeń:

- przy pierwszym komunikacie dla agregatu po starcie aplikacji;
- po tym, jak właściciel został zatrzymany z powodu bezczynności, żeby zwolnić pamięć: domyślnie węzeł
  zatrzymuje właścicieli spoza 200 ostatnio używanych, gdy są bezczynni od 2 minut, oraz każdego
  właściciela bezczynnego od 12 godzin;
- po przekazaniu agregatu innemu węzłowi klastra (rozdział 17);
- gdy coś poszło nie tak i stan trzeba było ponownie odczytać z magazynu.

Koszt rośnie razem z historią. W pomiarze z 17/10/2026 (przebieg `load-M5` benchmarku, jedno uruchomienie
na jednej maszynie: Windows 10, 12 wątków CPU, OpenJDK 17 ze stertą 2 GB, PostgreSQL 17.4 na tej samej
maszynie) nowy właściciel odtwarzał agregat z 50 000 zdarzeń, każde z 200-znakową notatką, przez 392 ms,
a sterta urosła w tym czasie z 17 do 52 MB. Polecenie po odtworzeniu trwało 3 ms.

Polecenia nie zwalniają więc wraz ze wzrostem historii: po odtworzeniu właściciel trzyma stan, a nie
zdarzenia. Wolniejsze robi się samo budzenie. A gdy właściciel odtwarza agregat, zajmuje jeden z 32 wątków
dispatchera blokującego i połączenie z bazą danych, więc każdy inny właściciel na tym węźle też czeka trochę
dłużej.

## Zakładka z karteczką

Teraz wyobraź sobie zakładkę w pamiętniku na stronie 200 z przyklejoną karteczką: „w tym miejscu lista to
chleb, jajka i kawa”. Następnym razem otwierasz pamiętnik na zakładce, czytasz karteczkę i przechodzisz tylko
przez linijki po niej.

To jest **migawka**: stan agregatu w danej wersji, zapisany po to, żeby odtwarzanie mogło od niego zacząć
i przeczytać tylko zdarzenia po nim.

Są dwie rzeczy, którymi migawka nie jest:

- **Nie jest częścią historii.** Prawdą pozostają zdarzenia. Wyrzuć wszystkie migawki, a nic nie przepadnie:
  odtwarzanie po prostu znów przeczyta wszystkie zdarzenia. Dlatego mówi się, że migawka jest
  *do wyrzucenia*.
- **Nie jest squashem.** Squash (rozdział 14) zastępuje stare zdarzenia jednym zdarzeniem. Migawka nie
  zmienia niczego w historii ani w wersjach.

## Włączanie migawek

Wystarczy jedna linijka w kontekście; tutaj w kontekście koszyków, który rozszerza kontekst z domeny
testowej:

<!-- snippet: cart/SnapshotExperimentSpec.scala#policy -->
```scala
import io.reactivecqrs.api.SnapshotPolicy

class SnapshottedCarts extends ShoppingCartAggregateContext {
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy())
}
```

Domyślnie `snapshots` to `None`. `SnapshotPolicy` ma trzy parametry, wszystkie z wartościami domyślnymi:

| Parametr | Domyślnie | Znaczenie |
| --- | --- | --- |
| `every` | `200` | Właściciel zapisuje migawkę zaraz po zatwierdzeniu, które doprowadza wersję agregatu do kolejnej wielokrotności `every`: przy 200, 400, 600 i tak dalej. |
| `onPassivation` | `true` | Właściciel zapisuje migawkę także wtedy, gdy zostaje zatrzymany (pasywacja), jeśli agregat zmienił się od ostatniej migawki. |
| `schemaVersion` | `1` | Podnieś, gdy klasa stanu zmienia kształt (zobacz niżej). |

Kilka rzeczy, które warto wiedzieć o zapisywaniu migawek:

- **Jedna migawka na agregat.** Najnowsza zastępuje poprzednią; tabela `snapshots` ma jeden wiersz na
  agregat.
- **Najważniejsza jest migawka przy pasywacji.** Właściciel jest zatrzymywany, gdy był bezczynny, gdy
  przekazuje się go innemu węzłowi albo gdy poprosisz o to komunikatem `PassivateAggregate`. Następny
  właściciel odtwarza stan prosto z tej migawki: często nie ma do przeczytania żadnych zdarzeń.
- **Zapis nigdy nie przeszkadza.** Migawka trafia do bazy we własnej, krótkiej transakcji, po zatwierdzeniu
  polecenia. Jeśli zapis się nie uda, zostaje tylko odnotowany w logu, a późniejsze odtwarzanie po prostu
  przeczyta więcej zdarzeń.

> **Wskazówka:** `SnapshotSpec` włącza migawki dla koszyków przez `SnapshotPolicy(every = 3)`, więc migawka
> pojawia się już po kilku poleceniach. Takiego małego `every` używaj, gdy chcesz obserwować migawki
> w teście; na produkcji zostaw wartość domyślną.

## Zobaczmy to w działaniu

Mały eksperyment z `SnapshotPolicy(every = 3)` w kontekście koszyka. `SnapshotExperimentSpec`
(`examples/src/test/scala/io/reactivecqrs/examples/cart/`) przeprowadza go krok po kroku:

1. Utwórz koszyk przez `CreateShoppingCart`: wersja 1.
2. Dodaj dwie pozycje przez `AddItem`: wersje 2 i 3. Zatwierdzenie, które dochodzi do wersji 3, zapisuje też
   migawkę w wersji 3.
3. Zajrzyj do tabeli: `SELECT aggregate_id, version FROM snapshots` pokazuje jeden wiersz dla koszyka,
   w wersji 3.
4. Uśpij właściciela:

   <!-- snippet: cart/SnapshotExperimentSpec.scala#passivate -->
   ```scala
   import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate

   carts ! PassivateAggregate(cartId)
   ```

   Agregat nie zmienił się od wersji 3, więc nowa migawka nie jest zapisywana.
5. Wyślij kolejne polecenie. Startuje nowy właściciel, czyta migawkę i nie znajduje po niej żadnego zdarzenia
   do odtworzenia.

Na produkcji to samo widać w metrykach (rozdział 18): czas każdego odtwarzania jest mierzony jako
`reactivecqrs.restore`, z tagiem `from` ustawionym na `snapshot` albo `events`.

## Co odtwarzanie robi z migawką

Odtwarzanie korzysta z migawki agregatu, gdy:

- migawkę zapisano przy obecnej wartości `version` kontekstu;
- migawkę zapisano przy obecnej wartości `schemaVersion` polityki;
- przy odczycie starszej wersji migawka leży w tej wersji lub przed nią.

Wtedy czyta tylko zdarzenia agregatu po migawce. W przeciwnym razie (brak migawki, migawka z innej wersji,
migawka, której nie da się odczytać) czyta wszystkie zdarzenia, dokładnie tak jak bez migawek. Migawka
nigdy nie sprawi, że odtwarzanie się nie uda: jeśli nie da się jej odczytać, trafia to do logu jako
ostrzeżenie, a migawka zostaje pominięta.

Te same zasady obowiązują także poza właścicielem:

- **Odczyty przeszłości** (rozdział 7): `GetAggregateForVersion` może zacząć od migawki w żądanej wersji lub
  poniżej niej. `GetAggregateAtInstant` nigdy z migawek nie korzysta, bo migawka nie zapamiętuje, kiedy
  agregat osiągnął jej wersję.
- **Budowy projekcji** (rozdział 10) odtwarzają potrzebne stany w ten sam sposób, więc migawki przyspieszają
  też przebudowę projekcji, które dostają stany.

`SnapshotSpec` sprawdza, że nic z tego nie widać z zewnątrz: dla losowych historii koszyków z cofnięciami
i duplikacją odtwarzanie z migawkami daje w każdej wersji ten sam stan co pełne odtworzenie. Odtwarza też
koszyk ze 100 000 zdarzeń z jednej migawki, nie czytając ani jednego zdarzenia, a po 150 kolejnych
zdarzeniach czyta tylko te 150.

## Kiedy migawki są wyrzucane

Historia może się zmienić pod migawką, a wtedy migawka by kłamała. Dlatego każda zmiana, która mogłaby
uczynić migawkę błędną, usuwa ją w swojej własnej transakcji:

| Zmiana | Usuwane migawki |
| --- | --- |
| cofnięcie (rozdział 6) | migawka agregatu |
| przepisanie historii (rozdział 15) | migawka agregatu i migawki jego duplikatów |
| squash (rozdział 14) | migawka agregatu, gdy leży w punkcie squasha lub poniżej; przy zdarzeniu squasha, które może zmienić stan, migawka agregatu i migawki duplikatów, które go czytają |
| trwałe usunięcie (rozdział 6) | migawka agregatu |

Dwie inne zmiany zostawiają migawkę w tabeli, ale czynią ją bezużyteczną: nowa wartość `version` kontekstu
i nowa wartość `schemaVersion` polityki. Stara migawka jest wtedy pomijana, a następna migawka agregatu ją
zastępuje.

> **Pod maską:** każda z tych zmian podnosi też *generację historii* agregatu, czyli licznik w tabeli
> `aggregates`. Odtwarzanie czyta generację przed historią, a migawka jest zapisywana tylko wtedy, gdy
> agregat w magazynie jest w wersji migawki lub późniejszej i ma tę generację, przy której odczytano jego
> stan. Właściciel, który wciąż trzyma stan sprzed zmiany, nie może więc zapisać go z powrotem jako migawki
> (D30, F119). Pełny opis: [ARCHITECTURE.md, sekcja 4.3](../ARCHITECTURE.md).

## Zmiana klasy stanu albo handlerów zdarzeń

Migawka przechowuje stan jako JSON klasy korzenia twojego agregatu, tutaj `ShoppingCart`. Dwa rodzaje zmian
psują stare migawki tak, że biblioteka tego nie zauważy:

- **Klasa korzenia zmienia kształt** (pole dodane, usunięte albo przemianowane): starej migawki może nie dać
  się odczytać jako nowej klasy. Podnieś `schemaVersion`, a stare migawki będą pomijane:

  <!-- snippet: cart/SnapshotExperimentSpec.scala#schemaVersion -->
  ```scala
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(schemaVersion = 2))
  ```

- **Handler zdarzenia zmienia to, co robi ze zdarzeń**: stare migawki trzymają stany zrobione przez stary
  handler. Podnieś `version` kontekstu. To również czyni stare migawki bezużytecznymi, a do tego
  przebudowuje projekcje zarządzane, które dostają stany agregatu (rozdział 10), bo i one trzymają stany
  zrobione przez stary handler.

> **Ostrzeżenie:** jeśli zapomnisz podnieść którąś z tych wartości, odtwarzanie zacznie od stanu, którego
> obecny kod by nie zrobił. Nic się nie wywróci: agregat będzie po cichu błędny, a jego następna migawka
> poniesie błąd dalej. W razie wątpliwości podnieś `schemaVersion`: kosztuje to jedno pełne odtworzenie na
> agregat, jednorazowo.

## Jak długa może być historia?

Agregat nie może rosnąć w nieskończoność. Magazyn zdarzeń odmawia zapisania zdarzenia ponad
`aggregateVersionLimit`, domyślnie 10 000. Polecenie, którego zdarzenia przekroczyłyby ten limit, kończy się
niepowodzeniem: wywołujący dostaje `EventHandlingError`, a w logu pojawia się `TooManyEventsException`.
Limit chroni przed agregatami, które wymknęły się spod kontroli: na przykład przed błędem, który w pętli
dodaje zdarzenia, albo przed jednym agregatem, który tak naprawdę powinien być kilkoma.

To parametr magazynu zdarzeń, więc dotyczy każdego typu agregatu, który ten magazyn przechowuje. Żeby
pozwolić na dłuższe historie, utwórz magazyn z większym limitem:

<!-- snippet: cart/SnapshotExperimentSpec.scala#limit -->
```scala
val eventStore = new PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000).initSchema()
```

(`SnapshotSpec` używa `aggregateVersionLimit = 200000` dla swojego koszyka ze 100 000 zdarzeń.)

> **Pamiętaj:** limit liczy wersje, a squash nie obniża wersji (rozdział 14). Agregat po squashu też
> zatrzyma się na limicie. Migawki sprawiają, że długą historię szybko się odtwarza; nie sprawiają, że staje
> się dozwolona.

## Kiedy je włączać

`docs/SCALING.md` (sekcja 3) zaleca migawki dla każdego typu, którego agregaty przekraczają kilka tysięcy
zdarzeń. Pomiary pokazują dlaczego. Oto czasy odtwarzania własnego agregatu benchmarku, ze zdarzeniami
niosącymi 200-znakową notatkę, zmierzone 17/10/2026 w przebiegu `baseline-M4` (jedno uruchomienie na
opisanej wyżej maszynie, ze zniesionym limitem zdarzeń na agregat):

| Zdarzenia | Pełne odtworzenie | Z migawki |
| --- | --- | --- |
| 1 000 | 16 ms | 5 ms |
| 10 000 | 58 ms | 2 ms |
| 100 000 | 356 ms | 2 ms |
| 500 000 | 2 088 ms | 1 ms |

`load-M5` z tego samego dnia daje 392 ms dla pełnego odtworzenia 50 000 zdarzeń wobec 12 ms z migawki. To
pojedyncze uruchomienia na jednej maszynie: dobre do porównań, ale nie mówią, ile udźwignie twoja maszyna.

Na podstawie tych liczb `docs/SCALING.md` szacuje, że jeden węzeł może odtworzyć na sekundę około 80
agregatów po 50 000 zdarzeń bez migawek i około 2 700 z migawkami (oszacowane, nie zmierzone: 32 wątki
podzielone przez 0,392 s i przez 0,012 s). Przy krótkich agregatach oszczędzasz zaledwie kilka milisekund.

Ile kosztują migawki:

- jeden dodatkowy zapis co `every` zdarzeń i jeden przy zatrzymaniu właściciela;
- jeden wiersz na agregat, z całym jego stanem jako JSON. Duży stan to duża migawka: nie trzymaj dużych
  dokumentów w stanach agregatów (rozdział 18).

A co pomaga poza migawkami: projektuj agregaty tak, by były krótkie (dziel je wzdłuż naturalnej granicy, na
przykład jeden na miesiąc albo jeden na wersję dokumentu), zwijaj starą historię squashem (rozdział 14)
i nigdy nie proś naraz o całą historię długiego agregatu (rozdział 18).

## Co już umiesz

- Właściciel przy każdym starcie odtwarza swój agregat, stosując wszystkie jego zdarzenia; czas odtwarzania
  rośnie razem z historią, czas poleceń nie.
- Migawka to stan w danej wersji; odtwarzanie czyta ją i tylko zdarzenia po niej. Jest do wyrzucenia
  i nigdy nie należy do historii.
- `override val snapshots = Some(SnapshotPolicy())` je włącza: migawka co 200 zdarzeń i jedna przy
  zatrzymaniu właściciela.
- Cofnięcie, przepisanie historii, squash i trwałe usunięcie usuwają migawki, które czynią błędnymi; nowa
  wartość `version` kontekstu albo `schemaVersion` sprawia, że stare migawki są pomijane.
- Podnieś `schemaVersion`, gdy klasa stanu zmienia kształt, a `version` kontekstu, gdy handler zdarzenia
  zmienia to, co robi ze zdarzeń.
- `aggregateVersionLimit` (domyślnie 10 000) ogranicza wersję każdego agregatu, a squash jej nie obniża.

Dalej: [rozdział 14](14-squash.md) zwija starą historię na dobre.
