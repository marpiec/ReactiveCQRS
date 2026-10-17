# Rozdział 5. Polecenia mają różne smaki

W rozdziale 4 stworzyłeś wózek i włożyłeś do niego jabłka. Niezauważalnie użyłeś dwóch różnych rodzajów
polecenia. ReactiveCQRS ma ich jeszcze kilka, a każda z nich odpowiada na inne pytanie: „czy to coś nowego?”,
„A co, jeśli ktoś to zmienił, gdy szukałem?”, „A co, jeśli moje zapytanie dotrze dwa razy?”. Na koniec
w tym rozdziale poznasz każdy rodzaj polecenia, które wybrać, jaka jest każda odpowiedź
Znaczenie magistrali poleceń i co zrobić, gdy program obsługi poleceń jest powolny. Poznasz również przełącznik, który
sprawia, że ​​agregat przyjmuje polecenia pojedynczo.

---

## Menu

Pomyśl o restauracji. „Poproszę stolik dla dwojga” – to prośba o coś nowego. „Dla stolika 12: zupa”
Zamiast sałatki, właśnie sprawdzam zamówienie” zmienia zamówienie, które widziałeś. „Kolejne
Szklanka wody dla stolika 12" też zmienia sytuację, ale nikogo nie obchodzi, co jeszcze jest na rachunku. ReactiveCQRS
ma polecenie typu dla każdego z nich:

| Rodzaj | Co jest napisane | W koszyku | Jeśli ktoś najpierw zmienił agregat |
|---|---|---|---|
| `FirstCommand` | "utwórz nowy" | `CreateShoppingCart` | (jest nowy) |
| `Command` | „zmień to; ostatnio widziałem wersję 3” | `AddItem` | otrzymujesz polecenie, ale nic nie jest zapisywane |
| `ConcurrentCommand` | "zmień go, bez względu na jego wersję" | `AddItemConcurrently` | biblioteka uruchamia go ponownie w nowszym stanie |

Dwa kolejne rodzaje korygują przeszłość zamiast ją uzupełniać. Na końcu otrzymują krótki fragment.
rozdział, a rozdział 15 dla siebie.

Każdy rodzaj jest klasą abstrakcyjną w `io.reactivecqrs.api`, a każda klasa przypadku polecenia rozszerza się dokładnie
jeden z nich. Pierwszy parametr typu to klasa stanu agregatu; drugi określa rodzaj
odpowiedź. Domena testowa zawsze zapisuje tam `CustomCommandResponse[_]`, więc możesz:

```scala
klasa przypadku AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   nazwa: String) rozszerza Command[ShoppingCart, CustomCommandResponse[_]]
```

Od tego momentu przykłady wykorzystują koszyk domeny testowej (`io.reactivecqrs.testdomain.shoppingcart`),
większy brat tego, o którym napisałeś w rozdziale 3: ten sam stan, plus dodatkowe polecenia i
Wydarzenia, których dotyczy ten i następny rozdział. Jego „CreateShoppingCart” ma jeszcze jedno pole,
wyjaśniono poniżej. Aby uruchomić przykłady, pracuj w tym repozytorium, zgodnie ze specyfikacjami w
`testdomain/src/test/` lub skopiuj potrzebne klasy do własnego pakietu. Konfiguracja z
magistrala poleceń podłączona jak w rozdziale 4:

```scala
importuj io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.testdomain.shoppingcart._
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

// system i commandBus: system aktora i magistrala poleceń wózków z rozdziału 4
import system.dispatcher // uruchamia mapę i poniższe wywołania zwrotne foreach
niejawna wartość timeout: Timeout = Timeout(70.sekund) // dłuższa niż limit czasu polecenia; patrz „Powolne procedury obsługi”

val alice = UserId(1)
val bob = UserId(2)
```

## FirstCommand: tworzenie czegoś nowego

Spotkałeś się z tym w rozdziałach 3 i 4. Wersja domeny testowej ma jeszcze jedno pole:

```scala
klasa przypadku CreateShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, name: String)
  rozszerza FirstCommand[ShoppingCart, CustomCommandResponse[_]] o IdempotentCommand[SagaStep]
```

(Na razie zignoruj ​​`idempotencyId`; ma ono własną sekcję poniżej.)

`FirstCommand` nie ma identyfikatora agregatu, ponieważ agregat jeszcze nie istnieje. Szyna poleceń pobiera
nowy identyfikator z puli identyfikatorów, który pobiera wcześniej, Twój program obsługi działa w kontekście
`initialAggregateRoot`, a odpowiedź powie Ci, jaki identyfikator otrzymał nowy wózek:

```scala
(commandBus ? CreateShoppingCart(None, alice, "Spożywcze")).mapTo[CustomCommandResponse[_]].foreach {
  case SuccessResponse(cartId, version) => println(s"Nowy koszyk $cartId w $version") // wersja: AggregateVersion(1)
  przypadek inny => println(s"Brak koszyka: $other")
}
```

Obsługujący `FirstCommand` odpowiada `FirstEvent`, czyli zdarzeniem rozpoczynającym dziennik i
mówi, do której organizacji należy nowy agregat. Rozdział 6 wyjaśnia.

> **Wskazówka:** odpowiedź to jedyne miejsce, w którym pojawia się nowy identyfikator. Jeśli odpowiedź zaginie po drodze (a
> problem z siecią, przekroczenie limitu czasu) i klient ponownie wysyła polecenie, otrzymujesz dwa wózki. „Wybór
> „id yourself” poniżej rozwiązuje ten problem.

## Polecenie: „Ostatnio widziałem wersję 3”

Alicja i Bob patrzą na wózek z zakupami w wersji 3. Alicja wyjmuje chleb, wózek się rusza
do wersji 4. Bob, wciąż patrząc na wersję 3, dodaje masło, bo jest chleb, na który można je położyć.
Czy zmiana Boba powinna zostać wprowadzona? Podjął decyzję w oparciu o coś, co już nie jest prawdą.

Polecenie „Command” zawiera „expectedVersion”: wersję, na podstawie której nadawca podjął decyzję. Przed
właściciel koszyka przechowuje cokolwiek, porównuje tę wersję z wersją w koszyku:

- **to samo:** zdarzenia Twojego programu obsługi są przechowywane jako kolejne wersje;
- **różnica:** nic nie jest przechowywane i pojawia się błąd `AggregateConcurrentModificationError`. Zwykle
  handler nawet nie działa.

```scala
def addItem(cartId: AggregateId, seen: AggregateVersion, item: String): Future[String] =
  (commandBus ? AddItem(bob, cartId, seen, item)).mapTo[CustomCommandResponse[_]].map {
    przypadek SuccessResponse(_, wersja) =>
      s"Dodano $item; koszyk jest teraz w wersji ${version.asInt}"
    przypadek AggregateConcurrentModificationError(_, _, oczekiwano, było) =>
      „Koszyk zmienił się, gdy patrzyłeś (widziałeś wersję ${expected.asInt}, jest w ${was.asInt})”
    przypadek FailureResponse(powody) =>
      "Odrzucono: " + reasons.mkString("; ")
    przypadek inny =>
      „Coś poszło nie tak:” + inne
  }
```

Błąd zawiera cztery elementy: identyfikator agregatu, jego typ, oczekiwaną wersję i
wersji, w której się znajdował. Typową reakcją jest ponowne odczytanie agregatu (`GetAggregate`), wyświetlenie go i
pozwól użytkownikowi (lub Twojemu kodowi) podjąć decyzję ponownie.

Koszyk, który nie istnieje, otrzymuje ten sam błąd, przy czym `was` jest równe `AggregateVersion.ZERO`: wersja
0 jest wersją niczego.

> **Pamiętaj:** wybierz wersję dla następnego `Polecenia` z najnowszej odpowiedzi.
> `SuccessResponse(cartId, AggregateVersion(4))` oznacza, że ​​„koszyk ma teraz wersję 4”, więc następny
> polecenie oczekuje wersji 4.

> **Pod maską:** wersja jest sprawdzana dwukrotnie, przez właściciela w pamięci i ponownie przez
> Funkcja PostgreSQL, która przechowuje zdarzenia, a także wychwytuje autorów, których właściciel nie widzi. Zobacz
> [ARCHITECTURE.md, sekcja 4.2](../ARCHITECTURE.md).

## ConcurrentCommand: „niezależnie od obecnej wersji”

Rodzinna lista zakupów na lodówce: na dole piszesz „mleko”. Nie obchodzi cię, co inni.
dodano od czasu, gdy ostatnio sprawdzałeś; mleko jest potrzebne w każdym przypadku.

```scala
// Brak oczekiwanej wersji: struktura używa bieżącej wersji i ponawia próby wprowadzenia równoczesnej zmiany.
klasa przypadku AddItemConcurrently(userId: UserId, aggregateId: AggregateId, nazwa: String,
                               idempotencyId: Opcja[KluczKlienta] = Brak)
  rozszerza ConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] o IdempotentCommand[ClientKey]
```

`ConcurrentCommand` ma zbiorczy identyfikator, ale nie ma oczekiwanej wersji. Twój handler działa na koszyku jako
to jest teraz. Jeśli zdarzenia innego polecenia zostaną zapisane podczas działania programu obsługi, biblioteka zgłosi błąd
Twój wynik i ponownie uruchamia handler, w nowszym stanie. Otrzymujesz jedną odpowiedź: sukces, w
niezależnie od wersji, w jakiej znalazły się Twoje wydarzenia.

```scala
commandBus ? AddItemConcurrently(alicja, identyfikator koszyka, "mleko")
// odpowiada SuccessResponse(cartId, AggregateVersion(n)), gdzie n jest wersją, na której wylądowało mleko
```

Jak bardzo się stara?

- **Polecenia, które docierają do tego samego właściciela** rozstrzygają kolizje poprzez kolejkowanie: te, które kolidowały, czekają
  swoją kolej i biegnij ponownie, jeden po drugim, a czekanie nigdy nie jest liczone jako nieudana próba. Zmierzone:
  10 000 poleceń wysłanych jednocześnie do jednego agregatu zakończyło się sukcesem, ostatnie odpowiedziało po 3,8 s (a
  pojedyncze uruchomienie na jednej maszynie programistycznej w dniu 17.10.2026 r.,
  [`docs/perf/load-M5.md`](../perf/load-M5.md)).
- **Konflikty z autorami, których właściciel nie widzi** (właściciel innego węzła tego samego agregatu,
  rozdział 17 lub zapis wykonany poza biblioteką) są powtarzane: maksymalnie dziesięć prób w sumie, czekając
  10 ms przed drugim i podwajanie do 500 ms, z losową częścią, aby rywale się rozprzestrzenili
  na zewnątrz. Po dziesiątym otrzymasz `AggregateConcurrentModificationError`.

> **Ostrzeżenie:** obsługa `ConcurrentCommand` może być uruchamiana więcej niż raz dla jednego polecenia. Zachowaj tę
> bez efektów ubocznych: żadnych e-maili, żadnych płatności, żadnych telefonów, które zmieniają coś gdzie indziej. To jest
> dobra rada dla każdego handlera, ponieważ wynik każdego handlera może zostać odrzucony (konflikt, przekroczenie limitu czasu).
> Efekty uboczne występują w sagach i projekcjach, które reagują na zdarzenia po ich zapisaniu
> (rozdziały 8 i 12).

### Którego powinienem użyć?

- Decyzja zależy od tego, co zobaczył użytkownik („masło, bo jest chleb”, „zapłać tyle, ile zapłaciłem”)
  pokazano"): użyj polecenia `Command`.
- Zmiana ma sens niezależnie od tego, co się stanie („dodaj mleko”, „policz jeszcze jedną wizytę”): użyj
  `ConcurrentCommand`.

Rodzaj polecenia decyduje o sposobie obsługi konfliktu i nie ma żadnych ustawień, które mogłyby to zmienić.

## Wybieranie identyfikatora samodzielnie

Wyobraź sobie papierowy formularz zamówienia z wydrukowanym numerem w rogu. Jeśli przez pomyłkę wyślesz go dwa razy,
Sklep widzi ten sam numer dwa razy i składa jedno zamówienie. `FirstCommand` może zawierać taki numer:
identyfikator agregatu, który tworzy.

1. Zapytaj magistralę poleceń o nowy identyfikator z parametrem `ReserveAggregateId`. Odpowiedzią jest `AggregateId` z
   tego samego basenu, z którego korzysta autobus.
2. Zachowaj identyfikator wraz z żądaniem (w kliencie, w dzienniku żądań) przed wysłaniem czegokolwiek.
3. Wyślij `FirstCommand`, który podaje nazwę id w `chosenAggregateId`.
4. Jeżeli nie otrzymałeś odpowiedzi, wyślij to samo polecenie jeszcze raz.

Domena testowa ma takie polecenie:

```scala
klasa przypadku CreateShoppingCartWithId(idempotencyId: Option[ClientKey], userId: UserId, cartId: AggregateId, name: String)
  rozszerza FirstCommand[ShoppingCart, CustomCommandResponse[_]] o IdempotentCommand[ClientKey] {
  nadpisz def chosenAggregateId: Option[AggregateId] = Some(cartId)
}
```

```scala
wartość utworzona: Future[CustomCommandResponse[_]] = dla {
  cartId <- (commandBus ? ReserveAggregateId).mapTo[AggregateId]
  odpowiedź <- (commandBus ? CreateShoppingCartWithId(None, alice, cartId, "Spożywcze")).mapTo[CustomCommandResponse[_]]
} odpowiedź // SuccessResponse(cartId, AggregateVersion(1))
```

Wysłano po raz drugi, ale polecenie nie może utworzyć drugiego koszyka. Koszyk istnieje, więc kopia jest…
odpowiedział `AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart],
AggregateVersion.ZERO, AggregateVersion(1))`: "niczego się tam nie spodziewałeś, ale jest to wersja 1".
W przypadku klucza idempotencji (następna sekcja) kopia otrzymuje dokładnie taki sam `SuccessResponse`, który
jest przyjaźniejszy.

### Jakie identyfikatory mogę wybrać?

Weź je z `ReserveAggregateId` i nie musisz już o tym myśleć. Jeśli wybierzesz inne identyfikatory,
sposób, magistrala poleceń sprawdza je przed uruchomieniem czegokolwiek i odpowiada `FailureResponse` na identyfikator, który
Jest:

- nie jest dodatnia (zero lub mniej; identyfikatory ujemne są przechowywane dla ulotnych agregatów w pamięci, wchodzących w skład
  1.0),
- nie mniej niż 2^53 (9 007 199 254 740 992), czyli największa liczba całkowita, jaką klient JavaScript odczytuje dokładnie,
- w zasięgu innej instalacji.

*Instalacja* to wdrożenie aplikacji z własną bazą danych, np. dla Europy
i jeden dla Azji. Każdy z nich ustawia `reactivecqrs.installation-number` (domyślnie 0) i instalację *n*
tworzy swoje identyfikatory od *n* × 10^12 do miejsca, w którym rozpoczyna się instalacja *n* + 1, więc identyfikatory dwóch
Instalacje nigdy nie kolidują ze sobą, jeśli ich dane się ze sobą stykają. Każdy węzeł jednej instalacji ustawia ten sam
numer:

```
# application.conf instalacji 3: jej identyfikatory zaczynają się od 3 000 000 000 000
reactivecqrs.numer-instalacji = 3
```

Identyfikatory poniżej 10^12, czyli zakresu, w którym znajdują się wszystkie identyfikatory utworzone przed wersją 1.0, są dozwolone w każdej instalacji.

> **Pod maską:** zakresy znajdują się w `IdRange` (`core/.../uid/IdRange.scala`). Zobacz
> [ARCHITECTURE.md, sekcja 13.5](../ARCHITECTURE.md).

## Polecenia idempotentne: można je bezpiecznie wysłać dwukrotnie

Klikasz „Zapłać” w sklepie internetowym i strona się zawiesza. Klikasz ponownie. Czy zapłaciłeś dwa razy? Dobry
Sklep przypisuje obu kliknięciom ten sam numer zamówienia, a przy drugim kliknięciu okazuje się, że zamówienie zostało już złożone.

Polecenie, które działa raz na każde naciśnięcie klawisza, niezależnie od częstotliwości jego wysyłania (i liczby otrzymanych kopii)
w tym samym momencie) nazywa się *idempotent*. Można go utworzyć, mieszając `IdempotentCommand` i
nadając mu `idempotencyId`:

```scala
cecha IdempotentCommand[IID <: CommandIdempotencyId] {
  val idempotencyId: Opcja[IID]
}
```

Kluczem jest dowolna klasa z ciągiem `asDbKey: String`. Domena testowa ma jeden dla klientów:

```scala
/** Klucz wybierany przez osobę wywołującą, sposób w jaki klient ponawiający żądanie by go wysłał. */
klasa przypadku ClientKey(wartość: String) rozszerza CommandIdempotencyId {
  nadpisz def asDbKey: String = wartość
}
```

a biblioteka ma jeden dla sag: `SagaStep(sagaId, step)`, którego kluczem jest `"sagaId|step"`. Sagi
(rozdział 12) podaj polecenie każdego kroku jako klucz, dzięki czemu saga będzie kontynuowana po katastrofie
nie można zastosować kroku dwa razy.

Oto śliwka dodana dwukrotnie z jednym kluczem:

```scala
val key = Some(ClientKey(java.util.UUID.randomUUID().toString)) // jeden klucz na akcję użytkownika
val addPlum = AddItemWithReceipt(klucz, alice, cartId, AggregateVersion(1), "plum")

val first = commandBus ? addPlum // CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))
val second = commandBus ? addPlum // ta sama odpowiedź; w koszyku jest jedna śliwka
```

Odpowiedź pierwszej kopii jest przechowywana w tej samej transakcji, co jej zdarzenia. Każda kolejna kopia i
każda kopia, która się z nią ściga, otrzymuje odpowiedź w postaci zapisanej odpowiedzi, a jej własne zdarzenia nigdy nie są
Zapisane. Dotyczy to również ponownego uruchomienia: odpowiedzi są przechowywane w PostgreSQL. Klucz „Brak” oznacza „nie”
tym razem idempotentne", jak `CreateShoppingCart(None, alice, "Groceries")` w przykładach.

Zasady:

- **Klucze są globalne**, współdzielone przez każdy typ agregatu. Używaj nowego UUID dla każdej akcji użytkownika, nigdy
  coś w stylu `"koszyk-1"`: `"koszyk-1" innego typu pobierze zapisaną odpowiedź Twojego koszyka. Klucz
  może mieć do 256 znaków.
- **Tylko polecenia przechowujące zdarzenia przechowują odpowiedź.** Kopia odrzuconego polecenia
  (`FailureResponse`) lub taki, który nie wygenerował żadnych zdarzeń, po prostu uruchamia się ponownie.
- **Odpowiedzi są domyślnie przechowywane przez 30 dni**, a każda magistrala poleceń usuwa starsze odpowiedzi co
  godzinę. Kopia, która dotrze później, zostanie uruchomiona jako nowe polecenie. Ustaw inny okres podczas tworzenia
  stan odpowiedzi:

  ```scala
  val responses = new PostgresCommandResponseState(mpjsons, typesNames, retention = 7.days).initSchema()
  ```

- **Informacje niestandardowe w zapisanej odpowiedzi** (patrz „Sukces z czymś dodatkowym” poniżej) muszą być klasą
  bez parametrów typu, takich jak klasa przypadku, np. `ItemReceipt`.

> **Ostrzeżenie:** idempotencja wymaga tabeli odpowiedzi i jej unikalnego indeksu, który
> Tworzy `PostgresCommandResponseState.initSchema()`. Zapomnij o `initSchema()` i każdym idempotentnym
> polecenie nie powiodło się.

> **Wskazówka:** aby tworzyć rzeczy, połącz oba triki: zarezerwuj identyfikator i wydaj polecenie tworzenia
> klucz, jak w `CreateShoppingCartWithId(Some(ClientKey(...)), alice, cartId, "Groceries")`. Dwie kopie
> ten wyścig, nawet przez dwa różne węzły, oba otrzymują `SuccessResponse(cartId, AggregateVersion(1))`.

## Na co może odpowiedzieć osoba obsługująca

Obsługa poleceń to funkcja od bieżącego stanu do wyniku. Oto wyniki, od
codzienne i te bardziej niezwykłe.

### Jedno wydarzenie lub kilka

`CommandSuccess(ItemAdded(c.name))` przechowuje jedno zdarzenie. Obsługujący może również zwrócić kilka zdarzeń, które
kolejne wersje:

```scala
przypadek c: AddItems => CommandSuccess(c.names.map(ItemAdded))
```

Odpowiedź na pytanie `AddItems(alice, cartId, AggregateVersion(1), Seq("apple", "pear"))`
`SuccessResponse(cartId, AggregateVersion(3))`. Zdarzenia jednego polecenia są przechowywane razem lub nie
wcale: jeśli drugiego nie da się zapisać, pierwszego też nie da się zapisać.

### Sukces z czymś ekstra

Czasami dzwoniący potrzebuje czegoś więcej niż tylko nowej wersji: paragonu, obliczonego numeru, wygenerowanego
kod. Przekaż go jako drugi argument:

```scala
klasa przypadku ItemReceipt(itemName: String)

przypadek c: AddItemWithReceipt => CommandSuccess(ItemAdded(c.name), ItemReceipt(c.name))
```

W takim przypadku odpowiedź brzmi `CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))`
zwykłego `SuccessResponse`.

### NIE

Reguła mówi: nie: zwróć `CommandFailure` z komunikatem. Nic nie jest zapisywane, a wywołujący otrzymuje
wiadomość zwrotna. Twój koszyk z rozdziału 3 egzekwuje swoją zasadę domową w ten sposób:

```scala
przypadek c: Dodaj element =>
  if (cart.items.size >= 5) CommandFailure("Nie możesz mieć więcej niż 5 produktów w koszyku")
  w przeciwnym razie CommandSuccess(ItemAdded(c.name))
```

Odpowiedź brzmi `FailureResponse(List("W koszyku nie może być więcej niż 5 pozycji")). Użyj jej do
reguły biznesowe; wyjątki należy zachować dla rzeczy, które są naprawdę nie tak.

### Nic do zrobienia

Niektóre polecenia wyglądają i nie znajdują niczego, co można by zmienić: powtarzające się sprawdzanie lub żądanie, które już jest
zadowolony. Zwróć sukces bez zdarzeń:

```scala
przypadek _: CheckCart => CommandSuccess(Seq.empty[Event[ShoppingCart]])
```

Odpowiedź brzmi `SuccessResponse(cartId, AggregateVersion(1))` z aktualną wersją koszyka i
nie ma tam kompletnie nic napisanego: żadnego wydarzenia, żadnej nowej wersji, nawet żadnego oświadczenia bazy danych o koszyku.
Zestaw testowy wysyła 10 000 takich wiadomości i dokładnie to sprawdza.

### Później: asynchroniczne procedury obsługi

Obsługujący, który musi czekać na coś, np. usługę standardową, zwraca `AsyncCommandResult` około
„Przyszłość” jednego z powyższych wyników. Obsługa „AddItem” domeny testowej jest zapisana w ten sposób. Tutaj
jest wariantem, który najpierw prosi o wykonanie własnej usługi magazynowej:

```scala
importuj java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

/** Twoja własna usługa, nie jest częścią biblioteki. */
cecha zapasowa {
  def available(element: String): Future[Boolean]
}

klasa StockCheckingCarts(stock: Stock) rozszerza ShoppingCartAggregateContext {
  // własny ograniczony zbiór do dalszych prac
  prywatna niejawna wartość stockPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  nadpisz def commandHandlers = cart => {
    przypadek c: Dodaj element =>
      AsyncCommandResult(stock.available(c.name).map {
        przypadek true => CommandSuccess(ItemAdded(c.name))
        case false => CommandFailure(c.name + "brak w magazynie")
      })
    przypadek inny => super.commandHandlers(koszyk)(inne)
  }
}
```

Twój handler powraca w momencie, gdy ma `Przyszłość`, co zwalnia jego wątek; właściciel odbiera
wynik po zakończeniu przyszłości. Wewnątrz `AggregateContext` możesz nawet zwrócić `Future`
sam w sobie: kontekst zamienia go dla Ciebie w `AsyncCommandResult`, czyli to, co jest domeną testową
`createShoppingCart` opiera się na.

## Każda odpowiedź na pierwszy rzut oka

Cokolwiek się stanie, polecenie otrzyma dokładnie jedną odpowiedź:

| Odpowiedź | Co się stało | Co robisz |
|---|---|---|
| `SuccessResponse(id, version)` | Zapisano lub nic do zapisania. `version` to aktualna wersja agregatu. | Kontynuuj; użyj `version` w następnym `Poleceniu`. |
| `CustomSuccessResponse(id, version, info)` | To samo, z tym że Twój program obsługi zwrócił `info`. | Przeczytaj `info`. |
| `FailureResponse(reasons)` | Twój program obsługi zdarzeń odpowiedział „nie” albo magistrala odrzuciła polecenie przed jego uruchomieniem: wybrany identyfikator, który nie jest dozwolony, nowy agregat, którego pierwsza nazwa zdarzenia nie jest spacją (rozdział 6) lub, w klastrze z agregatami shardowanymi, polecenie jest zbyt duże, aby można je było wysłać między węzłami (rozdział 17). | Pokaż powody. |
| `AggregateConcurrentModificationError(id, type, expected, was)` | Agregat nie jest w oczekiwanej wersji: ktoś go zmienił, nie istnieje (`was` wynosi 0) lub wybrany identyfikator jest już zajęty. Funkcja `ConcurrentCommand` otrzymuje go dopiero po dziesięciu próbach. | Przeczytaj agregat ponownie i podejmij decyzję. |
| `CommandRejected(id, reason)` | Nie uruchomiono: zbyt wiele poleceń czeka już na ten agregat (przetwarzanie szeregowe opisane poniżej) lub jego zdarzenia należą do klasy, której nie każdy węzeł klastra może jeszcze odczytać (rozdział 17). | Wyślij go ponownie nieco później. |
| `CommandHandlingError(command, errorId, commandId)` | Twój moduł obsługi rzucił wyjątek lub nie miał przypadku dla polecenia; trwało to dłużej niż limit czasu; nie było agregatu do uruchomienia (został usunięty lub brakowało w przypadku `ConcurrentCommand`); nie można było odczytać agregatu z magazynu; lub magistrala nie miała identyfikatorów. | Wyszukaj `errorId` w dzienniku. |
| `EventHandlingError(event, errorId, commandId)` | Zapisywanie nie powiodło się: obsługa zdarzeń zgłosiła błąd, zdarzenie było zbyt duże lub agregat zawierał zbyt wiele zdarzeń (rozdział 6) lub wystąpiła awaria bazy danych. Nic nie zostało zapisane. | ​​Wyszukaj `errorId` w dzienniku. |

Na wiadomość, która w ogóle nie jest poleceniem, nie odpowiada się żadną z poniższych odpowiedzi: magistrala nie obsługuje Twojego zapytania
`Future` z `IllegalArgumentException`.

> **Wskazówka:** pokaż `errorId` osobie zgłaszającej problem. Biblioteka zapisze ten sam identyfikator w swoim
> log z przyczyną (na przykład „Obsługa błędu polecenia AddItem, identyfikator błędu: [...]”), aby pomoc techniczna mogła
> znajdziesz w kilka sekund.

## Powolne zarządzanie

Większość procedur obsługi zajmuje mikrosekundy. Niektóre muszą wywołać powolną usługę lub wykonać wiele obliczeń. Liczą się dwie rzeczy.
następnie: dokąd biegnie przewodnik i ile to może potrwać.

### Gdzie biegają opiekunowie

Obsługujące polecenia programy działają w ramach własnej puli wątków `reactivecqrs.handler-dispatcher` z 32 wątkami
wątki (plik `reference.conf` biblioteki). Magistrale, projekcje i sagi działają gdzie indziej, więc powolny
Obsługujący spowalnia tylko polecenia. Ale obsługujący, który *przetrzymuje* swój wątek (długie obliczenie,
`Thread.sleep`, blokujące wywołanie HTTP lub JDBC) zajmuje jeden z tych 32, a gdy wszystkie 32 zostaną zajęte, każdy
polecenie na tym węźle czeka.

Pomiar wykonano 17.10.2026 r. (pojedynczy przebieg na jednej maszynie programistycznej z 12 wątkami procesora i bazą danych PostgreSQL na
ta sama maszyna, [`docs/perf/heavy-M5.md`](../perf/heavy-M5.md)): 36 poleceń, których programy obsługi każdy
utrzymywali swój wątek przez 5 sekund, na wszystkie odpowiedzi udzielono odpowiedzi po 10,0 s, ponieważ 32 wątki były uruchamiane na raz, a tymczasem
polecenie światła dla innego agregatu czekało 5,4 s. Ta sama praca na puli aplikacji
własny, zwrócony jako `AsyncCommandResult`, ostatnia odpowiedź nadeszła po 5,0 s, a polecenie światła zajęło
3 milisekundy.

Rozmiar puli można zmienić w `application.conf`:

```
reactivecqrs.handler-dispatcher.thread-pool-executor.fixed-pool-size = 64
```

Więcej wątków jest odpowiednich dla modułów obsługi oczekujących na wejście/wyjście; dla modułów obsługi, które wykonują obliczenia, liczba wątków odpowiada liczbie wątków procesora
rdzenie wystarczą.

### Limit czasu polecenia

Każdy kontekst ma `commandTimeout`: 60 sekund, chyba że go zmienisz. Liczy się od momentu
Właściciel agregatu przejmuje polecenie. Po jego zatwierdzeniu, wywołujący otrzymuje błąd `CommandHandlingError`, a
polecenie jest martwe: jeśli obiekt obsługujący zakończy działanie później, jego wynik zostanie odrzucony i nic nie zostanie zapisane.
Możesz poświęcić kontekstowi więcej czasu:

```scala
klasa PatientCarts rozszerza ShoppingCartAggregateContext {
  nadpisanie wartości commandTimeout: FiniteDuration = 2 minuty
}
```

> **Ostrzeżenie:** Twój własny limit czasu na żądanie to osobny zegar. Jeśli jest krótszy niż limit czasu na polecenie,
> Twój `Future` kończy się niepowodzeniem z powodu `AskTimeoutException` podczas wykonywania polecenia i może nadal występować
> zastosowano. Ustaw limit czasu na żądanie dłuższy niż limit czasu na polecenie (jak w konfiguracji z tego rozdziału) i
> wydaj polecenia, aby klienci ponawiali próbę użycia klucza idempotentności.

### Długa praca w wygodny sposób

W przypadku procedur, których wykonanie zajmuje kilka minut, w notatkach dotyczących skalowania biblioteki zaleca się najpierw
([`docs/SCALING.md`](../SCALING.md), sekcja 8):

1. **Przenieś długą pracę poza polecenie.** Polecenie rejestruje żądanie (jedno zdarzenie); pracownik lub
   saga (rozdział 12) wykonuje pracę na własnych wątkach, ponawianych i wznawianych; drugie polecenie
   rejestruje wynik. Suma pozostaje dostępna, a żaden timeout ani kolizja nigdy nie dotyka długiego
   praca.
2. **Jeśli polecenie musi to zrobić:** zwróć `AsyncCommandResult` z pracą na ograniczonym puli Twoich
   własny; podnieś `commandTimeout` powyżej najdłuższego przebiegu; użyj w tym celu przetwarzania szeregowego (następna sekcja)
   typ, aby praca nigdy nie została uruchomiona ponownie po kolizji; i daj klientom klucz idempotentności i
   odpowiednio długi limit czasu lub odpowiedz „zaakceptowano” od razu i pokaż wynik później za pomocą projekcji.

## Pojedynczo: przetwarzanie szeregowe

Domyślnie właściciel pozwala, aby polecenia jego agregatu były uruchamiane obok siebie, każde w znalezionym stanie,
i rozwiązuje konflikty później: za pomocą błędu dla `Polecenia`, za pomocą kolejnego uruchomienia dla
`ConcurrentCommand`. W przypadku agregatu, który zmienia wiele osób jednocześnie, pojedyncza kolejka może być spokojniejsza,
jak w okienku pocztowym: każdy czeka na swoją kolej i każdy widzi dokładnie, co mu się należy
zanim ich zostawią.

```scala
klasa BusyCarts rozszerza ShoppingCartAggregateContext {
  nadpisanie wartości commandProcessing: CommandProcessing = CommandProcessing.Serial(maxQueuePerAggregate = 100)
}
```

Z `Serial`:

- polecenia jednego agregatu są uruchamiane jedno po drugim, w kolejności nadejścia; każde rozpoczyna się, gdy
  jeden przed otrzymaniem odpowiedzi, więc jego operator widzi najnowszy stan;
- nie mogą ze sobą kolidować, więc obsługa `ConcurrentCommand` jest uruchamiana raz. (`Command`, którego
  oczekiwana wersja jest nieaktualna, nadal występuje błąd `AggregateConcurrentModificationError`.)
- co najwyżej polecenia `maxQueuePerAggregate` czekają za poleceniem, które jest uruchamiane (1 000, jeśli napiszesz tylko
  `Serial()`); na jeszcze jedno pytanie odpowiadamy od razu
  `CommandRejected(cartId, "100 poleceń z całej sumy już czeka")` i może zostać wysłane ponownie
  później;
- limit czasu polecenia liczony jest od momentu jego nadejścia, wliczając czas oczekiwania w kolejce. Dziesięć poleceń na minutę
  każdy z nich z rzędu potrzebował przerwy trwającej ponad dziesięć minut na ostatni.

Drugi parametr, `onOverflow`, określa, co zrobić z tym jednym poleceniem za dużo; obecnie ma on
pojedynczy wybór, `OverflowPolicy.Reject`.

Zmierzono 17.10.2026 (ta sama maszyna, [`docs/perf/load-M5.md`](../perf/load-M5.md)): 10 000 poleceń
wysłane jednocześnie do jednego agregatu w trybie szeregowym dały 1 003 sukcesy i 8 997 odrzuceń, ostatni
odpowiedź po 0,6 s. W trybie domyślnym wszystkie 10 000 operacji zakończyły się powodzeniem, ale ostatnia operacja trwała 3,8 s.

Wybierz opcję „Seryjny” w przypadku agregatów, które wielu klientów zapisuje jednocześnie, w przypadku obsługi wymagającej dużej ilości pracy
który nie powinien działać dwa razy, a gdy szybkie „zajęte, spróbuj ponownie” bardziej odpowiada Twoim klientom niż długie
Czekać.

## Korygowanie przeszłości

Czasami przeszłość sama w sobie musi się zmienić: klient prosi o usunięcie swojego nazwiska z każdej
wózek, jaki kiedykolwiek stworzyli. W tym celu istnieją jeszcze dwa rodzaje poleceń: `RewriteHistoryCommand` (z
(oczekiwana wersja) i `RewriteHistoryConcurrentCommand` (bez). Nazywają klasy zdarzeń, które
przepisz w `eventsTypes`; ich handlery znajdują się w kontekście `rewriteHistoryCommandHandlers`,
odbiera zapisane zdarzenia tych klas i odpowiada za pomocą `RewriteCommandSuccess`: przepisane
zdarzenia, a także nowe zdarzenia rejestrujące zmianę. Funkcja „RewriteCartName” koszyka zmienia nazwę koszyka w jego
Zdarzenie `ShoppingCartCreated` i dodaje `CartNameRewritten`. To ostre narzędzie i
[rozdział 15](15-rewriting-history.md) pokazuje, jak sobie z tym bezpiecznie poradzić.

## Czego się nauczyłeś

- `FirstCommand` tworzy agregat. Magistrala wybiera identyfikator, chyba że `chosenAggregateId` określi jeden
  dokonano rezerwacji za pomocą `ReserveAggregateId`, co sprawia, że ​​ponowne utworzenie konta jest nieszkodliwe.
- Polecenie `Command` niesie ze sobą `expectedVersion`; gdy agregat przesunie się dalej, nic nie zostanie zapisane i
  pobierz `AggregateConcurrentModificationError`.
- `ConcurrentCommand` działa w bieżącej wersji i jest uruchamiany ponownie po kolizji, więc jego obsługa
  musi być wolny od skutków ubocznych.
- `IdempotentCommand` z kluczem powoduje, że każda kopia polecenia otrzymuje odpowiedź pierwszej kopii; klucze są
  globalne i odpowiedzi są domyślnie przechowywane przez 30 dni.
- Obsługujący odpowiadają `CommandSuccess` (jedno zdarzenie, kilka, brak lub z dodatkowymi informacjami),
  `CommandFailure` lub `AsyncCommandResult`, a każde polecenie kończy się dokładnie jedną odpowiedzią.
- Powolne procedury obsługi spełniają wymagania dotyczące 60-sekundowego limitu czasu polecenia i puli procedur obsługi 32 wątków; długa praca należy do
  poza dowództwem.
- `CommandProcessing.Serial` uruchamia polecenia agregatu pojedynczo i odpowiada na `CommandRejected`
  gdy kolejka jest pełna.

Następnie: [rozdział 6](06-events.md) przygląda się bliżej temu, co pozostawiają po sobie polecenia: zdarzeniom.