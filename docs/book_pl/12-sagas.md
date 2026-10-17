# Rozdział 12. Sagi: Jak sprawić, by agregaty współpracowały

Do końca tego rozdziału będziesz w stanie napisać sagę: mały proces, który napędza zadanie
w kilku agregatach, krok po kroku, zapamiętuje, gdzie dotarł po ponownym uruchomieniu i cofa swoje działania
Praca, gdy krok się nie powiedzie. Przeczytamy sagę domeny testowej wiersz po wierszu, prześledzimy dwa z jej
biegnie, a potem naszkicujemy własną sagę.

---

## Kiedy jeden agregat nie wystarczy

Agregat rządzi się swoimi własnymi prawami (rozdział 1). Niektóre zadania wymagają kilku z nich:

- „utwórz pięć wózków, po jednym dla każdego sklepu, do którego dostarczamy”: pięć agregatów;
- „zarezerwuj towary w magazynie, a następnie pobierz opłatę od klienta”: suma zapasów i suma płatności.

Żadna transakcja ich nie obejmuje. Jak więc zrobić wszystko, albo nic?

Wyobraź sobie agenta podróży rezerwującego podróż: lot, hotel i samochód. Każda rezerwacja to osobna
Rozmowa telefoniczna, która może się powieść lub nie powieść. Agent prowadzi listę kontrolną zarezerwowanych terminów, aby…
daleko. Kiedy wypożyczalnia samochodów mówi „nie”, nie zostawia cię z lotem i hotelem, z którego nie możesz skorzystać:
Dzwoni ponownie do hotelu i linii lotniczych, anulując rezerwację w odwrotnej kolejności.

Ten agent prowadzący listę kontrolną to **saga** (nazywana też *menedżerem procesów*). Połączenia, które cofają
Wcześniejsze prace to **kompensacje**; w ReactiveCQRS nazywane są **powrotami**. Saga wysyła jeden
polecenie na raz, zapisuje swoją listę kontrolną w PostgreSQL po każdym kroku, a gdy krok się nie powiedzie,
cofa to, co zostało wykonane w poprzednich krokach.

## Części sagi

Wszystko poniżej znajduje się w `io.reactivecqrs.core.saga`, z wyjątkiem `SagaStep`, który znajduje się w
`io.reactivecqrs.api`.

| Część | Co to jest |
| --- | --- |
| `SagaActor` | klasa, którą rozszerzasz; jeden aktor prowadzi każdą sagę tego rodzaju |
| order, `SagaOrder` | wiadomość wysyłana przez osobę dzwoniącą w celu rozpoczęcia sagi |
| porządek wewnętrzny, `SagaInternalOrder` | wiadomość, którą saga wysyła sobie w następnym kroku (`SagaOrder` jest jednym z nich) |
| krok | jedno wywołanie `handleOrder` (lub `handleRevert`) z jednym zamówieniem |
| `SagaStep(sagaId, step)` | identyfikator sagi i numer kroku, nadawane każdemu krokowi |
| odpowiedź, `SagaResponse` | co otrzymuje osoba dzwoniąca |
| `SagaState` | gdzie przechowywany jest postęp; `PostgresSagaState` przechowuje go w tabeli `sagas` |

Krok zwraca „Przyszłość” o jednym z trzech statusów:

- `SagaContinues(order)`: ten krok jest wykonany; następny jest uruchamiany z `order`.
- `SagaSucceded(response)`: całe zadanie jest wykonane; osoba wywołująca otrzymuje `response`.
- `SagaFailed(response)`: zadanie nie powiodło się; wywołujący otrzymuje `response` i saga zaczyna się wycofywać.

Krok powrotu zwraca „Przyszłość” jednego z następujących:

- `SagaRevertContinues(order)`: tyle zostało cofnięte; następny krok przywracania zostanie wykonany z `order`.
- `SagaRevertSucceded`: wszystko zostało cofnięte.
- `SagaRevertFailed(exceptions)`: cofanie nie powiodło się.

> **Wskazówka:** zwróć uwagę na pisownię. Biblioteka zapisuje „SagaSucceded” i „SagaRevertSucceded”, z jednym
> "e" mniej niż w słowniku. Niech redaktor je uzupełni.

Saga zawsze znajduje się w jednej z trzech **faz** (`SagaPhase`): `KOŃCZY się`, gdy działa do przodu,
`REVERTING` podczas cofania i `ERROR`, który jest zarezerwowany: biegacz nigdy nie umieszcza tam sagi, i
zapisana saga znaleziona w tej fazie nie jest uruchamiana (na początku rejestrowane jest ostrzeżenie).

## Saga domeny testowej: wiele koszyków naraz

`MultipleCartCreatorSaga` (w `testdomain/.../shoppingcart/`) tworzy kilka koszyków o nazwach
„Artykuły spożywcze 1”, „Artykuły spożywcze 2” itd., po jednym na krok. Jeśli któryś z nich zostanie odrzucony, pozostałe zostaną usunięte.
To zostało zrobione. Przeczytajmy to od góry.

### Wiadomości

```scala
obiekt MultipleCartCreatorSaga {

  klasa przypadku CreateMultipleCarts(userId: UserId, cartName: String, cartsCount: Int) rozszerza SagaOrder
  prywatna klasa przypadku CreateRemainingCarts(userId: UserId, cartName: String, cartsCount: Int,
                                          createdCarts: List[AggregateIdWithVersion]) rozszerza SagaInternalOrder

  cecha MultipleCartCreatorSagaResponse rozszerza SagaResponse
  klasa przypadku CartsCreated(wózki: Lista[AggregateIdWithVersion]) rozszerza MultipleCartCreatorSagaResponse
  klasa przypadku CartsCreationFailure (wyjątki: Lista [String]) rozszerza MultipleCartCreatorSagaResponse
}
```

`CreateMultipleCarts` jest publiczne: wysyłają je osoby wywołujące. `CreateRemainingCarts` jest prywatne: wysyła tylko sagę
przekazuje je sobie i niesie wszystkie dotychczas wykonane wózki. Każde zamówienie ma identyfikator użytkownika.
`SagaInternalOrder` prosi o jedno.

> **Pamiętaj:** zamówienie zawiera wszystko, czego będzie potrzebował kolejny krok lub cofnięcie. Zamówienia są
> co zawiera saga; nic innego nie przetrwa ponownego uruchomienia.

### Klasa

```scala
klasa MultipleCartCreatorSaga(val state: SagaState, val uidGenerator: ActorRef, shoppingCartCommandBus: ActorRef)
  rozszerza SagaActor {

  import context.dispatcher

  nadpisanie val name = "MultipleCartCreatorSaga"

  nadpisz def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    kolejność przypadków: CreateMultipleCarts => handleCreateMultipleCarts(sagaStep, order)
    kolejność przypadków: CreateRemainingCarts => handleCreateRemainingCarts(sagaStep, order)
  }

  zastąp def handleRevert(sagaStep: SagaStep): OdbierzRevert = {
    cofnij przypadek: CreateRemainingCarts =>
      przywróćUtwórzPozostałeWózki(sagaStep, przywróć.Identyfikatorużytkownika, przywróćNazwaWózka, przywróćLiczbaWózków, przywróćUtworzoneWózki)
  }

  // ... kroki poniżej
}
```

Każda saga przypisuje biegaczowi trzy wartości i dwie funkcje:

- `state`: miejsce przechowywania postępu.
- `uidGenerator`: aktor generatora uid, z którego korzystają Twoje szyny poleceń (rozdział 4). Identyfikatory Saga pochodzą z niego.
- `name`: nazwa tego rodzaju sagi. Jej wiersze w tabeli `sagas` są odnajdywane po niej.
- `handleOrder(sagaStep)`: `ReceiveOrder`, czyli
  `FunkcjaCzęściowa[SagaInternalOrder, Future[SagaHandlingStatus]]`.
- `handleRevert(sagaStep)`: `ReceiveRevert`,
  `FunkcjaCzęściowa[SagaInternalOrder, Future[SagaRevertHandlingStatus]]`.

`import context.dispatcher` zapewnia `Future`om kontekst wykonania, na którym można uruchomić `mapę`.

### Krok

Oba zamówienia trafiają do tej samej metody. Wysyła ona jedno polecenie i zamienia odpowiedź na status:

```scala
prywatna def createShoppingCart(sagaStep: SagaStep, userId: UserId, cartName: String, cartsCount: Int,
                               createdCarts: Lista[AggregateIdWithVersion]) = {
  val currentCartId = createdCarts.size + 1
  (shoppingCartCommandBus ? CreateShoppingCart(Some(sagaStep), userId, cartName + " " + currentCartId))
    .mapTo[OdpowiedźNiestandardowegoPolecenia[_]]
    .mapa {
      przypadek c: SuccessResponse, jeśli createdCarts.length + 1 < cartsCount =>
        SagaContinues(CreateRemainingCarts(userId, cartName, cartsCount,
          AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
      przypadek c: SuccessResponse =>
        SagaSucceded(CartsCreated(AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
      przypadek c: FailureResponse =>
        SagaFailed(CartsCreationFailure(c.exceptions))
    }
}
```

Koszyk jest gotowy, a kolejne będą gotowe: kontynuuj, dodając nowy koszyk do listy. Ostatni koszyk
jest tworzony: succeed, z całą listą. Koszyk jest odrzucany: fail. `?` używa własnego sagi
niejawny limit czasu 60 sekund, z `SagaActor`. `Some(sagaStep)` to identyfikator idempotencji
polecenie; poniżej znajduje się jego własna sekcja.

(`handleCreateMultipleCarts` wywołuje tę metodę, gdy `cartsCount > 0`. W przeciwnym razie zwraca
`Future.successful(SagaSucceded(CartsCreationFailure(List("Brak koszyków do utworzenia"))))` na raz:
(Saga kończy się normalnie, bez niczego do zrobienia.)

### Powrót

```scala
prywatna def revertCreateRemainingCarts(sagaStep: SagaStep, userId: UserId, cartName: String, cartsCount: Int,
                                       createdCarts: Lista[AggregateIdWithVersion]) = {
  (shoppingCartCommandBus ? DeleteShoppingCart(Some(sagaStep), userId, createdCarts.head.id, createdCarts.head.version))
    .mapTo[OdpowiedźNiestandardowegoPolecenia[_]]
    .mapa {
      przypadek c: SuccessResponse, jeśli createdCarts.length > 1 =>
        SagaRevertContinues(CreateRemainingCarts(userId, cartName, cartsCount, createdCarts.tail))
      przypadek c: SuccessResponse =>
        SagaRevertSucceded
      przypadek c: FailureResponse =>
        SagaRevertFailed(c.exceptions)
    }
}
```

Jeden koszyk na krok przywracania: usuń najnowszy, a następnie kontynuuj z resztą listy.

### Bieg, który się udał

Wywołujący wysyła `CreateMultipleCarts(alice, "Groceries", 3)`:

| Krok | Rozkaz | Polecenie wysłane | Status |
| --- | --- | --- | --- |
| 0 | `CreateMultipleCarts` | utwórz "Zakupy spożywcze 1" | `SagaContinues(CreateRemainingCarts(..., [koszyk 1]))` |
| 1 | `CreateRemainingCarts([koszyk 1])` | utwórz "Zakupy 2" | `SagaContinues(CreateRemainingCarts(..., [koszyk 2, koszyk 1]))` |
| 2 | `CreateRemainingCarts([koszyk 2, koszyk 1])` | utwórz "Zakupy spożywcze 3" | `SagaSucceded(CartsCreated([koszyk 3, koszyk 2, koszyk 1]))` |

Po kroku 2 osoba wywołująca otrzymuje „CartsCreated” z trzema wózkami (najnowsze na początku) i sagą
wiersz został usunięty.

### Bieg, który się nie udaje

Program obsługi koszyka domeny testowej ma pułapkę dla testów: odrzuca każdy koszyk, którego nazwa kończy się na
„M 4”. Zatem wywołujący wysyła `CreateMultipleCarts(alicja, „Spożywcze M”, 5)`:

| Krok | Faza | Zamówienie | Co się dzieje |
| --- | --- | --- | --- |
| 0 do 2 | `CONTYNUUJE` | jak powyżej | Utworzono pozycje od „Artykuły spożywcze M 1” do „Artykuły spożywcze M 3” |
| 3 | `CONTINUES` | `CreateRemainingCarts([3, 2, 1])` | Odmówiono wykonania operacji „Groceries M 4”: `SagaFailed(CartsCreationFailure(List("Cannot add cart *M 4")))`, a wywołujący otrzymuje teraz tę odpowiedź |
| 4 | `REVERTING` | `CreateRemainingCarts([3, 2, 1])` | Koszyk 3 został usunięty: `SagaRevertContinues(CreateRemainingCarts([2, 1]))` |
| 5 | `REVERTING` | `CreateRemainingCarts([2, 1])` | Koszyk 2 został usunięty: `SagaRevertContinues(CreateRemainingCarts([1]))` |
| 6 | `REVERTING` | `CreateRemainingCarts([1])` | Koszyk 1 został usunięty: `SagaRevertSucceded`, a wiersz został usunięty |

Spójrz na krok 4: przywracanie zaczyna się od kolejności kroku, który się nie powiódł. Ta kolejność wie, co
kroki przed tym, jak to się stało (trzy wózki), co jest właśnie powodem, dla którego `CreateRemainingCarts` niesie
`createdCarts`. Zauważ również, że liczba kroków rośnie wraz z cofaniem się sagi: każde cofnięcie
krok ma swój własny `SagaStep`.

## Rozpoczęcie sagi i rozmowa z nią

1. **Utwórz stan sagi**, raz:

   ```scala
   import io.reactivecqrs.core.saga.PostgresSagaState

   val sagaState = new PostgresSagaState(mpjsons, typesNames)
   sagaState.initSchema()
   ```

2. **Uruchom aktora sagi**, tak jak robi to `ReactiveTestDomainSpec`:

   ```scala
   val cartsSaga: ActorRef = system.actorOf(
     Rekwizyty (nowy MultipleCartCreatorSaga (sagaState, uidGenerator, carts)), "cartsSaga")
   ```

   Nie wymaga on żadnego specjalnego dyspozytora: operacje zapisu do bazy danych wykonuje samodzielnie dyspozytor blokujący.

3. **Wyślij zamówienia** i czekaj na odpowiedź:

   ```scala
   import io.reactivecqrs.core.saga.SagaResponse
   import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.CreateMultipleCarts

   niejawna wartość timeout: Timeout = Timeout(2.minutes) // wystarczająco długi na całą sagę, a nie na jeden krok

   val answer: Future[SagaResponse] = (cartsSaga ? CreateMultipleCarts(alice, "Groceries", 3)).mapTo[SagaResponse]
   ```

Odpowiedź jest jedna z tych:

- odpowiedź `SagaSucceded` lub `SagaFailed`: tutaj `CartsCreated(...)` lub
  `CartsCreationFailure(...)`;
- `SagaFailureResponse(exceptions)`, gdy krok poszedł nie tak, jak planowano (jego `Future`
  nie powiodło się, wystąpił błąd lub `handleOrder` nie obsługuje zamówienia). Następnie saga wraca do normy, jak w przypadku
  `SagaNieudana`;
- `SagaFailureResponse(exceptions)` również wtedy, gdy nie można uruchomić sagi: nie można uzyskać identyfikatora sagi lub
  nie dało się go zapisać. W takim razie nic nie zrobiono.

Osoba dzwoniąca nie słyszy nic o odwołaniu: odpowiedź została udzielona, ​​gdy krok się nie powiódł.

> **Ostrzeżenie:** odpowiedzi są *przynajmniej raz*. Saga odpowiada dzwoniącemu tuż przed zapisaniem
> zapis, który kończy krok. Jeśli jego wykonawca zatrzyma się w międzyczasie (aplikacja zostanie ponownie uruchomiona lub jej węzeł
> spada), krok uruchamia się ponownie później i ponownie odpowiada. „Zapytanie” zachowuje tylko pierwszą odpowiedź;
> aktor, który wysyła rozkazy i żyje dalej, powinien traktować drugą odpowiedź na tę samą historię jako powtórzenie.

> **Wskazówka:** saga, która wznawia się po ponownym uruchomieniu, odpowiada zapisanemu rozmówcy, jeśli ten rozmówca nadal
> istnieje. „Zapytanie”, na które zabrakło czasu, zniknęło, a odpowiedź trafia do martwych listów. W przypadku pracy, która
> sprawy, sprawdź wynik, gdzie wyląduje (wózki, poprzez projekcję), a nie tylko w
> odpowiedź.

## Jak przechowywany jest postęp

Tabela „sagas” zawiera jeden wiersz na sagę w trakcie lotu: jej nazwę i identyfikator, osobę odbierającą połączenie, fazę,
Numer kroku i aktualne zamówienie w formacie JSON z odpowiednią klasą. Każdy krok przebiega w ten sam sposób:

1. Twój moduł obsługi zostaje uruchomiony i zwraca status.
2. Biegacz zapisuje postęp: następne zamówienie i numer kroku lub przejście do „COFANIA”.
3. Następny krok jest wykonywany.

Po zakończeniu zadania (`SagaSucceded` lub zakończeniu przywracania), wiersz jest usuwany.

W przypadku nieudanego zapisu, ponieważ baza danych jest chwilowo niedostępna, podejmowana jest ponowna próba: po 100 ms, a następnie
dwa razy dłużej za każdym razem, do 30 sekund, łącznie dziesięć prób. Jeśli każda próba się nie powiedzie, saga
pozostaje w ostatnim zapisanym kroku do następnego uruchomienia. Regułą jest wartość `persistRetries`
`SagaActor`, `Backoff` (z `io.reactivecqrs.core.util`), który możesz zastąpić; jest to ustawienie domyślne:

```scala
nadpisz chronioną wartość persistRetries: Backoff = Backoff(100.millis, 30.seconds, maxAttempts = 10)
```

## Po ponownym uruchomieniu

Kiedy aktor sagi zaczyna, odczytuje każdą zapisaną sagę o swoim „imieniu” i kontynuuje każdą z nich od jej
Zapisany krok i faza. Saga, która tworzyła wózki, wciąż je tworzy; ta, która się cofała
kontynuuje cofanie.

Krok, który był uruchomiony, gdy aplikacja została zatrzymana, jest uruchamiany **ponownie**, a jego polecenie może już
zostały zastosowane. To jest temat następnej sekcji.

> **Ostrzeżenie:** nazwa sagi i jej klasy kolejności są przechowywane. Nie zmieniaj nazwy podczas
> sagi są w locie (ich wiersze nie byłyby już dostępne) i nigdy nie dają dwóm aktorom sagi tego samego
> jeden. Traktuj klasy kolejności jak klasy zdarzeń (rozdział 6): saga, której zapisana kolejność nie jest już czytelna
> nie jest wznawiany (biegacz rejestruje to jako błąd i kontynuuje z innymi).
> `StoredDataCheck` (rozdział 18) odczytuje każde zapisane zamówienie na kopii danych produkcyjnych.

> **Ostrzeżenie:** `handleOrder` i `handleRevert` działają na samym aktorze sagi, który obsługuje wszystkie
> saga tego typu. Powinni po prostu zacząć pracę i zwrócić „Przyszłość”: czekanie w środku
> to. I nie przechowuj żadnego stanu w polach aktora: wywołania zwrotne `Przyszłości` działają gdzie indziej, a tylko
> zamówienia przetrwają ponowne uruchomienie.

## Kroki, które mogą zostać uruchomione dwukrotnie: `SagaStep`

Wracając do agenta podróży. Jej połączenie telefoniczne rozłącza się zaraz po tym, jak linia lotnicza powie „zarezerwowane”, zanim ona…
zaznacza pole na swojej liście kontrolnej. Kiedy oddzwoni, nie może rezerwować drugiego lotu, więc
cytuje jej odniesienie: „Rezerwacja 42, krok 1”. Linie lotnicze odpowiadają: „Już zrobione, oto twoje
bilet."

`SagaStep(sagaId, step)` to właśnie to odniesienie. Każdy krok ma jeden, unikalny dla swojej sagi i kroku.
Przekaż go jako identyfikator idempotencji polecenia wysyłanego przez krok; polecenie jest
`IdempotentCommand[SagaStep]` (rozdział 5), ponieważ polecenia koszyka domeny testowej to:

```scala
klasa przypadku CreateShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, name: String)
  rozszerza FirstCommand[ShoppingCart, CustomCommandResponse[_]] o IdempotentCommand[SagaStep]
```

Za pierwszym razem polecenie jest stosowane, a jego odpowiedź jest przechowywana w tej samej transakcji, co jego poprzednia transakcja.
zdarzenia. Gdy ten sam krok zostanie uruchomiony ponownie po ponownym uruchomieniu, ten sam klucz znajdzie zapisaną odpowiedź:
Bus poleceń odpowiada tym poleceniem i nic nie stosuje. W przypadku `CreateShoppingCart` oznacza to to samo.
identyfikator koszyka jako pierwszy. `SagaRecoverySpec` sprawdza dokładnie to: sagę, która stworzyła swoją pierwszą
koszyk, ale nie mógł zapisać tego, co miał, wznawia działanie po ponownym uruchomieniu, a każdy koszyk istnieje tylko raz.

> **Ostrzeżenie:** klucze idempotentności są globalne, we wszystkich typach agregatów. Drugie polecenie wysłane z
> ten sam `SagaStep` jest liczony jako kopia pierwszego: nie jest stosowany i otrzymuje wartość pierwszego
> zapisana odpowiedź. Wyślij jedno idempotentne polecenie na krok; zadanie składające się z dwóch poleceń to dwa kroki.

> **Pamiętaj:** zapisane odpowiedzi są domyślnie przechowywane przez 30 dni (czas przechowywania
> `PostgresCommandResponseState`). Saga, która zostanie wznowiona później, wyśle ​​polecenie swojego kroku
> na nowo.

## Twoja własna saga: zarezerwuj, a następnie pobierz opłatę

Oto kształt sagi z dwoma różnymi krokami. „Zapasy” i „Płatność” to agregaty utworzone
w tym przykładzie użyj poleceń takich jak to (`ReleaseItems` i `ChargePayment` wyglądają tak samo):

```scala
klasa przypadku ReserveItems(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, items: Int)
  rozszerza ConcurrentCommand[Stock, CustomCommandResponse[_]] o IdempotentCommand[SagaStep]
```

```scala
importuj io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
importuj io.reactivecqrs.core.saga._
import org.apache.pekko.actor.ActorRef
zaimportuj org.apache.pekko.pattern.ask
import scala.concurrent.Future

obiekt CheckoutSaga {
  klasa przypadku Checkout(userId: UserId, stockId: AggregateId, paymentId: AggregateId, items: Int, amountCents: Long)
    rozszerza SagaOrder
  prywatna klasa przypadków Charge(userId: UserId, stockId: AggregateId, paymentId: AggregateId, items: Int, amountCents: Long)
    rozszerza SagaInternalOrder

  klasa przypadku CheckedOut(paymentId: AggregateId) rozszerza SagaResponse
  klasa przypadku CheckoutRefused(powody: Lista [String]) rozszerza SagaResponse
}

klasa CheckoutSaga(val state: SagaState, val uidGenerator: ActorRef, stock: ActorRef, payments: ActorRef)
  rozszerza SagaActor {
  importuj CheckoutSaga._
  import context.dispatcher

  nadpisz nazwę wartości = "CheckoutSaga"

  nadpisz def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    przypadek o: Kasa => // krok 1: zarezerwuj przedmioty
      wyślij(stock, ReserveItems(Some(sagaStep), o.userId, o.stockId, o.items)).map {
        przypadek Right(_) => SagaContinues(Opłata(o.userId, o.stockId, o.paymentId, o.items, o.amountCents))
        przypadek Left(powody) => SagaFailed(odmowa płatności(powody))
      }
    przypadek o: Opłata => // krok 2: pobierz opłatę od klienta
      wyślij(płatności, ChargePayment(Some(sagaStep), o.userId, o.paymentId, o.amountCents)).map {
        case Right(_) => SagaSucceded(CheckedOut(o.paymentId))
        przypadek Left(powody) => SagaFailed(odmowa płatności(powody))
      }
  }

  zastąp def handleRevert(sagaStep: SagaStep): OdbierzRevert = {
    case _: Checkout => Future.successful(SagaRevertSucceded) // krok 1 nie powiódł się: nic nie zostało zarezerwowane
    przypadek o: opłata => // krok 2 nie powiódł się: zwróć zarezerwowane przedmioty
      wyślij(stock, ReleaseItems(Some(sagaStep), o.userId, o.stockId, o.items)).map {
        przypadek Right(_) => SagaRevertSucceded
        przypadek Left(powody) => SagaRevertFailed(powody)
      }
  }

  // Każda odpowiedź inna niż udana jest uznawana za odmowę, więc żadna odpowiedź nie zostanie przepuszczona przez rozgrywkę.
  prywatna def send(bus: ActorRef, polecenie: AnyRef): Future[Either[List[String], AggregateId]] =
    (bus ? polecenie).mapTo[CustomCommandResponse[_]].map {
      przypadek SuccessResponse(id, _) => Right(id)
      przypadek FailureResponse(powody) => Left(powody)
      przypadek inny => Lewy(Lista(inne.toString))
    }
}
```

Warto naśladować trzy nawyki:

- **Jedno polecenie na krok.** Każdy krok ma swój własny `SagaStep`, więc każde polecenie ma swój własny klucz.
- **Przypadek cofnięcia dla każdego zamówienia, w którym krok może się nie powieść**, wliczając w to pierwszy krok, nawet jeśli jest
  nic do cofnięcia. Przywrócenie kolejności, której `handleRevert` nie obsługuje, kończy się niepowodzeniem, co powoduje błąd.
  rejestruje jako błąd. (Saga domeny testowej pomija `CreateMultipleCarts`: gdy jest to jej pierwsze
  (jeśli koszyk zostanie odrzucony, błąd zostanie zarejestrowany i nie będzie możliwości jego cofnięcia.)
- **Każda odpowiedź jest obsługiwana.** Odpowiedź, której `match` się nie spodziewa, np.
  `AggregateConcurrentModificationError`, powoduje niepowodzenie kroku `Future`. W kroku do przodu oznacza to
  `SagaFailureResponse` i przywrócenie; w kroku przywracania zobacz następną sekcję.

## Kiedy przywracanie się nie powiedzie

Jeżeli krok przywracania nie powiedzie się (jego `Future` nie powiedzie się, zostanie zgłoszony wyjątek lub zwróci `SagaRevertFailed`),
rejestruje błąd i usuwa sagę. Nic nie ponawia próby przywrócenia, a wywołujący nie otrzymuje ponownie powiadomienia.

Utrudnij więc odrzucanie przywróceń. Przywrócenie w domenie testowej usuwa każdy koszyk w wersji, w której był.
utworzono z: jeśli ktoś w międzyczasie dodał element do tego koszyka, usunięcie spełnia wymagania wersji
konflikt, a powrót do poprzedniego stanu kończy się na tym, pozostawiając starsze wózki na miejscu. Kompensacja wysłana jako
`ConcurrentCommand`, podobnie jak opisany powyżej `ReleaseItems`, nie powoduje problemów z wersją, która została zmieniona.

## Sagi w klastrze

Saga Runner musi zostać uruchomiony raz w całym klastrze. Uruchom go poleceniem `Singletons.start` na każdym węźle.
i wysyłać zamówienia do zwracanego serwera proxy (rozdział 17 wyjaśnia singletony):

```scala
import io.reactivecqrs.core.cluster.Singletons

val cartsSaga: ActorRef = Singletons.start(system, "cartsSaga",
  Rekwizyty (nowy MultipleCartCreatorSaga (sagaState, uidGenerator, carts)))
```

W systemie aktorów, który nie jest klastrem, `Singletons.start` po prostu uruchamia aktora. W klastrze
zamówienia i odpowiedzi przemieszczają się między węzłami, więc muszą to być klasy, które może serializować biblioteka
pisać i czytać (rozdział 17).

## Sagi organizacji

Jeśli rozkazy sagi należą do jednej organizacji, powiedz to i zlikwiduj tę organizację
([rozdział 16](16-organizations.md)) usuwa sagę z resztą. Powiedz również „Wymelduj” powyżej
prowadził swój sklep jako `shop: SpaceId`:

```scala
importuj io.reactivecqrs.api.id.SpaceId

nadpisz chronione def spaceOf(order: SagaInternalOrder): Option[SpaceId] = dopasowanie kolejności {
  przypadek o: Kasa => Some(o.shop)
  przypadek _ => Brak
}
```

Miejsce to jest zapisywane wraz z sagą na początku. Domyślna wartość „Brak” chroni sagę przed czyszczeniem.

**W wersji 1.0:** prostszy sposób pisania sagi, w którym biblioteka jest właścicielem biegacza, a Ty
Opisujemy kroki. W tym rozdziale pokazujemy, co jest budowane dzisiaj.

> **Pod maską:** każdy wynik kroku i każdy zapis postępu wraca do aktora sagi jako
> wiadomość, więc wszystkie decyzje są podejmowane w jej wątku; zapisy są wykonywane przez blokującego dyspozytora.
> Identyfikatory Saga pochodzą z pul pobranych z wyprzedzeniem z generatora identyfikatorów UID. W klastrze programista przechowuje swoje identyfikatory.
> adres dzwoniącego z adresem węzła, dzięki czemu odpowiada on z dowolnego węzła, na którym działa (D46, F136,
> F137). Pełna historia: [ARCHITECTURE.md, sekcja 9](../ARCHITECTURE.md).

## Czego się nauczyłeś

- Saga kieruje zadaniem w agregatach krok po kroku: każdy krok wysyła jedno polecenie i zwraca
  `SagaContinues`, `SagaSucceded` lub `SagaFailed`.
- Wszystko, czego późniejszy krok lub powrót wymaga, odbywa się w kolejności, ponieważ kolejność jest tym, co jest
  przechowywane.
- W przypadku niepowodzenia osoba dzwoniąca otrzymuje natychmiastową odpowiedź, a `handleRevert` cofa wcześniejsze kroki, zaczynając od
  od kolejności kroku, który się nie powiódł.
- Postęp jest zapisywany po każdym kroku; po ponownym uruchomieniu sagi są kontynuowane od zapisanego kroku, który
  biegnie ponownie.
- `SagaStep` jako identyfikator idempotentności sprawia, że ​​powtarzany krok jest nieszkodliwy; wysyłaj jedno polecenie idempotentne na
  krok.
- Odpowiedzi udzielane są co najmniej raz, a nieudane odwołania są rejestrowane, a nie ponawiane.

Dalej: [rozdział 13](13-snapshots.md) szybko przechowuje agregaty o długiej historii.