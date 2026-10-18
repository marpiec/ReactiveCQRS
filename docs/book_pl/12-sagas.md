# Rozdział 12. Sagi: jak sprawić, by agregaty współpracowały

Pod koniec tego rozdziału będziesz umieć napisać sagę: mały proces, który krok po kroku prowadzi
zadanie przez kilka agregatów, pamięta, dokąd doszedł, także po restarcie, i cofa swoją pracę, gdy
któryś krok się nie powiedzie. Przeczytamy wiersz po wierszu sagę z domeny testowej, prześledzimy
dwa jej przebiegi, a potem naszkicujemy własną.

---

## Kiedy jeden agregat nie wystarcza

Agregat pilnuje swoich reguł w pojedynkę (rozdział 1). Niektóre zadania potrzebują jednak kilku
agregatów:

- „utwórz pięć koszyków, po jednym dla każdego sklepu, do którego dowozimy”: pięć agregatów;
- „zarezerwuj towar w magazynie, a potem obciąż klienta”: agregat magazynu i agregat płatności.

Żadna transakcja nie obejmuje ich wszystkich. Jak więc zrobić wszystko albo nic?

Pomyśl o agentce biura podróży, która rezerwuje wyjazd: lot, hotel i samochód. Każda rezerwacja to
osobny telefon, który może się udać albo nie, niezależnie od pozostałych. Agentka prowadzi listę
tego, co już zarezerwowała. Kiedy wypożyczalnia samochodów odmawia, agentka nie zostawia cię z lotem
i hotelem, z których nie skorzystasz: dzwoni jeszcze raz do hotelu oraz do linii lotniczej i odwołuje
rezerwacje, w odwrotnej kolejności.

Taka agentka z listą w ręku to **saga** (zwana też *menedżerem procesu*). Telefony, które cofają
wcześniejszą pracę, to **kompensacje**; w ReactiveCQRS nazywają się **wycofaniami** (ang. *reverts*).
Saga wysyła jedno polecenie naraz, po każdym kroku zapisuje swoją listę w PostgreSQL, a gdy krok się
nie powiedzie, wycofuje to, co zrobiły wcześniejsze kroki.

## Z czego składa się saga

Wszystko, o czym tu mowa, znajduje się w `io.reactivecqrs.core.saga`, z wyjątkiem `SagaStep`, który
jest w `io.reactivecqrs.api`.

| Element | Czym jest |
| --- | --- |
| `SagaDefinition` | to, co piszesz: nazwa sagi, jej kroki i wycofania, w postaci danych i funkcji |
| `SagaRunner` | aktor biblioteki, który wykonuje definicję: jeden aktor na wszystkie sagi o jej nazwie |
| zlecenie, `SagaOrder` | komunikat, którym nadawca uruchamia sagę |
| zlecenie wewnętrzne, `SagaInternalOrder` | komunikat, który saga przekazuje sama sobie na następny krok (`SagaOrder` też nim jest) |
| krok | jedno wywołanie twojego `handleOrder` (albo `handleRevert`) z jednym zleceniem |
| `SagaStep(sagaId, step)` | identyfikator sagi i numer kroku, przekazywane każdemu krokowi |
| odpowiedź, `SagaResponse` | to, co nadawca dostaje z powrotem |
| `SagaState` | miejsce, w którym zapisuje się postęp; `PostgresSagaState` trzyma go w tabeli `sagas` |

Krok zwraca `Future` z jednym z trzech statusów:

- `SagaContinues(order)`: ten krok jest gotowy; następny wykona się z `order`.
- `SagaSucceded(response)`: całe zadanie jest wykonane; nadawca dostaje `response`.
- `SagaFailed(response)`: zadanie się nie powiodło; nadawca dostaje `response`, a saga zaczyna się
  wycofywać.

Krok wycofania zwraca `Future` z jednym z tych statusów:

- `SagaRevertContinues(order)`: tyle już cofnięto; następny krok wycofania wykona się z `order`.
- `SagaRevertSucceded`: wszystko jest cofnięte.
- `SagaRevertFailed(exceptions)`: cofanie się nie powiodło.

> **Wskazówka:** uważaj na pisownię. Biblioteka pisze `SagaSucceded` i `SagaRevertSucceded`, z jednym
> „e” mniej, niż podaje słownik. Niech te nazwy dopisuje za ciebie edytor.

Saga jest zawsze w jednej z trzech **faz** (`SagaPhase`): `CONTINUES`, gdy idzie naprzód,
`REVERTING`, gdy cofa, i `ERROR`, która jest zarezerwowana: runner nigdy nie przenosi do niej sagi,
a zapisana saga znaleziona w tej fazie nie zostaje uruchomiona (przy starcie trafia do logu
ostrzeżenie).

## Saga domeny testowej: wiele koszyków naraz

`MultipleCartCreatorSaga` (w `testdomain/.../shoppingcart/`) tworzy kilka koszyków o nazwach
„Groceries 1”, „Groceries 2” i tak dalej, po jednym na krok. Jeśli któryś z nich zostanie odrzucony,
saga usuwa te, które już utworzyła. Przeczytajmy ją od początku.

### Komunikaty

<!-- snippet: shoppingcart/MultipleCartCreatorSaga.scala#messages -->
```scala
object MultipleCartCreatorSaga {

  case class CreateMultipleCarts(userId: UserId, cartName: String, cartsCount: Int) extends SagaOrder
  private case class CreateRemainingCarts(userId: UserId, cartName: String, cartsCount: Int,
                                          createdCarts: List[AggregateIdWithVersion]) extends SagaInternalOrder

  trait MultipleCartCreatorSagaResponse extends SagaResponse
  case class CartsCreated(carts: List[AggregateIdWithVersion]) extends MultipleCartCreatorSagaResponse
  case class CartsCreationFailure(exceptions: List[String]) extends MultipleCartCreatorSagaResponse
}
```

Zlecenie `CreateMultipleCarts` jest publiczne: wysyłają je nadawcy. Zlecenie `CreateRemainingCarts`
jest prywatne: tylko saga przekazuje je sama sobie, a w środku niesie koszyki utworzone do tej pory.
Każde zlecenie ma `userId`; wymaga tego `SagaInternalOrder`.

> **Pamiętaj:** zlecenie niesie wszystko, czego będzie potrzebował następny krok albo wycofanie. To
> właśnie zlecenia saga zapisuje; nic innego nie przetrwa restartu.

### Definicja

<!-- snippet: shoppingcart/MultipleCartCreatorSaga.scala#definition -->
```scala
class MultipleCartCreatorSaga(shoppingCartCommandBus: ActorRef)(implicit executionContext: ExecutionContext,
                                                                timeout: Timeout) extends SagaDefinition {

  override val name = "MultipleCartCreatorSaga"

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case order: CreateMultipleCarts => handleCreateMultipleCarts(sagaStep, order)
    case order: CreateRemainingCarts => handleCreateRemainingCarts(sagaStep, order)
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case revert: CreateRemainingCarts =>
      revertCreateRemainingCarts(sagaStep, revert.userId, revert.cartName, revert.cartsCount, revert.createdCarts)
  }

  // ... the steps, below
```

Definicja daje runnerowi nazwę i dwie funkcje:

- `name`: nazwa tego rodzaju sagi. Po niej odnajduje się wiersze tych sag w tabeli `sagas`.
- `handleOrder(sagaStep)`: `ReceiveOrder`, czyli
  `PartialFunction[SagaInternalOrder, Future[SagaHandlingStatus]]`.
- `handleRevert(sagaStep)`: `ReceiveRevert`, czyli
  `PartialFunction[SagaInternalOrder, Future[SagaRevertHandlingStatus]]`.

To, czego potrzebują kroki, definicja dostaje w konstruktorze: tutaj szynę poleceń koszyków,
`ExecutionContext`, na którym wykonują się `map` jej obiektów `Future`, oraz `Timeout`, czyli to, jak
długo jej polecenia czekają na odpowiedzi. Definicja nie jest aktorem. Jej funkcje wywołuje runner,
a jedna definicja obsługuje naraz wszystkie sagi o swojej nazwie, więc nie trzyma niczego we
własnych polach.

### Krok

Oba zlecenia trafiają do tej samej metody. Wysyła ona jedno polecenie i zamienia odpowiedź na status:

<!-- snippet: shoppingcart/MultipleCartCreatorSaga.scala#step -->
```scala
private def createShoppingCart(sagaStep: SagaStep, userId: UserId, cartName: String, cartsCount: Int,
                               createdCarts: List[AggregateIdWithVersion]) = {
  val currentCartId = createdCarts.size + 1
  (shoppingCartCommandBus ? CreateShoppingCart(Some(sagaStep), userId, cartName + " " + currentCartId))
    .mapTo[CustomCommandResponse[_]]
    .map {
      case c: SuccessResponse if createdCarts.length + 1 < cartsCount =>
        SagaContinues(CreateRemainingCarts(userId, cartName, cartsCount,
          AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
      case c: SuccessResponse =>
        SagaSucceded(CartsCreated(AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
      case c: FailureResponse =>
        SagaFailed(CartsCreationFailure(c.exceptions))
      case other =>
        throw new SagaStepNotDone("The cart was not created: " + other.getClass.getSimpleName)
    }
}
```

Koszyk powstał, a mają powstać kolejne: idziemy dalej, z nowym koszykiem dopisanym do listy. Powstał
ostatni koszyk: sukces, z całą listą. Koszyk został odrzucony: porażka. Każda inna odpowiedź (błąd,
odrzucenie w trakcie aktualizacji) może się zmienić, gdy polecenie zostanie wysłane jeszcze raz, więc
krok rzuca `SagaStepNotDone`, a runner wykonuje go ponownie z tym samym kluczem (zobacz „Gdy krok się
nie uda”). `?` używa `timeout`, który dostała definicja. `Some(sagaStep)` to identyfikator
idempotencji polecenia; niżej dostanie własną sekcję.

(`handleCreateMultipleCarts` wywołuje tę metodę, gdy `cartsCount > 0`. W przeciwnym razie od razu
zwraca `Future.successful(SagaSucceded(CartsCreationFailure(List("No carts to create"))))`: saga
kończy się normalnie, bo nie ma nic do zrobienia.)

### Wycofanie

<!-- snippet: shoppingcart/MultipleCartCreatorSaga.scala#revert -->
```scala
private def revertCreateRemainingCarts(sagaStep: SagaStep, userId: UserId, cartName: String, cartsCount: Int,
                                       createdCarts: List[AggregateIdWithVersion]): Future[SagaRevertHandlingStatus] = {
  (shoppingCartCommandBus ? DeleteShoppingCart(Some(sagaStep), userId, createdCarts.head.id, createdCarts.head.version))
    .mapTo[CustomCommandResponse[_]]
    .map {
      case c: SuccessResponse if createdCarts.length > 1 =>
        SagaRevertContinues(CreateRemainingCarts(userId, cartName, cartsCount, createdCarts.tail))
      case c: SuccessResponse =>
        SagaRevertSucceded
      case c: FailureResponse =>
        SagaRevertFailed(c.exceptions)
      case other =>
        throw new SagaStepNotDone("The cart was not deleted: " + other.getClass.getSimpleName)
    }
}
```

Jeden koszyk na krok wycofania: usuń najnowszy, potem przejdź do reszty listy. Odmowa kończy
wycofanie porażką; każda inna odpowiedź wykonuje krok wycofania ponownie, jak przy kroku.

### Przebieg, który się udaje

Nadawca wysyła `CreateMultipleCarts(alice, "Groceries", 3)`:

| Krok | Zlecenie | Wysłane polecenie | Status |
| --- | --- | --- | --- |
| 0 | `CreateMultipleCarts` | utwórz „Groceries 1” | `SagaContinues(CreateRemainingCarts(..., [cart 1]))` |
| 1 | `CreateRemainingCarts([cart 1])` | utwórz „Groceries 2” | `SagaContinues(CreateRemainingCarts(..., [cart 2, cart 1]))` |
| 2 | `CreateRemainingCarts([cart 2, cart 1])` | utwórz „Groceries 3” | `SagaSucceded(CartsCreated([cart 3, cart 2, cart 1]))` |

Po kroku 2 nadawca dostaje `CartsCreated` z trzema koszykami (od najnowszego), a wiersz sagi zostaje
usunięty.

### Przebieg, który się nie udaje

Handler poleceń koszyka w domenie testowej ma pułapkę na potrzeby testów: odrzuca każdy koszyk,
którego nazwa kończy się na „M 4”. Nadawca wysyła więc `CreateMultipleCarts(alice, "Groceries M", 5)`:

| Krok | Faza | Zlecenie | Co się dzieje |
| --- | --- | --- | --- |
| od 0 do 2 | `CONTINUES` | jak wyżej | powstają koszyki od „Groceries M 1” do „Groceries M 3” |
| 3 | `CONTINUES` | `CreateRemainingCarts([3, 2, 1])` | „Groceries M 4” zostaje odrzucony: `SagaFailed(CartsCreationFailure(List("Cannot add cart *M 4")))`, a nadawca dostaje tę odpowiedź już teraz |
| 4 | `REVERTING` | `CreateRemainingCarts([3, 2, 1])` | koszyk 3 zostaje usunięty: `SagaRevertContinues(CreateRemainingCarts([2, 1]))` |
| 5 | `REVERTING` | `CreateRemainingCarts([2, 1])` | koszyk 2 zostaje usunięty: `SagaRevertContinues(CreateRemainingCarts([1]))` |
| 6 | `REVERTING` | `CreateRemainingCarts([1])` | koszyk 1 zostaje usunięty: `SagaRevertSucceded`, a wiersz sagi znika |

Spójrz na krok 4: wycofanie zaczyna się od zlecenia kroku, który się nie powiódł. To zlecenie wie, co
zrobiły kroki przed nim (trzy koszyki), i właśnie dlatego `CreateRemainingCarts` niesie
`createdCarts`. Zauważ też, że numery kroków dalej rosną, gdy saga się wycofuje: każdy krok
wycofania ma własny `SagaStep`.

## Jak uruchomić sagę i z nią rozmawiać

Te kroki wykonuje `SagasSpec` (`examples/src/test/scala/io/reactivecqrs/examples/cart/`), który
odgrywa oba przebiegi opisane wyżej i sprawdza, co odpowiadają i co po sobie zostawiają.

1. **Utwórz stan sag**, jeden raz:

   <!-- snippet: cart/SagasSpec.scala#sagaState -->
   ```scala
   import io.reactivecqrs.core.saga.PostgresSagaState

   val sagaState = new PostgresSagaState(mpjsons, typesNames)
   sagaState.initSchema()
   ```

2. **Uruchom runnera**, tak jak robi to `ReactiveTestDomainSpec`:

   <!-- snippet: cart/SagasSpec.scala#runner -->
   ```scala
   import io.reactivecqrs.core.saga.SagaRunner

   val cartsSaga: ActorRef = system.actorOf(SagaRunner.props(
     new MultipleCartCreatorSaga(carts)(system.dispatcher, Timeout(60.seconds)), uidGenerator, sagaState), "cartsSaga")
   ```

   Runner dostaje definicję, aktora generatora uid, którego używają twoje szyny poleceń (rozdział 4)
   i z którego pochodzą identyfikatory sag, oraz stan sag. Nie potrzebuje specjalnego dispatchera:
   zapisy do bazy sam wykonuje na dispatcherze blokującym.

3. **Wysyłaj mu zlecenia** i czekaj na odpowiedź:

   <!-- snippet: cart/SagasSpec.scala#order -->
   ```scala
   import io.reactivecqrs.core.saga.SagaResponse
   import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.CreateMultipleCarts

   implicit val timeout: Timeout = Timeout(2.minutes) // long enough for the whole saga, not one step

   val answer: Future[SagaResponse] = (cartsSaga ? CreateMultipleCarts(alice, "Groceries", 3)).mapTo[SagaResponse]
   ```

Odpowiedź to jedno z poniższych:

- odpowiedź z `SagaSucceded` albo z `SagaFailed`: tutaj `CartsCreated(...)` albo
  `CartsCreationFailure(...)`;
- `SagaFailureResponse(exceptions)`, gdy krok zawiódł w sposób, którego nie przewidział (jego
  `Future` się nie powiódł, krok rzucił wyjątek albo `handleOrder` nie obsługuje zlecenia). Saga
  wtedy się wycofuje, tak jak po `SagaFailed`;
- `SagaFailureResponse(exceptions)` także wtedy, gdy saga nie mogła wystartować: nie udało się
  zdobyć identyfikatora sagi albo nie dało się jej zapisać. Wtedy nic nie zostało zrobione.

O wycofaniu nadawca się nie dowie: odpowiedź dostał już wtedy, gdy krok się nie powiódł.

> **Ostrzeżenie:** odpowiedzi przychodzą *co najmniej raz*. Saga odpowiada nadawcy tuż przed
> zapisem, który kończy krok. Jeśli jej runner zatrzyma się pomiędzy jednym a drugim (aplikacja się
> restartuje albo pada jej węzeł), krok wykona się później jeszcze raz i znowu odpowie. `ask`
> zachowuje tylko pierwszą odpowiedź; aktor, który wysyła zlecenia i działa dalej, powinien
> traktować drugą odpowiedź od tej samej sagi jako powtórkę.

> **Wskazówka:** saga wznowiona po restarcie odpowiada zapisanemu nadawcy, jeśli ten jeszcze
> istnieje. `ask`, któremu skończył się czas, już nie istnieje, a odpowiedź trafia do dead letters.
> Gdy wynik ma znaczenie, sprawdzaj go tam, gdzie ląduje (koszyki, przez projekcję), a nie tylko w
> odpowiedzi.

## Jak zapisywany jest postęp

Tabela `sagas` ma po jednym wierszu na każdą sagę w toku: jej nazwę i identyfikator, nadawcę, któremu
trzeba odpowiedzieć, fazę, numer kroku i bieżące zlecenie, zapisane jako JSON razem z jego klasą.
Każdy krok przebiega tak samo:

1. Twój handler się wykonuje i zwraca status.
2. Runner zapisuje postęp: następne zlecenie i numer kroku albo przejście do `REVERTING`.
3. Wykonuje się następny krok.

Gdy zadanie się kończy (`SagaSucceded` albo zakończone wycofanie), wiersz zostaje usunięty; wiersz
sagi uruchomionej zleceniem z kluczem zostaje, jako zakończony, razem z odpowiedzią (zobacz „Zlecenia
wysłane dwa razy” niżej). Nadawca dowiaduje się o porażce dopiero wtedy, gdy przejście sagi do
`REVERTING` jest zapisane, więc to, co usłyszał, zgadza się z tym, co mówi magazyn.

Zapis, który się nie uda, bo baza na chwilę zniknęła, jest ponawiany: po 100 ms, potem za każdym
razem po dwa razy dłuższej przerwie, aż do 30 sekund, łącznie dziesięć prób. Potem runner ponawia
go co 30 sekund, zapisując błąd w logu, dopóki zapis się nie uda: saga nigdy nie przepada, póki jej
runner działa. (Nowa saga, której pierwszy zapis nigdy się nie udał, jest za to odrzucana: nadawca
dostaje `SagaFailureResponse`.) Tę regułę wyznacza wartość `persistRetries` definicji, czyli
`Backoff` (z `io.reactivecqrs.core.util`), który możesz nadpisać; domyślnie wygląda tak:

<!-- snippet: cart/SagasSpec.scala#persistRetries -->
```scala
override val persistRetries: Backoff = Backoff(100.millis, 30.seconds, maxAttempts = 10)
```

## Po restarcie

Gdy runner sagi startuje, wczytuje wszystkie zapisane sagi o nazwie podanej w `name` jego definicji
i każdą kontynuuje od zapisanego kroku i fazy. Saga, która tworzyła koszyki, tworzy je dalej; ta,
która się wycofywała, wycofuje się dalej.

Krok, który trwał w chwili zatrzymania aplikacji, wykona się **ponownie**, a jego polecenie mogło
już zostać zastosowane. Tym zajmuje się następna sekcja.

> **Ostrzeżenie:** `name` sagi i klasy jej zleceń trafiają do bazy. Nie zmieniaj `name`, gdy są sagi
> w toku (ich wiersze przestałyby się odnajdywać), i nigdy nie dawaj tej samej nazwy dwóm
> definicjom. Traktuj klasy zleceń jak klasy zdarzeń (rozdział 6): saga, której zapisanego zlecenia
> nie da się już odczytać, nie zostanie wznowiona (runner zapisze to w logu jako błąd i zajmie się
> pozostałymi). `StoredDataCheck` (rozdział 18) odczytuje każde zapisane zlecenie na kopii twoich
> danych produkcyjnych.

> **Ostrzeżenie:** runner wywołuje `handleOrder` i `handleRevert` na własnym wątku, który obsługuje
> wszystkie sagi o tej nazwie. Te funkcje powinny tylko rozpocząć pracę i zwrócić `Future`: czekanie
> umieść w jego środku. I nie trzymaj stanu w polach definicji: kroki wielu sag wykonują się
> jednocześnie, a restart przetrwają tylko zlecenia.

## Kroki, które mogą wykonać się dwa razy: `SagaStep`

Wróćmy do agentki biura podróży. Połączenie zrywa się tuż po tym, jak linia lotnicza mówi
„zarezerwowane”, zanim agentka zdąży odhaczyć pozycję na liście. Gdy oddzwania, nie może
zarezerwować drugiego lotu, więc podaje swój numer: „rezerwacja 42, krok 1”. Linia odpowiada: „już
zrobione, oto bilet”.

`SagaStep(sagaId, step)` to właśnie taki numer. Każdy krok dostaje swój, niepowtarzalny dla jego
sagi i kroku. Przekaż go jako identyfikator idempotencji polecenia, które wysyła krok; samo
polecenie jest wtedy `IdempotentCommand[SagaStep]` (rozdział 5), tak jak polecenia koszyka w domenie
testowej:

<!-- snippet: shoppingcart/Commands.scala#create -->
```scala
case class CreateShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
```

Za pierwszym razem polecenie zostaje zastosowane, a jego odpowiedź zapisuje się w tej samej
transakcji co jego zdarzenia. Gdy ten sam krok wykona się znowu po restarcie, ten sam klucz znajduje
zapisaną odpowiedź: szyna poleceń odpowiada nią i niczego nie stosuje. Dla `CreateShoppingCart`
oznacza to ten sam identyfikator koszyka co za pierwszym razem. Dokładnie to sprawdza
`SagaRecoverySpec`: saga, która utworzyła pierwszy koszyk, ale nie zdołała zapisać, że to zrobiła,
wznawia się po restarcie, a każdy koszyk istnieje tylko raz.

> **Ostrzeżenie:** klucze idempotencji są globalne, wspólne dla wszystkich typów agregatów. Drugie
> polecenie wysłane z tym samym `SagaStep` liczy się jako kopia pierwszego: nie zostanie zastosowane
> i dostanie zapisaną odpowiedź pierwszego. Wysyłaj jedno idempotentne polecenie na krok; zadanie z
> dwoma poleceniami to dwa kroki.

> **Pamiętaj:** zapisane odpowiedzi są domyślnie przechowywane przez 30 dni (okres retencji
> `PostgresCommandResponseState`). Saga wznowiona później wysłałaby polecenie swojego kroku od nowa.

## Zlecenia wysłane dwa razy

`SagaStep` sprawia, że krok można bezpiecznie wykonać dwa razy. Samo zlecenie to inna sprawa: nadawca,
który w porę nie dostaje odpowiedzi (przeniesienie runnera na inny węzeł zajmuje sporą część minuty,
rozdział 17), wysyła zlecenie ponownie, a każde zlecenie uruchamia własną sagę, która znowu rezerwuje i
pobiera opłatę.

Daj takiemu zleceniu klucz wybrany przez nadawcę, ten sam dla każdej kopii, rozszerzając
`IdempotentSagaOrder`:

```scala
case class PayInvoice(userId: UserId, invoiceId: AggregateId, orderKey: String) extends IdempotentSagaOrder
```

Drugie zlecenie z tym samym `orderKey` (najwyżej 256 znaków) niczego nie uruchamia: dopóki pierwsza
saga działa, dołącza do niej i obaj nadawcy dostają jej odpowiedź; gdy saga już się zakończyła, dostaje
odpowiedź, którą dostał pierwszy nadawca. Wiersz zakończonej sagi trzyma tę odpowiedź przez
`answersKeptFor` (domyślnie 30 dni), potem znika. Klucz wywodź z tego, co nadawca ma na myśli, na
przykład `"pay-" + invoiceId`, nigdy z danych osobowych: jest zapisywany.

## Twoja własna saga: rezerwacja, potem płatność

Oto szkielet sagi z dwoma różnymi krokami. `Stock` i `Payment` to agregaty wymyślone na potrzeby
tego przykładu, z poleceniami takimi jak to (`ReleaseItems` i `ChargePayment` wyglądają tak samo):

<!-- snippet: e4/Shop.scala#reserveItems -->
```scala
case class ReserveItems(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, items: Int)
  extends ConcurrentCommand[Stock, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
```

<!-- snippet: e4/CheckoutSaga.scala#saga -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.saga._
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

object CheckoutSaga {
  case class Checkout(userId: UserId, stockId: AggregateId, paymentId: AggregateId, items: Int, amountCents: Long)
    extends SagaOrder
  // Stored with the saga, like every order: package-private, not private, so the stored-formats test (E11) can make one.
  private[examples] case class Charge(userId: UserId, stockId: AggregateId, paymentId: AggregateId, items: Int, amountCents: Long)
    extends SagaInternalOrder

  case class CheckedOut(paymentId: AggregateId) extends SagaResponse
  case class CheckoutRefused(reasons: List[String]) extends SagaResponse
}

class CheckoutSaga(stock: ActorRef, payments: ActorRef)(implicit executionContext: ExecutionContext, timeout: Timeout)
  extends SagaDefinition {
  import CheckoutSaga._

  override val name = "CheckoutSaga"

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case o: Checkout => // step 1: reserve the items
      send(stock, ReserveItems(Some(sagaStep), o.userId, o.stockId, o.items)).map {
        case Right(_) => SagaContinues(Charge(o.userId, o.stockId, o.paymentId, o.items, o.amountCents))
        case Left(reasons) => SagaFailed(CheckoutRefused(reasons))
      }
    case o: Charge => // step 2: charge the customer
      send(payments, ChargePayment(Some(sagaStep), o.userId, o.paymentId, o.amountCents)).map {
        case Right(_) => SagaSucceded(CheckedOut(o.paymentId))
        case Left(reasons) => SagaFailed(CheckoutRefused(reasons))
      }
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case _: Checkout => Future.successful(SagaRevertSucceded) // step 1 failed: nothing was reserved
    case o: Charge => // step 2 failed: give the reserved items back
      send(stock, ReleaseItems(Some(sagaStep), o.userId, o.stockId, o.items)).map {
        case Right(_) => SagaRevertSucceded
        case Left(reasons) => SagaRevertFailed(reasons)
      }
  }

  // A refusal ends the step; any other answer (an error, a rejection during an upgrade) may change when the command is
  // sent again, so the step runs again with the same key (F205).
  private def send(bus: ActorRef, command: AnyRef): Future[Either[List[String], AggregateId]] =
    (bus ? command).mapTo[CustomCommandResponse[_]].map {
      case SuccessResponse(id, _) => Right(id)
      case FailureResponse(reasons) => Left(reasons)
      case other => throw new SagaStepNotDone(other.getClass.getSimpleName)
    }
}
```

Warto przejąć cztery nawyki:

- **Jedno polecenie na krok.** Każdy krok ma własny `SagaStep`, więc każde polecenie ma własny klucz.
- **Zlecenia wewnętrzne, które test może utworzyć.** Runner zapisuje każde zlecenie, także
  wewnętrzne, a test zapisanych formatów z [rozdziału 19](19-testing.md) potrzebuje próbki każdego z
  nich: niech będą prywatne dla pakietu (tutaj `private[examples]`), a nie po prostu prywatne.
- **Przypadek wycofania dla każdego zlecenia, na którym krok może się nie powieść,** łącznie z
  pierwszym, nawet gdy nie ma czego cofać. Wycofanie zlecenia, którego `handleRevert` nie obsługuje,
  kończy się porażką, a runner zapisuje ją w logu jako błąd. (Saga domeny testowej pomija
  `CreateMultipleCarts`: gdy zostanie odrzucony jej pierwszy koszyk, w logu pojawia się ten błąd,
  choć nie ma czego cofać.)
- **Obsłużona każda odpowiedź.** Odpowiedź, której `match` się nie spodziewa, na przykład
  `AggregateConcurrentModificationError`, sprawia, że `Future` kroku kończy się porażką. W zwykłym
  kroku oznacza to `SagaFailureResponse` i wycofanie; o kroku wycofania mówi następna sekcja.

## Gdy krok się nie uda

`Future` kroku, który zawiedzie (zapytanie, któremu minął czas, wyjątek, handler, który nie przyjmuje
zlecenia), nie kończy sagi od razu. Runner wykonuje ten sam krok jeszcze raz, z tym samym `SagaStep`,
po 1 sekundzie, potem za każdym razem po dwa razy dłuższej przerwie, aż do 30 sekund, łącznie pięć
prób (`stepRetries` definicji). Polecenie zastosowane z opóźnieniem, już po tym, jak zapytanie się
poddało, odpowiada wtedy zapisaną odpowiedzią, a saga idzie dalej. Dopiero krok, który wciąż zawodzi,
każe sadze się wycofać, a jej nadawca dostaje `SagaFailureResponse`.

Zdecyduj więc, które odpowiedzi są odmowami. Zwracaj `SagaFailed` przy odmowie, która się nie zmieni
(`FailureResponse`: towaru nie ma). Pozwól `Future` zawieść przy odpowiedzi, która może się zmienić,
gdy polecenie zostanie wysłane jeszcze raz: `CommandRejected` (w trakcie aktualizacji, rozdział 17),
`CommandHandlingError`, `EventHandlingError` albo brak odpowiedzi w czasie. Przykłady z tego rozdziału
robią jedno i drugie.

## Gdy wycofanie się nie uda

Krok wycofania, którego `Future` zawiedzie, wykonuje się ponownie w ten sam sposób. Jeśli dalej
zawodzi albo zwraca `SagaRevertFailed`, runner zapisuje błąd w logu i kończy sagę; nadawca nie dostaje
już żadnej wiadomości.

Dlatego pisz wycofania tak, żeby trudno je było odrzucić. Wycofanie w domenie testowej usuwa każdy
koszyk w wersji, z którą ten koszyk powstał: jeśli ktoś w międzyczasie dodał do niego produkt,
usuwanie trafia na konflikt wersji i wycofanie kończy się w tym miejscu, a starsze koszyki zostają.
Kompensacja wysłana jako `ConcurrentCommand`, jak `ReleaseItems` wyżej, nie potknie się o wersję,
która poszła do przodu.

## Sagi w klastrze

W całym klastrze runner sagi musi działać w jednym egzemplarzu. Uruchamiaj go przez
`SagaRunner.start` na każdym węźle i wysyłaj zlecenia do zwróconego proxy (singletony wyjaśnia
rozdział 17):

<!-- snippet: cart/SagasSpec.scala#start -->
```scala
val cartsSaga: ActorRef = SagaRunner.start(system,
  new MultipleCartCreatorSaga(carts)(system.dispatcher, Timeout(60.seconds)), uidGenerator, sagaState)
```

Singleton dostaje nazwę od `name` definicji (podaj `actorName`, gdy ta nazwa nie jest poprawną
nazwą aktora). W systemie aktorów, który nie jest klastrem, `SagaRunner.start` po prostu uruchamia
aktora. W klastrze zlecenia i odpowiedzi podróżują między węzłami, więc muszą być klasami, które
serializer biblioteki potrafi zapisać i odczytać (rozdział 17).

Gdy runner startuje albo przenosi się na inny węzeł, czyta zapisane sagi i prowadzi je dalej, a sagi,
którą już wykonuje, nigdy nie uruchamia drugi raz. Każdy zapis postępu podaje fazę i krok, z
których przechodzi, więc nieaktualna kopia sagi (spóźniony zapis starego runnera) zatrzymuje się,
zamiast nadpisać nowszą. `GetSagaStatus` (w `SagaRunner`) odpowiada, ile sag działa i ile zleceń czeka na
identyfikator sagi; sagę, której nie da się w tej chwili zapisać, widać w metryce
`reactivecqrs.saga.write.failed` i w logu.

## Sagi organizacji

Gdy zlecenia sagi należą do jednej organizacji, zaznacz to, a usunięcie tej organizacji
([rozdział 16](16-organizations.md)) usunie sagę razem z resztą. Załóżmy, że `Checkout` z przykładu
wyżej niesie też swój sklep jako `shop: SpaceId`:

```scala
import io.reactivecqrs.api.id.SpaceId

override def spaceOf(order: SagaInternalOrder): Option[SpaceId] = order match {
  case o: Checkout => Some(o.shop)
  case _ => None
}
```

Przestrzeń zapisuje się razem z sagą, gdy ta startuje. `None`, wartość domyślna, wyłącza sagę z
usuwania organizacji.

## Jak przenieść sagę napisaną jako `SagaActor`

Przed wersją 1.0 saga była aktorem: klasą rozszerzającą `SagaActor`, która podawała `state`,
`uidGenerator`, `name` i obie funkcje. W 1.0 taka saga wciąż działa, w środku na tym samym runnerze,
ale jest oznaczona jako przestarzała: jej kod dzieli aktora z runnerem, więc może dotykać tego,
czego powinien dotykać tylko runner. Każdą taką sagę przeniesiesz w pięciu drobnych zmianach:

1. `extends SagaActor` zmienia się w `extends SagaDefinition`.
2. `val state` i `val uidGenerator` znikają z konstruktora: trafiają do `SagaRunner.props` albo
   `SagaRunner.start`.
3. To, czego kroki używały z aktora, przychodzi teraz przez konstruktor: `import context.dispatcher`
   zamienia się w niejawny (implicit) `ExecutionContext`, a 60-sekundowy timeout zapytań ask aktora
   – w niejawny `Timeout`.
4. `override protected` przy `spaceOf` albo `persistRetries` zmienia się w `override`.
5. Tam, gdzie saga była uruchamiana, `Props(new MySaga(state, uidGenerator, ...))` zmienia się w
   `SagaRunner.props(new MySaga(...)(system.dispatcher, Timeout(60.seconds)), uidGenerator, state)`.

Zachowaj `name` i klasy zleceń: to pod nimi przechowywane są zapisane sagi, a sagi w toku wznowią
się w runnerze o tej samej nazwie. Dokładnie to sprawdza `SagaRunnerSpec`.

> **Pod maską:** każdy wynik kroku i każdy zapis postępu wraca do runnera jako komunikat, więc
> wszystkie decyzje zapadają na jego wątku; zapisy wykonują się na dispatcherze blokującym.
> Identyfikatory sag pochodzą z pul pobieranych z wyprzedzeniem z generatora uid. W klastrze runner
> zapisuje adres nadawcy razem z adresem węzła, więc odpowiada z dowolnego węzła, na którym akurat
> działa (D46, F136, F137). Pełna historia: [ARCHITECTURE.md, sekcja 9](../ARCHITECTURE.md).

## Co już umiesz

- Saga prowadzi zadanie przez kilka agregatów krok po kroku: każdy krok wysyła jedno polecenie i
  zwraca `SagaContinues`, `SagaSucceded` albo `SagaFailed`.
- Piszesz `SagaDefinition` (nazwę, kroki i wycofania); wykonuje ją `SagaRunner` z biblioteki,
  uruchamiany przez `SagaRunner.props` albo, w jednym egzemplarzu na klaster, przez
  `SagaRunner.start`.
- Wszystko, czego potrzebuje późniejszy krok albo wycofanie, podróżuje w zleceniach, bo to zlecenia
  są zapisywane.
- Przy porażce nadawca dostaje odpowiedź od razu, a `handleRevert` cofa wcześniejsze kroki,
  zaczynając od zlecenia kroku, który się nie powiódł.
- Postęp zapisuje się po każdym kroku; po restarcie sagi idą dalej od zapisanego kroku, który
  wykonuje się ponownie.
- `SagaStep` jako identyfikator idempotencji sprawia, że powtórzony krok nie szkodzi; wysyłaj jedno
  idempotentne polecenie na krok.
- Odpowiedzi przychodzą co najmniej raz, a nieudane wycofania trafiają do logu, bez ponawiania.

Dalej: [rozdział 13](13-snapshots.md) sprawi, że agregaty z długą historią pozostaną szybkie.
