# Rozdział 15. Przepisywanie historii, ostrożnie

W rozdziale 1 padła obietnica: zdarzenia to fakty, a usunięcie to nowe zdarzenie, a nie gumka do
mazania. Ten rozdział opowiada o jedynym narzędziu, które tę obietnicę łamie. Na jego końcu będziesz
umieć zmieniać zdarzenia, które są już zapisane (poprawić nazwę wszędzie, gdzie kiedykolwiek ją
zapisano), sprawić, by twoje modele odczytu dowiedziały się o zmianie, i za pomocą *pełnego
przepisania* wymazać wartość ze wszystkich miejsc, w których przechowuje ją biblioteka. Obchodź się
z nim jak z ostrym nożem: rzadko, z rozmysłem i z szeroko otwartymi oczami.

---

## Korektor, ale z umiarem

Wróćmy do rodzinnego pamiętnika. Na jego pierwszej stronie ktoś zapisał nazwę koszyka jako
„Grocceries”. Zwykle dopisujesz wtedy na dole linijkę: „zmiana nazwy na Groceries”. Stara linijka
zostaje i to jest w porządku: tak właśnie było, a pamiętnik uczciwie to pokazuje.

Czasem jednak stare słowa nie mogą zostać nigdzie. Klient prosi, żeby o nim zapomnieć, a jego
nazwisko jest w pamiętniku. Przepisy o ochronie prywatności mogą wymagać właśnie tego: na przykład
unijne ogólne rozporządzenie o ochronie danych (RODO) daje ludziom prawo do usunięcia ich danych
osobowych ([rozporządzenie (UE) 2016/679, art. 17](https://eur-lex.europa.eu/eli/reg/2016/679/oj)).
Wtedy sięgasz po korektor: zmieniasz samą pierwszą stronę i dopisujesz na dole nową linijkę
„poprawiono stronę 1”, żeby dowiedział się o tym każdy, kto czyta pamiętnik od miejsca, w którym
ostatnio skończył.

To jest **przepisanie historii**: polecenie, które zmienia już zapisane zdarzenia, razem z nowymi
zdarzeniami, które informują o tym wszystkich pozostałych.

> **Pamiętaj:** nowe zdarzenie to prawie zawsze właściwa odpowiedź. Przepisuj historię tylko wtedy,
> gdy stara wartość nie może przetrwać. Czego prawo wymaga od twojego systemu, to pytanie do twoich
> doradców prawnych; ten rozdział pokazuje, co może w tej sprawie zrobić biblioteka.

## Polecenie przepisania

Tak jak zwykłe polecenia ([rozdział 5](05-command-flavors.md)), polecenia przepisania występują w
dwóch smakach:

- **`RewriteHistoryCommand`** podaje wersję, której oczekuje (`expectedVersion`). Jeśli agregat
  zdążył pójść dalej, polecenie dostaje odpowiedź `AggregateConcurrentModificationError`, tak jak
  zwykłe `Command`.
- **`RewriteHistoryConcurrentCommand`** nie ma oczekiwanej wersji. Działa na tej wersji, którą
  agregat akurat ma, a po kolizji biblioteka uruchamia je jeszcze raz, tak jak `ConcurrentCommand`.

Oba mają jeszcze jedną składową, `eventsTypes`: klasy zdarzeń, które chcesz zmienić. Domena
testowa ma po jednym poleceniu każdego rodzaju:

<!-- snippet: shoppingcart/Commands.scala#rewriteCommands -->
```scala
case class RewriteCartName(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                           name: String) extends RewriteHistoryCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes = Set(classOf[ShoppingCartCreated])
}

/** Renames the cart throughout its history, on whatever version it has (like an anonymization). */
case class RewriteCartNameConcurrently(userId: UserId, aggregateId: AggregateId, name: String)
  extends RewriteHistoryConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes = Set(classOf[ShoppingCartCreated])
}
```

Nazwa koszyka jest zapisana tylko w zdarzeniu jego utworzenia, więc to jedyna klasa, którą
wymieniają.

## Handler przepisania

Polecenia przepisania nie trafiają do `commandHandlers`. Mają w kontekście własny handler,
`rewriteHistoryCommandHandlers`, który dostaje dwie rzeczy: zdarzenia do przepisania i bieżący stan.

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#rewriteHandlers -->
```scala
override def rewriteHistoryCommandHandlers = (events, shoppingCart) => {
  case c: RewriteCartName => rewriteCartName(c.name, events)
  case c: RewriteCartNameConcurrently => rewriteCartName(c.name, events)
}
```

`events` to `Iterable[EventWithVersion[ShoppingCart]]`: własne zdarzenia agregatu z klas wymienionych
w `eventsTypes`, od najstarszego, każde ze swoją wersją. Handler z domeny testowej:

<!-- snippet: shoppingcart/CommandsHandlers.scala#rewriteCartName -->
```scala
def rewriteCartName(name: String, events: Iterable[EventWithVersion[ShoppingCart]]) = {
  val rewritten: Iterable[EventWithVersion[ShoppingCart]] = events.map(ev => ev.event match {
    case e: ShoppingCartCreated => EventWithVersion(ev.version, e.copy(name = name))
    case _ => ev
  })
  RewriteCommandSuccess(rewritten.toSeq, CartNameRewritten(name))
}
```

Odpowiada wynikiem `RewriteCommandSuccess`, który niesie dwie listy:

1. **przepisane zdarzenia**, każde z wersją zapisanego zdarzenia, które zastępuje;
2. **nowe zdarzenia** do dopisania na końcu, tutaj jedno: `CartNameRewritten(name)`.

Pełna postać to `RewriteCommandSuccess(eventsRewritten, events, responseInfo)`; krótsze przyjmują
jedno nowe zdarzenie albo ich sekwencję, bez informacji dołączanej do odpowiedzi. Jak każdy handler,
handler przepisania może też odmówić, zwracając `CommandFailure`, albo dać sobie czas, zwracając
`AsyncCommandResult` ([rozdział 5](05-command-flavors.md)).

> **Wskazówka:** kontekst, który nie nadpisuje `rewriteHistoryCommandHandlers`, odpowiada na
> polecenie przepisania błędem `CommandHandlingError`, a log prosi o zaimplementowanie handlera. Jeśli
> zobaczysz to na produkcji, znaczy to, że ktoś wysłał przepisanie do niewłaściwego typu agregatu.

## Wysyłamy polecenie

Bloki z tego rozdziału działają w `RewritingSpec`
(`examples/src/test/scala/io/reactivecqrs/examples/cart/`), który sprawdza, co robią:

<!-- snippet: cart/RewritingSpec.scala#send -->
```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(10.seconds)

val answer: Future[CustomCommandResponse[_]] =
  (carts ? RewriteCartName(alice, cartId, AggregateVersion(3), "Groceries")).mapTo[CustomCommandResponse[_]]
// SuccessResponse(cartId, AggregateVersion(4))
```

Nasz koszyk powstał z literówką, a potem trafiły do niego jabłka i pomarańcze. Oto jego pamiętnik
przed i po:

```
version  before                                  after
1        ShoppingCartCreated("Grocceries")       ShoppingCartCreated("Groceries")    rewritten in place
2        ItemAdded("apples")                     ItemAdded("apples")
3        ItemAdded("oranges")                    ItemAdded("oranges")
4                                                CartNameRewritten("Groceries")      new, and published
```

Wersja zmieniła się z 3 na 4 za sprawą jednego nowego zdarzenia; przepisane zdarzenie zachowało
swoją wersję, czyli 1.

Co po kolei zrobiła biblioteka:

1. Właściciel koszyka odczytał własne zdarzenia koszyka z wymienionych klas i wywołał z nimi twój
   handler, podając też bieżący stan.
2. W jednej transakcji podmienił zapisane zdarzenia, dopisał nowe i usunął migawki
   ([rozdział 13](13-snapshots.md)) koszyka oraz kopii, które czytają jego historię: powstały one ze
   starych wartości.
3. Po zatwierdzeniu transakcji właściciel odczytał stan koszyka od nowa z magazynu, odtwarzając
   poprawioną historię. Właściciele tamtych kopii zostali zatrzymani, więc przy następnym komunikacie
   odczytują swoje stany od nowa.
4. Projekcjom i sagom zostały opublikowane tylko nowe zdarzenia.

> **Pod maską:** każde przepisane zdarzenie jest zapisywane przez `UPDATE` jego wiersza, w tej samej
> transakcji co nowe zdarzenia, i tylko tam, gdzie zapisane zdarzenie ma tę samą klasę; w przeciwnym
> razie wycofywane jest całe polecenie (decyzja D17, problem F8). Zobacz
> [ARCHITECTURE.md, sekcję 4.5](../ARCHITECTURE.md).

## Zasady gry

1. **Przepisane zdarzenie zachowuje swoją klasę.** Zamiana `ShoppingCartCreated` w `ItemAdded`
   zostanie odrzucona: polecenie dostanie odpowiedź `EventHandlingError` i nic z niego się nie
   zapisze. Historia musi dalej znaczyć to, co znaczyła.
2. **Dostajesz klasy, które wymienisz, i tylko własne zdarzenia agregatu.** Przepisanie kopii nigdy
   nie sięga zdarzeń, które kopia czyta ze swojego oryginału (kopie objaśnia
   [rozdział 6](06-events.md)). Zdarzenia anulowane przez cofnięcie też są w zestawie: wciąż stoją w
   pamiętniku, nawet jeśli już się nie liczą.
3. **Zwróć zmienione zdarzenia razem z ich wersjami.** Niezmienione mogą wrócić takie, jakie są;
   zostaną po prostu zapisane z powrotem bez zmian.
4. **Z zasady dodawaj nowe zdarzenie.** Przepisanie bez niego też się zapisze, ale niczego nie
   opublikuje: zobacz ostrzeżenie niżej.
5. **Koperta metadanych zostaje.** Przepisanie zmienia zdarzenie, a nie kopertę zapisaną razem z nim
   ([rozdział 6](06-events.md)). Nie wkładaj do metadanych wartości, które pewnego dnia być może trzeba
   będzie wymazać.
6. **Nowe zdarzenia to zwykłe zdarzenia.** Zapisują się z użytkownikiem polecenia przepisania, więc
   przy okazji zapamiętują, kto i kiedy poprawił historię. Żeby zapisać także powód, na przykład
   numer wniosku o wymazanie danych, domieszaj do polecenia przepisania `WithMetadata`.

> **Ostrzeżenie:** przepisanie, którego wynik nie zawiera nowego zdarzenia, przepisuje zdarzenia,
> zachowuje wersję i niczego nie publikuje, więc twoje projekcje o niczym się nie dowiadują i dalej
> pokazują starą wartość. Dodaj zdarzenie kompensujące albo przebuduj potem projekcje. (Przed 1.0
> takie przepisanie dostawało odpowiedź `SuccessResponse`, a *nie zapisywało się zupełnie nic*,
> problem F146.)

## Jak dać znać modelom odczytu: zdarzenie kompensujące

Projekcje nigdy nie widzą przepisanego zdarzenia: nie jest ono publikowane jeszcze raz. Lista
koszyków, która pokazuje „Grocceries”, pokazuje to dalej, dopóki coś jej o zmianie nie powie. To
zadanie nowego zdarzenia, w domenie testowej `CartNameRewritten`: **zdarzenia kompensującego**.
Obsługuje je każda projekcja, która pokazuje nazwę koszyka, na przykład lista koszyków z domeny
testowej:

<!-- snippet: shoppingcart/ShoppingCartsListProjection.scala#nameRewritten -->
```scala
case CartNameRewritten(name) =>
  documentStore.updateDocument(0, aggregateId.asLong, _ => Document(name))
```

Stosuje je też własny handler zdarzenia agregatu. Ustawia nazwę, którą poprawione zdarzenie
utworzenia już nadało, więc nic się nie zmienia, a historia pozostaje spójna od góry do dołu:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#nameRewrittenHandler -->
```scala
case e: CartNameRewritten => cartNameRewritten(shoppingCart, e) // shoppingCart.copy(name = e.name)
```

Projekcja przebudowana później ([rozdział 10](10-rebuilding-projections.md)) czyta z magazynu
poprawioną historię, więc starej wartości w ogóle nie zobaczy.

> **Ostrzeżenie:** zdarzenie kompensujące poprawia to, co projekcja *pokazuje*, a nie to, co
> *zachowała*. Jeśli projekcja skopiowała starą wartość gdzie indziej (do indeksu wyszukiwania, do
> dziennika zmian, do dokumentu innego agregatu), jej handler zdarzenia kompensującego musi posprzątać
> i tam, inaczej trzeba przebudować projekcję z poprawionej historii
> ([rozdział 10](10-rebuilding-projections.md)).

## Dokąd zwykłe przepisanie nie sięga

Polecenie przepisania zmienia zapisane zdarzenia jednego agregatu, z klas, które wymienisz, oraz
zbudowane z nich stany i migawki. Oto gdzie stara wartość może się jeszcze ukrywać:

- **Zdarzenia squasha.** Po squashu ([rozdział 14](14-squash.md)) stara nazwa może siedzieć w
  zdarzeniu squasha: `CartSquashed` też przechowuje nazwę koszyka, a `RewriteCartName` wymienia tylko
  `ShoppingCartCreated`. Jeśli zwijasz historię swoich agregatów, wpisz do `eventsTypes` także klasę
  zdarzenia squasha i przepisz je w ten sam sposób
  (`case e: CartSquashed => EventWithVersion(ev.version, e.copy(name = name))`).
- **Archiwum.** Zdarzenia, które squash przeniósł do archiwum ([rozdział 14](14-squash.md)),
  zachowują stare wartości.
- **Kopie, które stoją na własnych nogach.** Kopia, która wciąż czyta historię oryginału, widzi
  przepisanie (jej właściciel nawet wczytuje ją od nowa). Kopia odłączona przez squash albo trwałe
  usunięcie niesie własne podsumowanie starego stanu, a tego przepisanie oryginału nie dotyka.
- **Modele odczytu.** Tylko przez twoje zdarzenie kompensujące albo przebudowę.
- **Wiersze, które twój kod zapisał razem ze zdarzeniami** przez writer outboxu (`WithOutboxWriter`).
- **Wszystko inne, co skopiowało wartość**: zdarzenia innych agregatów, zapisane odpowiedzi poleceń
  idempotentnych, dane sag, koperty metadanych, logi.

Do poprawienia literówki to wystarczy. Do wymazania wartości, która nie może nigdzie przetrwać, służy
większe narzędzie.

## Pełne przepisanie: wymazywanie wszędzie

Wróćmy do pamiętnika. Rodzina zrobiła jego kserokopie: jeden kuzyn ma kopię zszytą z oryginałem i
pierwsze strony wciąż czyta z niego; kuzynka przepisała pierwsze strony ręcznie do własnego zeszytu.
Stare strony trafiły też do pudła na strychu (archiwum). Porządne wymazanie nazwiska obejmuje
oryginał, pudło na strychu, obie kopie i każde podsumowanie z nich spisane. To jest **pełne
przepisanie**.

### Krok 1: opisz zmianę jako dane

Przykładem jest zamówienie, które przechowuje imię i nazwisko oraz adres e-mail klienta: `Order`, ze
zdarzeniami `OrderPlaced` i `OrderSquashed`, w
`examples/src/test/scala/io/reactivecqrs/examples/cart/Orders.scala`. Jego klient prosi, żeby o nim
zapomnieć. Zmiana to mała case class, która mówi, co wymazać. Rozszerza `HistoryChange`:

<!-- snippet: cart/Orders.scala#eraseCustomer -->
```scala
import io.reactivecqrs.api.HistoryChange

case class EraseCustomer(customer: CustomerId) extends HistoryChange
```

### Krok 2: naucz każdy kontekst, co robi zmiana

Kontekst dostaje `historyChanges`: dla danej zmiany funkcję częściową ze zdarzenia w zdarzenie,
którym ono się staje. Zdarzenia, których nie dopasuje, zostają bez zmian.

<!-- snippet: cart/Orders.scala#historyChanges -->
```scala
override def historyChanges: HistoryChange => PartialFunction[Event[Order], Event[Order]] = {
  case EraseCustomer(c) => {
    case e: OrderPlaced if e.customer == c => e.copy(customerName = "erased", email = None)
    case s: OrderSquashed if s.customer == c => s.copy(customerName = "erased", email = None)
  }
  case _ => PartialFunction.empty
}
```

Zwróć uwagę na drugą linijkę: obsługuje też zdarzenie squasha, bo po squashu
([rozdział 14](14-squash.md)) imię i nazwisko może mieszkać właśnie tam. Jeśli koperty metadanych
twoich zdarzeń mogą przechowywać tę wartość (wpis `actor` albo `custom`, [rozdział 6](06-events.md)),
nadpisz w ten sam sposób `metadataChanges`.

> **Pamiętaj:** zmiana musi być *idempotentna*: uruchom ją dwa razy, a drugie uruchomienie nie
> znajdzie nic do zmiany. Zastąpienie imienia i nazwiska napisem „erased” jest idempotentne; dopisanie
> do niego „ (erased)” już nie.

### Krok 3: uruchom

<!-- snippet: cart/RewritingSpec.scala#run -->
```scala
import io.reactivecqrs.core.rewrite.{HistoryRewriter, RewriteReport}

val report: Future[RewriteReport] =
  new HistoryRewriter(orders, eventStore, projections = Map("orders" -> ordersProjection))
    .rewrite(orderId, EraseCustomer(customerId), operator)
```

`orders` to szyna poleceń tego typu; `projections` to projekcje zarządzane
([rozdział 10](10-rebuilding-projections.md)), które pokazują tę wartość, podane według nazw. Co się
dzieje:

1. **Najpierw agregat.** Jego właściciel, między poleceniami, stosuje zmianę do każdego zapisanego
   zdarzenia agregatu (razem ze zdarzeniami squasha) i do jego zarchiwizowanych zdarzeń, w jednej
   transakcji. Wersja się nie zmienia i nic nie jest publikowane. Migawki zrobione ze starych
   wartości są usuwane, a właściciel odczytuje swój stan od nowa.
2. **Potem każda kopia.** Kopie, które wciąż czytają agregat, kopie, które od niego odłączono
   (biblioteka zapamiętuje każde odłączenie w *rodowodzie*), i kopie tych kopii, jedna po drugiej.
   Własne zdarzenia każdej kopii też mogą przechowywać tę wartość.
3. **Potem modele odczytu.** Każda wskazana projekcja przebudowuje w miejscu każdy agregat, który
   przepisanie odwiedziło ([rozdział 10](10-rebuilding-projections.md)): te, które się zmieniły, i te,
   które się nie zmieniły, bo kopia czytająca zmieniony agregat pokazuje jego wartości bez żadnej
   własnej zmiany. Potrzebuje do tego `clearForRebuild`. Projekcja budowana właśnie od nowa też
   dostaje tę przebudowę, zanim jej nowa budowa zacznie obsługiwać zapytania.
4. **Na koniec notatka w dzienniku.** Przepisanie każdego agregatu zostawia wiersz w
   `history_rewrites`: kto, kiedy, jaka zmiana, jakie klasy zdarzeń i pola. Nigdy wartości.

Twoje własne wiersze, które writer outboxu zapisał razem ze zdarzeniami (`WithOutboxWriter`), można
wyczyścić w tej samej transakcji: nadpisz `OutboxWriter.rewrite(aggregateId, change)`.

### Krok 4: przeczytaj raport

`RewriteReport` wymienia przepisane agregaty (z liczbą zdarzeń i zarchiwizowanych zdarzeń), te,
których zmiana nie ruszyła, te odrzucone (z powodem: zmiana zrobiła ze zdarzenia inną klasę albo
dotknęła zdarzenia cofnięcia lub duplikacji), przebudowane projekcje i te, którym się nie udało, oraz
`bases`: agregaty, które czyta pierwszy agregat. Przepisanie ich nie zmienia, bo wartość mogła
przyjść właśnie stamtąd, a zdecydować o tym możesz tylko ty. `report.complete` ma wartość true, gdy
nic nie zostało odrzucone i każda projekcja się przebudowała. Jeśli nie, usuń przyczynę i uruchom
przepisanie jeszcze raz: tam, gdzie zmiana już wykonała swoją pracę, nie znajdzie nic do zmiany, a
projekcje znowu przebudują każdy agregat.

Polecenie, którego handler działa w chwili przepisania, nie zapisze z powrotem starej wartości: jego
zdarzenia zostaną odrzucone przy zatwierdzaniu, jak przy poleceniu, które przegrało wyścig, a
polecenie współbieżne uruchomi się ponownie na stanie po wymazaniu. Dotyczy to także przepisania
wykonanego na innym węźle.

### Gdy zdarzenia nie da się odczytać

Agregat może przechowywać zdarzenie, którego biblioteka nie umie już odczytać: klasę, której pola
zmieniły się tak, że mpjsons za tym nie nadąża, albo wiersz poprawiony ręcznie. Jego właściciel nie
odtworzy agregatu, ale wartość z takiego wiersza i tak musi zniknąć. Daj zmianie postać tekstową dla
takich wierszy: `inUnreadableEvent` dostaje zapisany JSON i zwraca go z wymazaną wartością albo bez
zmian, gdy wiersz jej nie zawiera.

```scala
case class EraseEmail(email: String) extends HistoryChange {
  override def inUnreadableEvent(json: String): Option[String] =
    Some(json.replace("\"" + email + "\"", "\"erased\""))
}
```

Bez niej (domyślnie odpowiada `None`) przepisanie, które natrafi na taki wiersz, zostaje odrzucone i
niczego nie zmienia. Wiersze zmienione jako tekst są liczone w `HistoryRewritten.unreadable`; agregat
pozostaje nieczytelny, dopóki nie naprawisz jego klasy albo wiersza.

> **Ostrzeżenie:** pełne przepisanie obejmuje to, co biblioteka przechowuje dla agregatu i jego
> kopii. Nie sięga innych agregatów, które skopiowały wartość do własnych zdarzeń, danych sag,
> zapisanych odpowiedzi poleceń idempotentnych (te wygasają), projekcji, których nie wskażesz, logów
> ani wyeksportowanych plików. Identyfikatory użytkowników (`events.user_id`) to identyfikatory, a nie
> dane osobowe, i zostają; samą osobę wymaż w jej własnym agregacie użytkownika.

## Wymazujemy imię i nazwisko klienta, krok po kroku

1. **Znajdź, gdzie mieszka wartość**: klasy zdarzeń, które ją przechowują (razem z klasą zdarzenia
   squasha), ich metadane, projekcje, które ją pokazują, twoje własne tabele i inne agregaty, które ją
   skopiowały.
2. **Napisz zmianę** (`EraseCustomer`) i `historyChanges` każdego kontekstu (oraz `metadataChanges`,
   jeśli trzeba); dla własnych wierszy nadpisz `OutboxWriter.rewrite`.
3. **Uruchom `HistoryRewriter.rewrite`** dla każdego agregatu, który przechowuje wartość, wskazując
   projekcje, które ją pokazują.
4. **Przeczytaj raport.** Jeśli nie jest `complete`, uruchom przepisanie jeszcze raz; przyjrzyj się
   wymienionym w nim `bases`.
5. **Sprawdź wynik**: `GetEventsForAggregate` ([rozdział 7](07-reading-the-past.md)) i
   `GetArchivedEvents` zwracają wartości po wymazaniu, i projekcje też.

## Co już umiesz

- Przepisanie historii zmienia zdarzenia, które są już zapisane; używaj go tylko wtedy, gdy stara
  wartość nie może przetrwać.
- `RewriteHistoryCommand` (z `expectedVersion`) i `RewriteHistoryConcurrentCommand` wymieniają w
  `eventsTypes` klasy zdarzeń, które zmieniają; `rewriteHistoryCommandHandlers` kontekstu dostaje te
  zdarzenia i stan, a odpowiada `RewriteCommandSuccess(rewritten, newEvents)`.
- Przepisane zdarzenie zachowuje klasę i wersję; potem agregat i kopie, które czytają jego historię,
  odczytują swoje stany od nowa, a ich migawki są usuwane.
- Publikowane są tylko nowe zdarzenia, więc przepisaniu towarzyszy zdarzenie kompensujące, które
  obsługują twoje projekcje; przepisanie bez niego zostaje zapisane, ale nikogo o niczym nie
  informuje.
- Pełne przepisanie (`HistoryChange`, `historyChanges` kontekstu, `HistoryRewriter`) wymazuje
  wartość ze zdarzeń agregatu, zdarzeń squasha i archiwum, z każdej kopii, dołączonej czy odłączonej,
  oraz z projekcji, które wskażesz, i zostawia wiersz audytu bez wartości.

Dalej: [rozdział 16](16-organizations.md) otwiera część VI tematem organizacji, czyli wielu klientów
na jednej instalacji.
