# Rozdział 15. Przepisywanie historii z rozwagą

Rozdział 1 zawierał obietnicę: wydarzenia są faktami, a usunięcie jest nowym wydarzeniem, a nie gumką.
Rozdział dotyczy jednego narzędzia, które łamie tę obietnicę. Pod koniec będziesz w stanie zmienić
zdarzenia, które są już zapisane (popraw nazwę wszędzie tam, gdzie została zapisana), stwórz modele odczytu
dowiedz się o zmianie i usuń wartość ze wszystkich miejsc, w których biblioteka ją przechowuje, za pomocą *complete
Przepisz*. Obchodź się z tym jak z ostrym nożem: rzadko, rozważnie i z otwartymi oczami.

---

## Płyn korekcyjny, używany oszczędnie

Wracając do rodzinnego pamiętnika. Na pierwszej stronie ktoś napisał, że wózek nazywa się „Spożywcze”. Zwykle
Dodajesz wiersz na dole: „Zmieniono nazwę na Artykuły spożywcze”. Stary wiersz pozostaje i to jest w porządku: jest
co się wydarzyło i pamiętnik jest w tej sprawie szczery.

Ale czasami stare słowa nie mogą pozostać nigdzie. Klient prosi o zapomnienie, a jego imię
jest w pamiętniku. Przepisy dotyczące prywatności mogą wymagać dokładnie tego: ogólnego rozporządzenia o ochronie danych UE,
na przykład daje ludziom prawo do usunięcia swoich danych osobowych
([Rozporządzenie (UE) 2016/679, artykuł 17](https://eur-lex.europa.eu/eli/reg/2016/679/oj)). Następnie
sięgnij po korektor: zmień samą pierwszą stronę i dodaj nową linię
na dole napis „strona 1 została poprawiona”, dzięki czemu każdy, kto czyta pamiętnik, może wrócić do miejsca, w którym przerwał
Słyszał o tym.

To jest **przepisanie historii**: polecenie, które zmienia już zapisane zdarzenia, wraz z nowymi
wydarzenia, o których mówią wszyscy inni.

> **Pamiętaj:** nowe wydarzenie prawie zawsze jest właściwą odpowiedzią. Przepisz historię tylko wtedy, gdy stare
> wartość nie może przetrwać. To, czego prawo wymaga od twojego systemu, to pytanie do twoich doradców prawnych;
> W tym rozdziale pokazano, co biblioteka może z tym zrobić.

## Polecenie przepisywania

Podobnie jak zwykłe polecenia ([rozdział 5](05-command-flavors.md)), polecenia przepisywania występują w dwóch wariantach:

- **`RewriteHistoryCommand`** podaje nazwę oczekiwanej wersji (`expectedVersion`). Jeśli agregat ma
  po przejściu dalej pojawia się odpowiedź `AggregateConcurrentModificationError`, jak zwykłe `Command`.
- **`RewriteHistoryConcurrentCommand`** nie ma oczekiwanej wersji. Działa na dowolnej wersji
  agregat ma i po kolizji biblioteka uruchamia go ponownie, jak `ConcurrentCommand`.

Oba mają jeszcze jeden element, `eventsTypes`: klasy zdarzeń, które chcesz zmienić. Domena testowa ma
po jednym z każdego:

```scala
importuj io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}

klasa przypadku RewriteCartName(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                           nazwa: String) rozszerza RewriteHistoryCommand[ShoppingCart, CustomCommandResponse[_]] {
  nadpisz def eventsTypes = Set(classOf[ShoppingCartCreated])
}

/** Zmienia nazwę koszyka w całej jego historii, niezależnie od jego wersji (jak anonimizacja). */
klasa przypadku RewriteCartNameConcurrently(userId: UserId, aggregateId: AggregateId, name: String)
  rozszerza RewriteHistoryConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] {
  nadpisz def eventsTypes = Set(classOf[ShoppingCartCreated])
}
```

Nazwa koszyka jest zapisana tylko w zdarzeniu jego utworzenia, więc jest to jedyna klasa, jaką wymieniają.

## Obsługa przepisywania

Polecenia przepisywania nie trafiają do `commandHandlers`. Mają własny handler w kontekście,
`rewriteHistoryCommandHandlers`, który odbiera dwie rzeczy: zdarzenia do przepisania i bieżące
państwo.

```scala
nadpisz def rewriteHistoryCommandHandlers = (wydarzenia, koszyk) => {
  przypadek c: RewriteCartName => rewriteCartName(c.name, events)
  przypadek c: RewriteCartNameConcurrently => rewriteCartName(c.name, events)
}
```

`events` to `Iterable[EventWithVersion[ShoppingCart]]`: własne zdarzenia agregatu
Klasy w `eventsTypes`, najpierw najstarsze, każda z własną wersją. Obsługujący domenę testową:

```scala
def rewriteCartName(nazwa: String, zdarzenia: Iterowalne[EventWithVersion[ShoppingCart]]) = {
  val przepisany: Iterable[EventWithVersion[ShoppingCart]] = events.map(ev => ev.event match {
    przypadek e: ShoppingCartCreated => EventWithVersion(ev.version, e.copy(name = name))
    przypadek _ => ev
  })
  RewriteCommandSuccess(rewritten.toSeq, CartNameRewritten(name))
}
```

Odpowiada poleceniem `RewriteCommandSuccess`, które zawiera dwie listy:

1. **przepisane wydarzenia**, każde z wersją zapisanego wydarzenia, które zastępuje;
2. **nowe zdarzenia** do dodania na końcu, tutaj pojedyncze `CartNameRewritten(name)`.

Pełna forma to `RewriteCommandSuccess(eventsRewritten, events, responseInfo)`; krótsze formy przyjmują
jedno nowe zdarzenie lub ich sekwencję, bez informacji o odpowiedzi. Podobnie jak każdy moduł obsługi, moduł obsługi przepisywania
może również powiedzieć nie za pomocą `CommandFailure` lub poczekać za pomocą `AsyncCommandResult`
([rozdział 5](05-command-flavors.md)).

> **Wskazówka:** kontekst, który nigdy nie zastępuje `rewriteHistoryCommandHandlers`, odpowiada na polecenie przepisywania
> z błędem `CommandHandlingError`, a log prosi o zaimplementowanie procedury obsługi. Jeśli widzisz to w
> produkcja, ktoś wysłał przepis do niewłaściwego typu agregatu.

## Wysyłanie

```scala
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

niejawny limit czasu wartości: Limit czasu = Limit czasu (10 sekund)

val answer: Future[CustomCommandResponse[_]] =
  (koszyki ? RewriteCartName(alicja, cartId, AggregateVersion(3), "Spożywcze")).mapTo[CustomCommandResponse[_]]
// Odpowiedź sukcesu (cartId, AggregateVersion (4))
```

Nasz koszyk powstał z literówką, a potem dostał jabłka i pomarańcze. Oto jego dziennik przed i po…
Po:

```
wersja przed po
1 ShoppingCartCreated("Spożywcze") ShoppingCartCreated("Spożywcze") przepisany w miejscu
2 ItemAdded("jabłka") ItemAdded("jabłka")
3 ItemAdded("pomarańcze") ItemAdded("pomarańcze")
4 CartNameRewritten("Groceries") nowe i opublikowane
```

Wersja została przeniesiona z 3 na 4 ze względu na jedno nowe wydarzenie; przepisane wydarzenie zachowało swoją wersję,
wersja 1.

Kolejność działań biblioteki:

1. Właściciel wózka odczytał własne zdarzenia wózka z wymienionych klas i wywołał Twój program obsługi za pomocą
   ich i stanu obecnego.
2. W jednej transakcji zastąpiono zapisane zdarzenia, dodano nowe i usunięto
   migawki ([rozdział 13](13-snapshots.md)) wózka i kopii odczytujących jego historię:
   zostały stworzone na podstawie starych wartości.
3. Po zatwierdzeniu właściciel ponownie odczytuje stan koszyka ze sklepu, odtwarzając poprawiony stan.
   Historia. Właściciele tych kopii zostali zatrzymani, więc przeczytali swoje stany ponownie przy następnej okazji.
   wiadomość.
4. W projekcjach i sagach publikowano tylko nowe wydarzenia.

> **Pod maską:** każde przepisane zdarzenie jest zapisywane z `UPDATE` swojego wiersza w tym samym
> transakcja jako nowe zdarzenia i tylko wtedy, gdy zapisane zdarzenie ma tę samą klasę; w przeciwnym razie
> całe polecenie zostaje cofnięte (decyzja D17, problem F8). Zobacz
> [ARCHITECTURE.md, sekcja 4.5](../ARCHITECTURE.md).

## Zasady gry

1. **Przepisane zdarzenie zachowuje swoją klasę.** Zmiana `ShoppingCartCreated` na `ItemAdded` jest
   odrzucono: polecenie otrzymuje odpowiedź `EventHandlingError` i nic z tego nie jest przechowywane. Historia
   musi nadal oznaczać to, co oznaczało.
2. **Otrzymujesz klasy, które wymieniłeś, i tylko własne zdarzenia zbiorcze.** Przepisanie kopii nigdy nie jest możliwe.
   dociera do wydarzeń, które odczytuje z oryginału ([rozdział 6](06-events.md) wyjaśnia kopie).
   Wydarzenia, które zostały anulowane przez cofnięcie, są uwzględniane: nadal znajdują się w dzienniku, nawet jeśli nie zostały anulowane.
   nie liczę już dłużej.
3. **Zwróć zmienione wydarzenia wraz z ich wersjami.** Wydarzenia niezmienione mogą powrócić w obecnej formie;
   Są one po prostu zapisywane ponownie bez zmian.
4. **Dodaj nowe zdarzenie, jako zasadę.** Przepisanie bez zdarzenia również zostanie zapisane, ale niczego nie opublikuje:
   zobacz ostrzeżenie poniżej.
5. **Koperta metadanych pozostaje.** Ponowne zapisanie zmienia zdarzenie, a nie zapisaną z nim kopertę.
   ([rozdział 6](06-events.md)). Zachowaj wartości, które być może kiedyś będziesz musiał usunąć z metadanych.
6. **Nowe zdarzenia są zwykłymi zdarzeniami.** Są one przechowywane u użytkownika polecenia przepisywania, więc
   Pełnią również funkcję zapisu tego, kto i kiedy poprawił historię. Wmieszaj „WithMetadata” do
   polecenie przepisania, aby zapisać przyczynę, na przykład numer żądania usunięcia.

> **Ostrzeżenie:** przepisanie, którego wynikiem nie jest żadne nowe zdarzenie, powoduje przepisanie zdarzeń, zachowanie wersji i
> nic nie publikuje, więc Twoje projekcje nic nie słyszą i nadal pokazują starą wartość. Dodaj
> zdarzenie kompensujące lub późniejsza odbudowa projekcji. (Przed wersją 1.0 taka przeróbka była odpowiedzią
> `SuccessResponse` i *nic nie zostało zapisane*, problem F146.)

## Informowanie modeli odczytu: zdarzenie kompensujące

Projekcje nigdy nie widzą przepisanego zdarzenia: nie jest ono ponownie publikowane. Lista koszyków, która pokazuje
„Spożywcze” ciągle to pokazuje, chyba że coś mu to podpowiada. To zadanie nowego wydarzenia,
`CartNameRewritten` w domenie testowej: **zdarzenie kompensacyjne**. Każda projekcja, która pokazuje
nazwa koszyka to obsługuje, na przykład:

```scala
przypadek CartNameRewritten(name) =>
  documentStore.updateDocument(0, aggregateId.asLong, _ => Document(name))
```

Własny moduł obsługi zdarzeń agregatu również to stosuje. Ustawia nazwę, którą będzie miało poprawione utworzenie.
wydarzenie już miało miejsce, więc nic się nie zmienia, a historia pozostaje spójna od góry do dołu:

```scala
przypadek e: CartNameRewritten => cartNameRewritten(shoppingCart, e) // shoppingCart.copy(name = e.name)
```

Późniejsza projekcja odbudowana ([rozdział 10](10-rebuilding-projections.md)) odczytuje poprawioną historię
ze sklepu, więc nigdy nie zobaczy starej wartości.

> **Ostrzeżenie:** zdarzenie kompensujące ustala to, co projekcja *pokazuje*, a nie to, co *zachowała*. Jeśli
> projekcja skopiowała starą wartość w inne miejsce (indeks wyszukiwania, dziennik zmian, dokument innej osoby)
> agregacji), jego obsługa zdarzenia kompensującego musi również to wyczyścić, w przeciwnym razie należy odbudować
> projekcja z poprawionej historii ([rozdział 10](10-rebuilding-projections.md)).

## Czego nie osiągnie się zwykłym przepisaniem

Polecenie przepisania zmienia zapisane zdarzenia jednego agregatu, klas, które nazwiesz, i
Stany i migawki z nich zbudowane. Oto miejsce, w którym stara wartość może się nadal ukrywać:

- **Wydarzenia squasha.** Po squashu ([rozdział 14](14-squash.md)) stara nazwa może znajdować się w
  zdarzenie squash: `CartSquashed` zawiera również nazwę koszyka, a `RewriteCartName` zawiera tylko listę
  `ShoppingCartCreated`. Jeśli Twoje agregaty są spłaszczone, wypisz klasę zdarzenia spłaszczenia w
  `eventsTypes` i przepisz go w ten sam sposób
  (`case e: CartSquashed => EventWithVersion(ev.version, e.copy(name = name))`).
- **Archiwum.** Wydarzenia, w których squash został przeniesiony do archiwum ([rozdział 14](14-squash.md)) są zachowywane
  ich stare wartości.
- **Kopie, które stoją samodzielnie.** Kopia, która nadal odczytuje historię oryginału, widzi
  przepisać (jego właściciel nawet przeładowuje). Kopia, którą odłączono przez zgniecenie lub trwałe usunięcie, niesie
  własne podsumowanie starego stanu, którego nie narusza przeróbka oryginału.
- **Odczyt modeli.** Tylko poprzez zdarzenie kompensacyjne lub przebudowę.
- **Wiersze napisane przez Ciebie wraz ze zdarzeniami**, za pośrednictwem modułu piszącego skrzynkę nadawczą (`WithOutboxWriter`).
- **Wszystko inne, co skopiowało wartość**: zdarzenia innych agregatów, zapisane odpowiedzi
  polecenia idempotentne, dane saga, koperty metadanych, logi.

To wystarczy, żeby poprawić literówkę. Aby usunąć wartość, która nie może nigdzie pozostać, istnieje
większe narzędzie.

## Całkowite przepisanie: wymazywanie wszędzie

Wracając do pamiętnika. Rodzina zrobiła jego kserokopie: kopia jednego z kuzynów jest zszyta z oryginałem.
i nadal czyta z niego pierwsze strony; inny kuzyn przepisał pierwsze strony ręcznie do
własny notatnik. Stare strony również przeniesiono do pudełka na strychu (archiwum). Wymazywanie imienia
właściwie oznacza oryginał, pudełko na strychu, obie kopie i każde streszczenie napisane z
ich. To jest **całkowite przepisanie**.

### Krok 1: opisz zmianę jako dane

Zmiana to mała klasa przypadku, która wskazuje, co usunąć. Rozszerza ona `HistoryChange`:

```scala
import io.reactivecqrs.api.HistoryChange

klasa przypadku EraseCustomer(klient: CustomerId) rozszerza HistoryChange
```

### Krok 2: naucz każdego kontekstu, co powoduje zmiana

Kontekst otrzymuje `historyChanges`: w przypadku zmiany, częściową funkcję ze zdarzenia do zdarzenia, które powoduje.
Staje się. Wydarzenia, których nie pasuje, pozostają takie, jakie są.

```scala
nadpisz def historyChanges: HistoryChange => PartialFunction[Event[Order], Event[Order]] = {
  przypadek EraseCustomer(c) => {
    przypadek e: Złożone zamówienie, jeśli e.customer == c => e.copy(customerName = "erased", email = None)
    przypadek s: OrderSquashed jeśli s.customer == c => s.copy(customerName = "erased", email = None)
  }
  case _ => PartialFunction.empty
}
```

Zwróć uwagę na drugi wiersz: obsługuje on również zdarzenie squash, ponieważ po squash
([rozdział 14](14-squash.md)) nazwa może tam być. Jeśli koperty metadanych Twoich zdarzeń
może przechowywać wartość (wpis `aktor` lub `niestandardowy`, [rozdział 6](06-events.md)), nadpisać
`metadataChanges` w ten sam sposób.

> **Pamiętaj:** zmiana musi być *idempotentna*: uruchom ją dwa razy, a drugie uruchomienie nie znajdzie niczego, co mogłoby ją zmienić.
> zmiana. Zastąpienie nazwy słowem „erased” jest idempotentne; dodanie do niej słowa „(erased)” nie jest.

### Krok 3: uruchom

```scala
import io.reactivecqrs.core.rewrite.HistoryRewriter

raport wartości: Future[RewriteReport] =
  nowy HistoryRewriter(zamówienia, eventStore, projekcje = Mapa("zamówienia" -> zamówieniaProjekcja))
    .rewrite(orderId, EraseCustomer(customerId), operator)
```

`orders` to magistrala poleceń typu; `projections` to zarządzane projekcje
([rozdział 10](10-rebuilding-projections.md)), które pokazują wartość według nazwy. Co się dzieje:

1. **Najpierw agregat.** Jego właściciel, pomiędzy poleceniami, stosuje zmianę do każdego zapisanego zdarzenia
   agregat (wliczając zdarzenia squasha) i jego zarchiwizowane zdarzenia w jednej transakcji.
   Wersja się nie zmienia i nic nie jest publikowane. Migawki wykonane ze starych wartości są
   upuszczony, a właściciel ponownie odczytuje jego stan.
2. **Następnie każda kopia.** Kopie, które nadal odczytują całość, kopie, które zostały od niej odłączone (tzw.
   Biblioteka pamięta każdy odłam w *linii*), a także kopie tych odłamów, po jednej na raz. Każda kopia
   własne wydarzenia również mogą mieć wartość.
3. **Następnie modele odczytu.** Każda podana przez Ciebie projekcja odbudowuje każdy zmieniony agregat, w
   miejsce ([rozdział 10](10-rebuilding-projections.md)). Wymaga `clearForRebuild`.
4. **Oraz notatka w dzienniku.** Każde przepisanie agregatu pozostawia wiersz w `history_rewrites`: kto,
   Kiedy, jaka zmiana, jakie zdarzenia, klasy i pola. Nigdy wartości.

Można wyczyścić własne wiersze, które autor skrzynki nadawczej zapisał przy użyciu zdarzeń (`WithOutboxWriter`)
w tej samej transakcji: nadpisz `OutboxWriter.rewrite(aggregateId, change)`.

### Krok 4: przeczytaj raport

`RewriteReport` wyświetla agregaty przepisane (z liczbą zdarzeń i zarchiwizowanych zdarzeń), te
zmiana pozostawiona sama sobie, odmówili (z powodu: zmiana uczyniła wydarzenie innej klasy lub dotknęła
zdarzenie cofnięcia lub duplikacji), projekcje odbudowane i te, które się nie powiodły, oraz `bazy`:
agregaty, które pierwszy odczytuje, a których po ponownym zapisie nie zmienia, ponieważ wartość może mieć
pochodzić stamtąd i tylko ty możesz zdecydować. `report.complete` jest prawdą, gdy nic nie zostało odrzucone i
Każda projekcja jest odbudowana. Jeśli nie, usuń przyczynę i uruchom ją ponownie: zmiana nic nie wykryje.
zmienić tam, gdzie już wykonało swoją pracę.

> **Ostrzeżenie:** całkowite przepisanie obejmuje dane przechowywane przez bibliotekę dla agregatu i jego kopii.
> Nie dociera do innych agregatów, które skopiowały wartość do własnych zdarzeń, danych sagi, przechowywanych
> odpowiedzi poleceń idempotentnych (wygasają), projekcje, których nie nazwałeś, logi lub nie wyeksportowałeś
> pliki. Identyfikatory użytkowników (`events.user_id`) to identyfikatory, a nie dane osobowe, i pozostają; usuń osobę
> we własnym zbiorze użytkowników.

## Usuwanie imienia i nazwiska klienta krok po kroku

1. **Znajdź miejsce, w którym znajduje się wartość**: klasy zdarzeń, które ją przechowują (w tym klasa zdarzeń squash),
   ich metadane, projekcje, które je pokazują, własne tabele i inne agregaty, które zostały skopiowane
   To.
2. **Zapisz zmianę** (`EraseCustomer`) i `historyChanges` każdego kontekstu (i
   `metadataChanges` jeśli to konieczne); nadpisz `OutboxWriter.rewrite` dla własnych wierszy.
3. **Uruchom `HistoryRewriter.rewrite`** dla każdego agregatu zawierającego wartość, nadając mu nazwę
   prognozy, które to pokazują.
4. **Przeczytaj raport.** Jeśli nie jest kompletny, uruchom go ponownie; sprawdź nazwy podanych w nim `baz`.
5. **Sprawdź wynik**: `GetEventsForAggregate` ([rozdział 7](07-reading-the-past.md)) i
   `GetArchivedEvents` zwraca usunięte wartości, podobnie jak projekcje.

## Czego się nauczyłeś

- Przepisanie historii zmienia zdarzenia, które są już zapisane; należy jej używać tylko wtedy, gdy stara wartość musi zostać zachowana.
  nie przetrwać.
- `RewriteHistoryCommand` (z `expectedVersion`) i `RewriteHistoryConcurrentCommand` nadają nazwy
  klasy zdarzeń, które zmieniają w `eventsTypes`; kontekst `rewriteHistoryCommandHandlers` otrzymuje
  te zdarzenia i stan oraz odpowiedzi `RewriteCommandSuccess(rewritten, newEvents)`.
- Przepisane zdarzenie zachowuje swoją klasę i wersję; później cały agregat i kopie, które
  przeczytaj jego historię, przeczytaj jego stany ponownie, a jego migawki zostaną usunięte.
- Publikowane są tylko nowe wydarzenia, więc przepisanie wiąże się z wydarzeniem kompensującym, które
  uchwyt projekcji; przepis bez niego jest przechowywany, ale nikomu o tym nie mówi.
- Całkowite przepisanie (`HistoryChange`, kontekstowe `historyChanges`, `HistoryRewriter`)
  usuwa wartość ze zdarzeń agregatu, zgniata zdarzenia i archiwizuje z każdej dołączonej kopii
  lub odłączony, i od podanych przez Ciebie projekcji, i pozostawia wiersz audytu bez wartości.

Dalej: [rozdział 16](16-organizations.md) otwiera część VI z organizacjami: wielu klientów współdzielących jeden
instalacja.