# Rozdział 20. Ulotne agregaty: szkice, które żyją w pamięci

Nie wszystko zasługuje na miejsce w kalendarzu. Klient wypełnia koszyk i odchodzi; menedżer
bawi się projektem budżetu i zamyka kartę. Przechowuje każde z tych kliknięć na zawsze, z
projekcje i sagi, słysząc o każdym z nich, to dużo tuszu na papierze. Do końca tego
w tym rozdziale będziesz mógł uruchamiać agregaty, które znajdują się tylko w pamięci jednej maszyny, zachowaj te,
które okazują się istotne, a pozostałe pozwalają im zniknąć.

---

## Brudny papier

**Agregat efemeryczny** to zwykły agregat (Twój kontekst, Twoje polecenia, Twoje zdarzenia, Twoje
(reguły), które znajdują się w pamięci tylko jednego węzła. Nic nie jest zapisywane w PostgreSQL, nic nie jest
Opublikowane w projekcjach lub sagach, a kiedy węzeł się zatrzyma, znika. Pomyśl o szkicu ołówkiem na
papier roboczy: możesz go zmieniać tak często, jak chcesz, a gdy jest dobry, przepisujesz go czysto,
do pamiętnika. Ten egzemplarz nazywa się **materializowanie**.

## Krok 1: tymczasowa magistrala poleceń

Agregaty efemeryczne mają własną magistralę poleceń, obok zwykłej magistrali dla tego samego typu:

```scala
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor

val carts = system.actorOf(
  AggregateCommandBusActor(nowy ShoppingCartAggregateContext, uidGenerator, eventStore, odpowiedzi, eventBus), "koszyki")

val draftCarts = system.actorOf(
  AggregateCommandBusActor.ephemeral(nowy ShoppingCartAggregateContext, persistentBus = carts, uidGenerator, mpjsons),
  „szkicowe wózki”)
```

Przyjmuje ten sam kontekst, trwały autobus tego typu (to tam trafiają zmaterializowane wózki),
Generator identyfikatorów (dla identyfikatorów poleceń) i pliki `MPJsons` (aby sprawdzić rozmiary zdarzeń, poniżej). Nie ma żadnych zdarzeń
przechowuje stan odpowiedzi lub parametry magistrali zdarzeń: przechowuje je wszystkie w pamięci, dla siebie.

## Krok 2: używaj go jak każdego innego autobusu

Wyślij mu te same polecenia:

```scala
val draft = send(draftCarts, CreateShoppingCart(None, alice, "Może później"))
// SuccessResponse(Id(-38654705663), Wersja(1))
send(draftCarts, AddItem(alicja, draft.aggregateId, draft.aggregateVersion, "jabłka"))
// SuccessResponse(Id(-38654705663), Wersja(2))
```

Zwróć uwagę na **ujemne identyfikatory**: w ten sposób Ty i Twój interfejs użytkownika możecie odróżnić wersję roboczą od zapisanej
Wszystko inne działa tak, jak to znasz: decydują Twoje programy obsługi poleceń, zmieniają się programy obsługi zdarzeń
stan, nieaktualny `expectedVersion` otrzymuje `AggregateConcurrentModificationError`, `GetAggregate`
odpowiada na stan, a nawet `GetEventsForAggregate` pokazuje zdarzenia projektu.

> **Pamiętaj:** szyna efemeryczna sprawdza to, co sprawdziłby PostgreSQL: JSON zdarzenia może nie być
> dłuższy niż limit dla jego klasy, a agregat nie może mieć więcej zdarzeń niż wersja
> limit ([rozdział 6](06-events.md)). Projekt działający w pamięci można zatem zawsze zapisać.

## Krok 3: zachowaj to dzięki Materialize

Kiedy klient w końcu dokonuje zakupu:

```scala
(draftCarts ? Materialize(draft.aggregateId, alice)).mapTo[MaterializeResponse]
// Zmaterializowane(Id(-38654705663), Id(1001))
```

Biblioteka przyjmuje aktualny stan projektu i zamienia go na zdarzenie **squash** typu
([rozdział 14](14-squash.md)) sprawdza, czy zdarzenie zwraca dokładnie taki stan i ma
Trwały autobus przechowuje go jako pierwsze zdarzenie nowego koszyka, tutaj `1001`. Następnie projekt jest
Usunięto: polecenia dla jego ujemnego identyfikatora otrzymują `EphemeralAggregateUnknown`. Od teraz pracujesz z
`1001` na wytrwałym autobusie, a projekcje i sagi słyszą o nim jak o każdym nowym wózku.

Kilka szczegółów, które zapewniają bezpieczeństwo:

- Podczas trwania materializacji na polecenia dotyczące projektu odpowiadamy „CommandRejected”, więc co jest
  „Stored” to dokładnie to, co widział dzwoniący. Wyślij go ponownie do nowego koszyka.
- Jeśli polecenia są nadal uruchamiane w projekcie lub typ nie ma zdarzenia squash, odpowiedź brzmi:
  `MaterializeRefused(id, reason)` i projekt pozostaje taki sam.
- Materializacja jest idempotentna: jeżeli odpowiedź nadejdzie później i żądanie zostanie wysłane ponownie,
  Biblioteka rozpoznaje go i odpowiada tym samym nowym identyfikatorem zamiast przechowywać drugi koszyk.

> **Wskazówka:** zapisany koszyk zaczyna się od wersji 1, z jednym zdarzeniem podsumowującym: kliknięcia wersji roboczej nie są
> zachowane. Właśnie o to chodzi w brudnopisie. Jeśli potrzebujesz kliknięć, powinny znaleźć się w kalendarzu.
> zacznij.

## Krok 4: odpuść sobie

Wersje robocze, których nikt nie rusza, nie gromadzą się w nieskończoność:

- **Czas bezczynności**: projekt nieużywany przez `idleTime` (domyślnie 12 godzin) jest usuwany. Magistrala nigdy nie jest usuwana.
  jeden wcześniej niż limit czasu polecenia kontekstu, więc polecenie, które jest nadal uruchomione, nie może zapisać
  do projektu, który już nie istnieje.
- **Maksymalnie**: magistrala może pomieścić maksymalnie `maxAggregates` projektów (domyślnie 100 000); nowy powyżej
  odpowiedź brzmi `CommandRejected`.

```scala
AggregateCommandBusActor.ephemeral(context, persistentBus = carts, uidGenerator, mpjsons,
  maxAggregates = 10000, idleTime = 2 godziny)
```

## Kiedy projekt znika

Pamięć jest krucha i biblioteka mówi o tym wyraźnie:

| Odpowiedź | Oznacza |
|---|---|
| `EphemeralAggregateLost(id)` | Wersja robocza nie znajduje się na tym węźle: węzeł został ponownie uruchomiony od momentu jego utworzenia lub polecenie dotarło do innego węzła. |
| `EphemeralAggregateUnknown(id)` | Ten węzeł nigdy go nie utworzył, upuścił go, gdy był bezczynny, ani nie zmaterializował. |

Zapytania kończą się niepowodzeniem w ten sam sposób: `GetAggregate` odpowiada `Failure(EphemeralAggregateLostException)` lub
`Błąd(NoEventsForAggregateException)`.

> **Ostrzeżenie:** w klastrze ([rozdział 17](17-cluster.md)), projekt znajduje się na węźle, którego efemeryczny
> autobus to zrobił. Wyślij każde polecenie do tego węzła, na przykład utrzymując sesję użytkownika
> jeden węzeł. Węzeł, który uruchamia się ponownie, traci swoje wersje robocze; poinformuj o tym użytkowników lub zmaterializuj się wcześniej.

> **Pod maską:** każde uruchomienie efemerycznej magistrali losuje losowy token, a jego identyfikatory są
> `-(token × 2^24 + n)`. Zatem identyfikator sprzed restartu lub z innego węzła jest uznawany za utracony,
> dwa węzły prawie nigdy nie dzielą identyfikatorów (raz na 2^29 par startów), a każdy identyfikator pozostaje liczbą
> Interfejs użytkownika JavaScript może pomieścić (poniżej 2^53). Zobacz [ARCHITECTURE.md, sekcja 4.10](../ARCHITECTURE.md).

## Czego projekty nie robią

- Nigdy nie są publikowane, więc projekcje i sagi nic o nich nie wiedzą, dopóki nie zostaną opublikowane.
  zmaterializował się.
- Nie są one ściskane, kopiowane ani przepisywane: najpierw należy je zmaterializować.
- Nie przetrwają restartu. Nic o nich nie ma w bazie danych.

## Czego się nauczyłeś

- `AggregateCommandBusActor.ephemeral(context, persistentBus, uidGenerator, mpjsons)` uruchamia Twój zwykły
  agregaty w pamięci jednego węzła, z negatywnymi identyfikatorami, nie zapisując i nie publikując niczego.
- Odpowiadają na polecenia dokładnie tak samo, jak agregaty przechowywane, z takimi samymi ograniczeniami rozmiaru i wersji.
- `Materialize(id, userId)` przechowuje szkic jako nowy agregat, z jednego zdarzenia squash jego stanu,
  i upuszcza projekt; jest on odrzucany bez zdarzenia squash lub podczas wykonywania poleceń.
- Nieużywane projekty są usuwane po upływie `idleTime`, utrzymywane jest maksymalnie `maxAggregates`, a utracone lub nieznane
  projekt jest odpowiedzią taką.

To koniec rozdziałów. Dodatki służą do wyszukiwania informacji:
[słownik](appendix-a-glossary.md),
[ściągawka z wiadomościami i odpowiedziami](appendix-b-cheat-sheet.md) i
[rozwiązywanie problemów](appendix-c-troubleshooting.md).