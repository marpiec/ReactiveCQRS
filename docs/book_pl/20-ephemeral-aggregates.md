# Rozdział 20. Agregaty efemeryczne: szkice, które żyją w pamięci

Nie wszystko zasługuje na miejsce w pamiętniku. Klient zapełnia koszyk i odchodzi; menedżer bawi się
szkicem budżetu i zamyka kartę. Przechowywanie każdego z tych kliknięć na zawsze, w dodatku tak, żeby
projekcje i sagi słyszały o każdym z nich, to sporo atramentu jak na brudnopis. Pod koniec tego
rozdziału będziesz umieć uruchamiać agregaty, które żyją tylko w pamięci jednej maszyny, zachowywać
te, które okażą się ważne, a resztę puszczać w niepamięć.

---

## Brudnopis

**Agregat efemeryczny** to zwykły agregat (twój kontekst, twoje polecenia, twoje zdarzenia, twoje
reguły), który żyje w pamięci tylko jednego węzła. Nic nie jest zapisywane w PostgreSQL, nic nie jest
publikowane do projekcji ani sag, a gdy węzeł się zatrzymuje, agregat znika. Pomyśl o szkicu ołówkiem
w brudnopisie: możesz go zmieniać, ile tylko chcesz, a gdy jest dobry, przepisujesz go na czysto do
pamiętnika. Takie przepisanie to **materializacja**.

## Krok 1: efemeryczna szyna poleceń

Agregaty efemeryczne mają własną szynę poleceń, obok zwykłej szyny tego samego typu. E8, przykład z
tego rozdziału, przechowuje szkice list zakupów z przykładu E5 z rozdziału 13:

<!-- snippet: e8/DraftsSetup.scala#buses -->
```scala
val lists = system.actorOf(
  AggregateCommandBusActor(new ShoppingListContext, uidGenerator, eventStore, responses, eventBus), "lists")
val drafts = system.actorOf(
  AggregateCommandBusActor.ephemeral(new ShoppingListContext, persistentBus = lists, uidGenerator, mpjsons), "draftLists")
```

Przyjmuje ten sam kontekst, trwałą szynę tego typu (to tam trafiają zmaterializowane listy),
generator identyfikatorów (dla identyfikatorów poleceń) i twój `MPJsons` (do sprawdzania rozmiarów
zdarzeń, o czym niżej). Nie ma parametru magazynu zdarzeń, stanu odpowiedzi ani szyny zdarzeń:
wszystko to trzyma w pamięci, na własny użytek.

## Krok 2: używaj jej jak każdej innej szyny

Wysyłaj do niej dokładnie te same polecenia:

<!-- snippet: e8/DraftsSetup.scala#draft -->
```scala
val draft = send(buses.drafts, CreateList(user, "Maybe this weekend")).asInstanceOf[SuccessResponse] // a negative id
send(buses.drafts, WriteItem(user, draft.aggregateId, draft.aggregateVersion, "tent"))
send(buses.drafts, WriteItem(user, draft.aggregateId, AggregateVersion(2), "stove"))
```

Zwróć uwagę na **ujemny identyfikator** w `SuccessResponse` szkicu: po nim ty i twój interfejs
użytkownika odróżnicie szkic od zapisanej listy. Wszystko inne działa tak, jak już znasz: twoje
handlery poleceń decydują, twoje handlery zdarzeń zmieniają stan, nieaktualne `expectedVersion`
dostaje `AggregateConcurrentModificationError`, `GetAggregate` odpowiada stanem, a nawet
`GetEventsForAggregate` pokazuje zdarzenia szkicu.

> **Pamiętaj:** szyna efemeryczna sprawdza to, co sprawdziłby PostgreSQL: JSON zdarzenia nie może być
> dłuższy niż limit dla jego klasy, a agregat nie może mieć więcej zdarzeń, niż pozwala limit wersji
> ([rozdział 6](06-events.md)). Szkic, który działa w pamięci, zawsze da się więc zapisać.

## Krok 3: zachowaj go przez Materialize

Gdy lista okaże się warta zachowania:

<!-- snippet: e8/DraftsSetup.scala#keep -->
```scala
val kept = Await.result((buses.drafts ? Materialize(draft.aggregateId, user)).mapTo[MaterializeResponse], timeout.duration)
```

Odpowiedzią jest `Materialized(draftId, persistentId)`: ujemny identyfikator szkicu i identyfikator
nowej listy. Biblioteka bierze bieżący stan szkicu, zamienia go na **zdarzenie squasha** tego typu
([rozdział 14](14-squash.md)), sprawdza, że to zdarzenie odtwarza dokładnie ten stan, i zleca trwałej
szynie zapisanie go jako pierwszego zdarzenia zupełnie nowej listy. Potem szkic jest usuwany:
polecenia dla jego ujemnego identyfikatora dostają `EphemeralAggregateUnknown`. Od teraz pracujesz z
nowym identyfikatorem na trwałej szynie, a projekcje i sagi dowiadują się o nim jak o każdej nowej
liście. `DraftsSpec` sprawdza to wszystko: w `events` nie ma nic ze szkicu, zapisana lista jest w
wersji 1 z obiema pozycjami, a szkicu już nie ma.

Kilka szczegółów, które dbają o bezpieczeństwo:

- Dopóki trwa materializacja, polecenia dla szkicu dostają odpowiedź `CommandRejected`, więc zapisane
  zostaje dokładnie to, co widział wywołujący. Wyślij je ponownie, już do nowej listy.
- Jeśli na szkicu wciąż wykonują się polecenia albo typ nie ma zdarzenia squasha, odpowiedzią jest
  `MaterializeRefused(id, reason)`, a szkic zostaje taki, jaki był.
- Materializacja jest idempotentna: jeśli odpowiedź się spóźni i prośba pójdzie ponownie, biblioteka
  ją rozpozna i odpowie tym samym nowym identyfikatorem, zamiast zapisywać drugą listę.

> **Wskazówka:** zapisana lista zaczyna od wersji 1, z jednym zdarzeniem podsumowującym: kliknięcia ze
> szkicu nie są zachowywane. Na tym właśnie polega brudnopis. Jeśli kliknięcia są ci potrzebne, ich
> miejsce jest w pamiętniku od samego początku.

## Krok 4: pozwól mu odejść

Szkice, których nikt nie dotyka, nie gromadzą się w nieskończoność:

- **Czas bezczynności**: szkic nieużywany przez `idleTime` (domyślnie 12 godzin) zostaje usunięty.
  Szyna nigdy nie usuwa szkicu wcześniej, niż wynosi limit czasu poleceń w kontekście, więc
  polecenie, które wciąż się wykonuje, nie może pisać do szkicu, którego już nie ma.
- **Górny limit**: szyna trzyma najwyżej `maxAggregates` szkiców (domyślnie 100 000); nowy szkic
  ponad tę liczbę dostaje odpowiedź `CommandRejected`.

<!-- snippet: e8/DraftsSpec.scala#limits -->
```scala
AggregateCommandBusActor.ephemeral(new ShoppingListContext, persistentBus = lists, uidGenerator, mpjsons,
  maxAggregates = 10000, idleTime = 2.hours)
```

## Gdy szkic zaginie

Pamięć jest ulotna, a biblioteka mówi o tym wprost:

| Odpowiedź | Znaczenie |
|---|---|
| `EphemeralAggregateLost(id)` | Szkicu nie ma na tym węźle: węzeł zrestartował się, odkąd szkic powstał, albo polecenie trafiło do innego węzła. |
| `EphemeralAggregateUnknown(id)` | Ten węzeł nigdy go nie utworzył, usunął go po okresie bezczynności albo go zmaterializował. |

Zapytania zawodzą w ten sam sposób: `GetAggregate` odpowiada
`Failure(EphemeralAggregateLostException)` albo `Failure(NoEventsForAggregateException)`.

> **Ostrzeżenie:** w klastrze ([rozdział 17](17-cluster.md)) szkic żyje na tym węźle, którego szyna
> efemeryczna go utworzyła. Każde polecenie dla niego wysyłaj do tego węzła, na przykład trzymając
> sesję użytkownika na jednym węźle. Węzeł, który się restartuje, traci swoje szkice; uprzedź o tym
> użytkowników albo materializuj wcześnie.

> **Pod maską:** przy każdym starcie szyna efemeryczna losuje token, a jej identyfikatory to
> `-(token × 2^24 + n)`. Dzięki temu identyfikator sprzed restartu albo z innego węzła zostaje
> rozpoznany jako utracony, dwa węzły prawie nigdy nie mają wspólnych identyfikatorów (raz na 2^29 par
> startów), a każdy identyfikator pozostaje liczbą, którą interfejs użytkownika w JavaScripcie potrafi
> przechować (poniżej 2^53). Zobacz [ARCHITECTURE.md, sekcja 4.10](../ARCHITECTURE.md).

## Czego szkice nie robią

- Nigdy nie są publikowane, więc projekcje i sagi nic o nich nie wiedzą, dopóki nie zostaną
  zmaterializowane.
- Nie przechodzą squasha, kopiowania ani przepisywania historii: najpierw je zmaterializuj.
- Nie przetrwają restartu. W bazie danych nie ma o nich nic.

## Co już umiesz

- `AggregateCommandBusActor.ephemeral(context, persistentBus, uidGenerator, mpjsons)` uruchamia twoje
  zwykłe agregaty w pamięci jednego węzła, z ujemnymi identyfikatorami, niczego nie zapisując ani nie
  publikując.
- Odpowiadają na polecenia dokładnie tak jak zapisane agregaty, z tymi samymi limitami rozmiaru i
  wersji.
- `Materialize(id, userId)` zapisuje szkic jako nowy agregat, z jednego zdarzenia squasha jego stanu,
  i usuwa szkic; jest odrzucany, gdy typ nie ma zdarzenia squasha albo gdy wykonują się polecenia.
- Nieużywane szkice znikają po `idleTime`, szyna trzyma ich najwyżej `maxAggregates`, a o utraconym
  lub nieznanym szkicu odpowiedź mówi wprost.

To już koniec rozdziałów. Dodatki służą do szybkiego wyszukiwania informacji:
[słownik pojęć](appendix-a-glossary.md),
[ściągawka: komunikaty i odpowiedzi](appendix-b-cheat-sheet.md) i
[rozwiązywanie problemów](appendix-c-troubleshooting.md).
