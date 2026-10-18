# ReactiveCQRS, krok po kroku

*Przyjazny przewodnik: jak małymi krokami budować w Scali aplikacje oparte na event sourcingu.*

*In English: [ReactiveCQRS, Step by Step](../book/README.md).*

---

## Dla kogo jest ta książka

Piszesz w Scali. Słowa takie jak *CQRS*, *event sourcing* i *projekcje* znasz ze słyszenia, może nawet
zdarzyło ci się grzecznie przytaknąć, gdy padły na spotkaniu, a teraz masz coś zbudować
z ReactiveCQRS. Dobra wiadomość: w żadnej z tych dziedzin nie musisz być ekspertem. Ta książka zaczyna
od zera i dokłada jeden pomysł naraz.

Przyda ci się:

- trochę Scali (case class, dopasowanie wzorców, `Future`),
- odrobina SQL-a (głównie popatrzysz, jak pisze go biblioteka),
- mgliste wspomnienie tego, czym jest aktor. Jeśli i tego brak, nic nie szkodzi: rozdział 1 wyjaśnia
  to, czego potrzebujesz.

## Jak ułożona jest ta książka

Rozdziały prowadzą od prostych do zaawansowanych. Części I i II przeczytaj po kolei, a dalej skacz do
tego, czego twój projekt potrzebuje dzisiaj.

- **Część I, Pierwsze kroki**: czym jest biblioteka, jak ją zainstalować, twój pierwszy agregat i twoje
  pierwsze polecenie.
- **Część II, Polecenia i zdarzenia od podszewki**: każdy rodzaj polecenia, wszystko, co potrafią
  zdarzenia, i jak odczytać przeszłość.
- **Część III, Modele odczytu**: projekcje, które zamieniają zdarzenia w dane, o które można pytać, jak
  podnoszą się po awariach, jak się je przebudowuje i jak wysyłają aktualizacje na żywo.
- **Część IV, Procesy**: sagi – do zadań, które obejmują kilka agregatów.
- **Część V, Życie z długą historią**: migawki, squash, archiwum audytowe i przepisywanie historii.
- **Część VI, Organizacje, klastry i produkcja**: organizacje (przestrzenie), praca na kilku węzłach,
  utrzymanie tego wszystkiego w ruchu i testowanie.
- **Część VII, Brudnopis**: agregaty efemeryczne, które żyją w pamięci, dopóki ich nie zachowasz.
- **Dodatki**: słownik pojęć, ściągawka z komunikatów, poradnik rozwiązywania problemów, architektura
  w obrazkach i wszystkie ustawienia w jednym miejscu.

## Ramki w tej książce

> **Wskazówka:** skrót albo dobry nawyk, który oszczędza ci czas.

> **Pamiętaj:** coś na tyle ważnego, że warto mieć to w głowie jeszcze po zamknięciu książki.

> **Ostrzeżenie:** błąd, który łatwo popełnić, a drogo naprawić. Takie ramki czytaj dwa razy.

> **Pod maską:** jak to działa w środku. Tę ramkę można bez obaw pominąć, ale przydaje się, gdy coś cię
> zaskoczy.

## Spis treści

**Część I: Pierwsze kroki**

1. [CQRS i event sourcing bez bólu głowy](01-cqrs-without-the-headache.md)
2. [Przygotowanie warsztatu](02-setting-up.md)
3. [Twój pierwszy agregat: koszyk zakupów](03-first-aggregate.md)
4. [Łączymy wszystko i wysyłamy pierwsze polecenie](04-first-command.md)

**Część II: Polecenia i zdarzenia od podszewki**

5. [Polecenia mają różne smaki](05-command-flavors.md)
6. [Zdarzenia: jedyne źródło prawdy](06-events.md)
7. [Spojrzenie w przeszłość](07-reading-the-past.md)

**Część III: Modele odczytu**

8. [Projekcje: widoki, o które można pytać](08-projections.md)
9. [Kiedy w projekcjach coś pójdzie nie tak](09-projection-failures.md)
10. [Przebudowa projekcji bez przestojów](10-rebuilding-projections.md)
11. [Aktualizacje na żywo z projekcjami subskrybowalnymi](11-subscribable-projections.md)

**Część IV: Procesy**

12. [Sagi: jak sprawić, by agregaty współpracowały](12-sagas.md)

**Część V: Życie z długą historią**

13. [Szybkie agregaty dzięki migawkom](13-snapshots.md)
14. [Squash: zwijanie starej historii](14-squash.md)
15. [Przepisywanie historii, ostrożnie](15-rewriting-history.md)

**Część VI: Organizacje, klastry i produkcja**

16. [Organizacje (przestrzenie)](16-organizations.md)
17. [Na wielu węzłach: praca w klastrze](17-cluster.md)
18. [Na produkcji](18-production.md)
19. [Testowanie domeny](19-testing.md)

**Część VII: Brudnopis**

20. [Agregaty efemeryczne: szkice, które żyją w pamięci](20-ephemeral-aggregates.md)

**Dodatki**

- A. [Słownik pojęć](appendix-a-glossary.md)
- B. [Ściągawka: komunikaty i odpowiedzi](appendix-b-cheat-sheet.md)
- C. [Rozwiązywanie problemów](appendix-c-troubleshooting.md)
- D. [Architektura w obrazkach](appendix-d-architecture.md)
- E. [Konfiguracja](appendix-e-configuration.md)

## Skąd pochodzą przykłady

Kod w tej książce jest kompilowany i uruchamiany. Pochodzi z trzech miejsc w tym repozytorium:

- **Przykłady książki, od E1 do E11**, w `examples/src/main/scala/io/reactivecqrs/examples/`: małe
  programy; każdy ma `main`, który możesz uruchomić, i spec, który uruchamia ten sam kod.
- **Koszyk zakupów z domeny testowej**, w `testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`,
  z którego korzystają rozdziały od 5 do 18. To ten sam koszyk, na którym działają testy samej biblioteki.
- **Testy tych rozdziałów**, w `examples/src/test/scala/io/reactivecqrs/examples/cart/`: każdy blok,
  który coś wysyła, jest regionem jednego z nich i stoi tuż przed liniami, które sprawdzają odpowiedź
  opisaną w rozdziale.

Blok wzięty z któregoś z tych miejsc ma w źródle rozdziału komentarz z nazwą pliku i regionu,
a `BookSnippetsSpec` sprawdza, że każdy taki blok jest dokładnie tym regionem, słowo w słowo. Nieliczne
bloki bez takiego znacznika pokazują jedno wywołanie albo linijkę konfiguracji, albo kod, który rozdział
przedstawia jako hipotetyczny lub celowo błędny.

## Wersja, którą opisuje ta książka

ReactiveCQRS **1.0** (wydanie `1.0.0` z 18/10/2026), na Scali 2.13, Apache Pekko 1.7, Javie 21
i PostgreSQL. Każda funkcja, którą pokazuje książka, jest zbudowana i przetestowana.

Dla opiekunów biblioteki: jak działa ona w środku, opisuje [`../ARCHITECTURE.md`](../ARCHITECTURE.md);
znane problemy i podjęte decyzje są w [`../KNOWN-ISSUES.md`](../KNOWN-ISSUES.md); zachowanie pod dużym
obciążeniem opisuje [`../SCALING.md`](../SCALING.md). Jak pisać do tej książki, mówi
[`STYLE.md`](STYLE.md).
