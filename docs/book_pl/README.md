# ReactiveCQRS, krok po kroku

*Przyjazny przewodnik po tworzeniu aplikacji Scala opartych na zdarzeniach, krok po kroku.*

---

## Dla kogo jest ta książka

Piszesz w Scali. Słyszałeś takie słowa jak *CQRS*, *event sourcing* i *projekcje*, może skinąłeś głową.
grzecznie na spotkaniu, a teraz musisz coś zbudować z ReactiveCQRS. Dobra wiadomość: nie musisz
Nie musisz być ekspertem w żadnej z tych dziedzin. Ta książka zaczyna od zera i stopniowo dodaje jeden pomysł na raz.

Będziesz potrzebować:

- trochę Scali (klasy przypadków, dopasowywanie wzorców, `Future`),
- trochę SQL (głównie będziesz obserwował, jak biblioteka to pisze),
- mgliste wspomnienie o tym, kim jest aktor. Jeśli nie masz, to też dobrze: rozdział 1 wyjaśnia
  potrzebne Ci części.

## Jak zorganizowana jest ta książka

Rozdziały są podzielone od prostych do zaawansowanych. Przeczytaj części I i II po kolei; następnie przejdź do kolejnych.
czego potrzebuje Twój projekt już dziś.

- **Część I, Pierwsze kroki**: czym jest biblioteka, jak ją zainstalować, Twój pierwszy agregat i Twoje
  pierwsze polecenie.
- **Część II, Polecenia i zdarzenia szczegółowo**: każdy rodzaj polecenia, wszystko, co mogą zrobić zdarzenia i
  jak czytać przeszłość.
- **Część III, Odczyt modeli**: projekcje, które zmieniają zdarzenia w dane, które ludzie mogą kwerendować, w jaki sposób
  jak odzyskać dane po awariach, jak je odbudować i jak publikować aktualizacje na żywo.
- **Część IV, Procesy**: sagi dotyczące pracy obejmującej kilka zbiorów.
- **Część V, Życie z długą historią**: migawki, squash, archiwum audytów i przepisywanie historii.
- **Część VI, Organizacje, klastry i produkcja**: organizacje (przestrzenie) działające na kilku
  węzły, obsługa wszystkiego i testowanie.
- **Część VII, Brudne zapiski**: ulotne zbiory, które żyją w pamięci do momentu, aż je zachowasz.
- **Dodatki**: słownik, ściągawka z komunikatami i przewodnik rozwiązywania problemów.

## Ikony używane w tej książce

> **Wskazówka:** skrót lub dobry nawyk, który pozwoli Ci zaoszczędzić czas.

> **Zapamiętaj**: coś na tyle ważnego, że warto o tym pamiętać po zamknięciu książki.

> **Ostrzeżenie:** błąd, który łatwo popełnić i który jest kosztowny w naprawie. Przeczytaj to dwa razy.

> **Pod maską:** jak to działa w środku. Można to pominąć; przydatne, gdy coś Cię zaskoczy.

## Spis treści

**Część I: Pierwsze kroki**

1. [CQRS i pozyskiwanie zdarzeń bezproblemowe](01-cqrs-without-the-headache.md)
2. [Konfigurowanie warsztatu](02-setting-up.md)
3. [Twój pierwszy agregat: koszyk zakupowy](03-first-aggregate.md)
4. [Podłączanie i wysyłanie pierwszego polecenia](04-first-command.md)

**Część II: Polecenia i zdarzenia szczegółowo**

5. [Polecenia mają swoje smaki](05-command-flavors.md)
6. [Wydarzenia: Twoje jedyne źródło prawdy](06-events.md)
7. [Patrząc wstecz w czasie](07-reading-the-past.md)

**Część III: Odczyt modeli**

8. [Projekcje: widoki, które ludzie mogą kwerendować](08-projections.md)
9. [Kiedy prognozy idą źle](09-projection-failures.md)
10. [Odbudowa projekcji bez przestoju](10-rebuilding-projections.md)
11. [Aktualizacje na żywo z projekcjami do subskrypcji](11-subscribable-projections.md)

**Część IV: Procesy**

12. [Sagas: Jak sprawić, by agregaty współpracowały](12-sagas.md)

**Część V: Życie z długą historią**

13. [Utrzymywanie szybkości agregatów za pomocą migawek](13-snapshots.md)
14. [Squash: Odrzucanie starej historii](14-squash.md)
15. [Przepisywanie historii z rozwagą](15-rewriting-history.md)

**Część VI: Organizacje, klastry i produkcja**

16. [Organizacje (przestrzenie)](16-organizations.md)
17. [Przejście na tryb rozproszony: uruchamianie w klastrze](17-cluster.md)
18. [Uruchomienie w środowisku produkcyjnym](18-production.md)
19. [Testowanie domeny](19-testing.md)

**Część VII: Brudnopis**

20. [Ephemeral Aggregates: Drafts That Live in Memory](20-ephemeral-aggregates.md)

**Załączniki**

- A. [Słownik](appendix-a-glossary.md)
- B. [Ściągawka: Wiadomości i odpowiedzi](appendix-b-cheat-sheet.md)
- C. [Rozwiązywanie problemów](appendix-c-troubleshooting.md)

## Skąd pochodzą przykłady

Prawie każdy przykład pochodzi z domeny koszyka zakupowego w domenie „testdomain” lub jest na niej wzorowany
moduł tego repozytorium (`testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`).
To prawdziwy kod uruchamiany przez zestaw testów, więc możesz go otworzyć, uruchomić i zmodyfikować.

## Wersja opisana w tej książce

ReactiveCQRS **1.0** (obecnie `1.0.0-SNAPSHOT`) na Scala 2.13, Apache Pekko 1.7 i PostgreSQL.
Niektóre funkcje wersji 1.0 są nadal opracowywane; rozdziały, które o nich wspominają, mówią o tym wyraźnie i nigdy nie
udawać, że już tam są.

Dla opiekunów: opis działania biblioteki w środku można znaleźć w pliku [`../ARCHITECTURE.md`](../ARCHITECTURE.md);
znane problemy i decyzje znajdują się w [`../KNOWN-ISSUES.md`](../KNOWN-ISSUES.md); zachowanie w ramach
„Ciężkie obciążenie” znajduje się w [`../SCALING.md`](../SCALING.md). Jak pisać do tej książki, znajdziesz w
[`STYLE.md`](STYLE.md).