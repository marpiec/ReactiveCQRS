# Jak pisać „ReactiveCQRS, krok po kroku”

Jak pisać i aktualizować rozdziały tej książki. Książka to droga użytkownika do biblioteki: krok po
kroku uczy, jak z niej *korzystać*. To, jak biblioteka działa w środku, opisuje `../ARCHITECTURE.md`.

Ta książka jest tłumaczeniem angielskiej `../book/`, rozdział po rozdziale, pod tymi samymi nazwami
plików. Angielska wersja jest wersją źródłową: zmiana w niej trafia tutaj w tym samym zatwierdzeniu
albo w następnym.

## Głos

- **Przyjaźnie i lekko.** Pisz tak, jak cierpliwy kolega tłumaczy coś przy tablicy. Krótkie zdania,
  krótkie akapity, „ty” do czytelnika, „my”, gdy pracujecie razem.
- **Najpierw codzienne porównanie, potem żargon.** Wprowadź pomysł przez coś znajomego (pamiętnik,
  wyciąg z konta, paragon, kartka z zamówieniem w kuchni), potem podaj jego właściwą nazwę, a na końcu
  pokaż kod. Nigdy odwrotnie.
- **Odrobina humoru jest mile widziana, sarkazm nie.** Czytelnik nigdy nie może poczuć się powolny.
- **Jeden nowy pomysł naraz.** Jeśli sekcja potrzebuje dwóch nowych pomysłów, podziel ją.
- **Prosty język.** Lepiej „użyć” niż „wykorzystać potencjał”, „uruchomić” niż „zainstancjonować”.
- **Bez rodzaju gramatycznego czytelnika.** Unikaj form „zrobiłeś/zrobiłaś”: pisz w czasie teraźniejszym
  lub przyszłym („widzisz”, „dostaniesz”), bezosobowo („warto”, „trzeba”) albo w pierwszej osobie liczby
  mnogiej („zobaczmy”).

## Kształt rozdziału

1. Akapit otwierający: co będziesz umieć na końcu i po co.
2. Sekcje, z których każda uczy jednej rzeczy: porównanie, nazwa, kod, co się dzieje po uruchomieniu.
3. Numerowane kroki dla wszystkiego, co czytelnik robi po kolei (przygotowanie, połączenie, obsługa).
4. Ramki tam, gdzie pomagają (poniżej), nie na każdej stronie.
5. Na końcu lista **„Co już umiesz”** z trzema do siedmiu punktów i odnośnik do następnego rozdziału.

Rozdziały idą od prostych do zaawansowanych w całej książce i wewnątrz każdego rozdziału: najpierw
zwykły przypadek, potem przypadki brzegowe.

## Ramki

Dokładnie te cztery, jako cytaty z pogrubioną etykietą:

> **Wskazówka:** skrót albo dobry nawyk.

> **Pamiętaj:** coś, co warto mieć w głowie po zamknięciu książki.

> **Ostrzeżenie:** łatwy błąd o kosztownych skutkach.

> **Pod maską:** jak to działa w środku; zawsze można pominąć. Wskaż sekcję `../ARCHITECTURE.md` z pełną
> historią.

## Przykłady i kod

- Kod pochodzi z przykładów, które się kompilują i są testowane, tak jak w wersji angielskiej. Blok
  kodu jest **identyczny** z blokiem angielskiej wersji, znak w znak, razem ze znacznikiem
  `<!-- snippet: plik#region -->` przed nim i komentarzami w kodzie (po angielsku): `BookSnippetsSpec`
  sprawdza obie książki.
- Nie tłumacz niczego, co jest kodem: nazw klas, metod, komunikatów, kluczy konfiguracji, tabel, plików,
  poleceń powłoki ani linii logów, także w tekście (w `takich fragmentach`).
- **Każda klasa, metoda, komunikat i klucz konfiguracji, które wymieniasz, muszą istnieć w kodzie.**
- Pokazuj, co czytelnik dostaje w odpowiedzi („szyna odpowiada `SuccessResponse(cartId, AggregateVersion(2))`”).

## Zasady dokładności

- Opisuj tylko to, co zostało zbudowane.
- Liczby (czasy, rozmiary, przepustowość) pochodzą z `../perf/` albo `../SCALING.md` i mówią, czy były
  zmierzone (gdzie, kiedy), czy oszacowane.
- Jednostki: kilogramy, kilometry, litry, °C, megabajty dla rozmiarów. Daty: DD/MM/RRRR. W tekście
  przecinek dziesiętny („2,5 sekundy”), w kodzie kropka.
- Żadnych danych prawdziwych osób ani klientów: zmyślone nazwy („Zakupy”, „Alicja”); w kodzie te z
  wersji angielskiej.

## Tłumaczenie

- Tłumacz sens, nie słowa: polskie zdanie ma brzmieć naturalnie. Zachowaj każdą informację,
  ostrzeżenie i liczbę oryginału, nic nie dodawaj.
- Cudzysłowy „…”, myślnik zamiast średnika tam, gdzie po polsku brzmi lepiej.
- Nagłówki tłumacz. Odnośnik do nagłówka w tym samym pliku ma kotwicę, jaką GitHub robi z polskiego
  nagłówka: małe litery, polskie znaki zostają, interpunkcja znika (poza `-` i `_`), spacje na `-`
  („## Co już umiesz” → `#co-już-umiesz`).
- Odnośniki do innych plików książki mają te same nazwy plików co w wersji angielskiej. Odnośniki poza
  książkę (`../ARCHITECTURE.md`, `../KNOWN-ISSUES.md`) prowadzą do dokumentów angielskich.
- Diagramy Mermaid: tłumacz tylko etykiety, nie zmieniając identyfikatorów węzłów, strzałek ani składni;
  nazwy z kodu zostają.

## Słownictwo

Te wybory obowiązują w całej książce.

| Angielski | Polski |
|---|---|
| aggregate, aggregate root, aggregate type | agregat, korzeń agregatu, typ agregatu |
| aggregate context | kontekst agregatu |
| aggregate owner (`AggregateOwner`) | właściciel agregatu |
| event, first event, event class | zdarzenie, pierwsze zdarzenie, klasa zdarzenia |
| event handler, command handler | handler zdarzenia, handler polecenia |
| command | polecenie |
| command bus, event bus | szyna poleceń, szyna zdarzeń |
| event sourcing, CQRS | event sourcing, CQRS (bez tłumaczenia) |
| state, version, expected version | stan, wersja, oczekiwana wersja |
| projection, read model | projekcja, model odczytu |
| listener | listener |
| subscriber, subscription | subskrybent, subskrypcja |
| delivery, acknowledge (ack) | dostarczenie, potwierdzić (potwierdzenie) |
| publish, re-publish | opublikować, opublikować ponownie |
| outbox, outbox poller | outbox, poller outboxu |
| snapshot | migawka |
| squash, squash event | squash (zwinięcie historii), zdarzenie squasha |
| archive | archiwum |
| undo, duplicate | cofnięcie, duplikat (kopia) |
| history rewrite, erasure | przepisanie historii, wymazanie danych |
| saga, step, revert | saga, krok, wycofanie |
| organization, space | organizacja, przestrzeń (space) |
| node, cluster, shard, sharding | węzeł, klaster, shard, sharding |
| singleton, proxy | singleton, proxy |
| split brain resolver | split brain resolver |
| rolling upgrade | aktualizacja krocząca |
| idempotency key | klucz idempotencji |
| managed projection, build, rebuild | projekcja zarządzana, budowa, przebudowa |
| supervisor | nadzorca |
| ephemeral aggregate, materialize | agregat efemeryczny, zmaterializować |
| passivation | pasywacja |
| flow control, window | kontrola przepływu, okno |
| host (the application using the library) | aplikacja (host) |
| test kit, spec | test kit, spec |
| case class, pattern matching | case class, dopasowanie wzorców |
| event store, document store | magazyn zdarzeń, magazyn dokumentów |
| the projection's notebook (`SubscriptionsState`) | notatnik projekcji |
| saga order, internal order | zlecenie (sagi), zlecenie wewnętrzne |
| saga runner, squash scheduler | runner (sagi), scheduler squasha |
| targeted rebuild, retire (a projection) | przebudowa celowana, wycofać z użytku |
| purge (an organization), clear (documents) | usunięcie organizacji, czyszczenie |
| message, mailbox | komunikat, skrzynka pocztowa |
| commit, roll back | zatwierdzić, wycofać (transakcję) |
| blocking dispatcher, handler dispatcher | dispatcher blokujący, dispatcher handlerów |
| uid generator | generator identyfikatorów |
| restore, replay | odtworzenie (odtwarzać), stosować zdarzenia po kolei |
| failure (recorded), failed subscription | awaria, nieudana subskrypcja |
| poll (of the outbox poller), outbox writer | przegląd (przeglądać outbox), writer outboxu |

Imiona i nazwy w tekście: Alice to Alicja, Bob zostaje Bobem, Carol to Karolina, koszyk „Groceries” to
„Zakupy”; w kodzie i w tym, co program wypisuje, zostają angielskie.

Tytuły rozdziałów, takie same w spisie treści i w nagłówkach:

1. CQRS i event sourcing bez bólu głowy
2. Przygotowanie warsztatu
3. Twój pierwszy agregat: koszyk zakupów
4. Łączymy wszystko i wysyłamy pierwsze polecenie
5. Polecenia mają różne smaki
6. Zdarzenia: jedyne źródło prawdy
7. Spojrzenie w przeszłość
8. Projekcje: widoki, o które można pytać
9. Kiedy w projekcjach coś pójdzie nie tak
10. Przebudowa projekcji bez przestojów
11. Aktualizacje na żywo z projekcjami subskrybowalnymi
12. Sagi: jak sprawić, by agregaty współpracowały
13. Szybkie agregaty dzięki migawkom
14. Squash: zwijanie starej historii
15. Przepisywanie historii, ostrożnie
16. Organizacje (przestrzenie)
17. Na wielu węzłach: praca w klastrze
18. Na produkcji
19. Testowanie domeny
20. Agregaty efemeryczne: szkice, które żyją w pamięci

Dodatki: A. Słownik pojęć; B. Ściągawka: komunikaty i odpowiedzi; C. Rozwiązywanie problemów;
D. Architektura w obrazkach; E. Konfiguracja.

## Utrzymywanie aktualności

Każda zmiana tego, co użytkownik pisze lub uruchamia, aktualizuje rozdział angielski w tym samym
zatwierdzeniu, a jego tłumaczenie tutaj: nowa opcja, nowy komunikat, zmieniona wartość domyślna, nowy
krok aplikacji. Bloki kodu wypełnia ze źródeł ten sam skrypt w obu książkach.
