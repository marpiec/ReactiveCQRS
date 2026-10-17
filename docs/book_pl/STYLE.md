# Pisanie dla „ReactiveCQRS, krok po kroku”

Jak pisać i aktualizować rozdziały tej książki. Książka to droga użytkownika: uczy, krok po kroku
Krok po kroku, jak *korzystać* z biblioteki. Sposób działania biblioteki w środku znajduje się w pliku `../ARCHITECTURE.md`.

## Głos

- **Przyjazny i lekki.** Napisz tak, jak cierpliwy kolega wyjaśnia coś na tablicy. Krótki
  zdania, krótkie akapity, „ty” dla czytelnika, „my” dla was dwojga pracujących razem.
- **Najpierw codzienne zdjęcia, potem żargon.** Wprowadź pomysł za pomocą czegoś znajomego (pamiętnika,
  wyciąg bankowy, paragon ze sklepu, paragon z kuchni), a następnie podaj jego właściwą nazwę i pokaż
  kod. Nigdy odwrotnie.
- **Odrobina humoru jest mile widziana, sarkazm nie.** Nigdy nie sprawiaj, by czytelnik czuł się spowolniony.
- **Jeden nowy pomysł na raz.** Jeśli sekcja potrzebuje dwóch nowych pomysłów, podziel ją.
- **Prostym językiem.** Lepiej używać słów „use” zamiast „leverage”, „start” zamiast „instantiate”, gdzie znaczenie jest takie samo.

## Kształt rozdziału

1. Akapit wstępny: co będziesz w stanie zrobić do końca i dlaczego chciałbyś to zrobić.
2. Sekcje, z których każda uczy jednej rzeczy: obrazu, nazwy, kodu i tego, co się dzieje po jego uruchomieniu.
3. Ponumerowane kroki każdej czynności wykonywanej przez czytelnika (konfiguracja, podłączanie przewodów, obsługa).
4. Odnośniki tam, gdzie są pomocne (patrz poniżej), a nie na każdej stronie.
5. Końcowa lista **Czego się nauczyłeś/nauczyłaś** zawierająca od trzech do siedmiu punktów i odnośnik do następnego rozdziału.

Rozdziały są podzielone od prostych do zaawansowanych w całej książce, a także w każdym rozdziale: typowe przypadki
najpierw skrajne przypadki.

## Objaśnienia

Użyj dokładnie tych czterech, jako cytatów blokowych z pogrubioną etykietą:

> **Wskazówka:** skrót lub dobry nawyk.

> **Pamiętaj:** o czym należy pamiętać po zamknięciu książki.

> **Ostrzeżenie:** łatwy błąd z kosztownymi konsekwencjami.

> **Pod maską:** jak to działa w środku; zawsze bezpiecznie pominąć. Wskaż odpowiednią sekcję
> `../ARCHITECTURE.md`, aby przeczytać pełną historię.

## Przykłady

- Przykłady bazowe na domenie koszyka zakupowego w `testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`
  i specyfikacje w `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/`. Kiedy rozdział potrzebuje
  czegoś, czego nie ma w koszyku, napisz mały nowy przykład w tym samym duchu (lista rzeczy do zrobienia,
  wypożyczenie książki z biblioteki) i zachować spójność w ramach rozdziału.
- **Każda klasa, metoda, wiadomość i klucz konfiguracji, które wymienisz, muszą znajdować się w kodzie.** Sprawdź w
  Przeszukaj, zanim to zapiszesz. Jeśli upraszczasz przykład, musi on nadal być zgodny z duchem:
  właściwe typy, właściwe nazwy wiadomości, właściwe odpowiedzi.
- Pokaż importy, gdy nie są oczywiste (`io.reactivecqrs.api._`, `org.apache.pekko.pattern.ask`).
- Zadbaj o to, aby bloki kodu były krótkie: pokaż część, która jest najważniejsza i powiedz, co pominąłeś.
- Pokaż, co otrzymuje czytelnik (autobus odpowiada `SuccessResponse(cartId, AggregateVersion(2))`").

## Zasady dokładności

- Opisz tylko to, co zostało zbudowane. Funkcja, która jest zaplanowana, ale jeszcze nie została zbudowana, jest określana jako
  „w wersji 1.0” (lub nowszej), bez żadnych obietnic odnośnie interfejsu API.
- Liczby (czasy, rozmiary, przepustowość) pochodzą z pliku `../perf/` lub `../SCALING.md` i określają, czy
  zostały zmierzone (gdzie, kiedy) lub oszacowane.
- Jednostki: kilogramy, kilometry, litry, °C i megabajty dla rozmiarów. Daty: DD/MM/RRRR.
- W przykładach nie podawaj danych prawdziwych osób ani klientów: użyj wymyślonych nazw („Sklep spożywczy”, „Alicja”).

## Utrzymywanie aktualności

Każda zmiana, która zmienia to, co użytkownik pisze lub uruchamia, aktualizuje rozdział w tym samym zatwierdzeniu: nowym
opcja, nowa wiadomość, zmieniona wartość domyślna, nowy krok hosta. Funkcja, która ląduje, zastępuje swoją
notatka „nadchodząca” z własną sekcją.