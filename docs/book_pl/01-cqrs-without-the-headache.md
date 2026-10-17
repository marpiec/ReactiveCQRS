# Rozdział 1. CQRS i pozyskiwanie zdarzeń bez bólu głowy

Do końca tego rozdziału będziesz wiedział, czym są *polecenia*, *wydarzenia*, *agregaty* i *projekcje*
dlaczego ktoś miałby tworzyć oprogramowanie w ten sposób i jaką część zadania wykonuje za Ciebie ReactiveCQRS.
Nie ma jeszcze kodu do wpisania. Napij się kawy.

---

## Tablica i pamiętnik

Wyobraź sobie listę zakupów na tablicy w swojej kuchni. Ktoś pisze „mleko”, ktoś inny dodaje
„Chleb”, ktoś wyciera „mleko”, bo kupił je w drodze do domu. W każdej chwili
Tablica pokazuje *aktualną* listę. Nie może jednak powiedzieć, w jaki sposób lista się tam znalazła: kto
Dodałeś chleb? Czy było na nim mleko dziś rano? Czy ktoś kiedyś napisał „czekolada” i cicho…
wytrzeć to?

Wyobraź sobie teraz, że zamiast tablicy rodzina prowadzi **dziennik**, w którym zapisuje listę:

1. Poniedziałek 08:10, Alicja: *dodała mleko*
2. Poniedziałek 12:30, Bob: *dodał chleb*
3. Poniedziałek 18:05, Alicja: *wyjęła mleko*

Dziennik nigdy nie zmienia tego, co zostało napisane; nowe linijki pojawiają się tylko na dole. I kiedykolwiek
chcesz aktualną listę, czytasz pamiętnik od góry i powtarzasz każdą linijkę: mleko, potem chleb, potem
Brak mleka. Wynik: chleb. Ta sama odpowiedź, co na tablicy, plus cała historia.

To jest **źródło zdarzeń** w jednym akapicie. Każdy wiersz dziennika to **wydarzenie**: fakt dotyczący
coś, co się wydarzyło, napisane w czasie przeszłym. Aktualna lista to **stan**, a ty otrzymujesz
poprzez odtwarzanie wydarzeń w kolejności. Dziennik jest źródłem prawdy; tablica jest tylko
wygodny obrazek, który możesz przerysować w każdej chwili.

> **Pamiętaj:** wydarzenia to fakty z przeszłości. Zdanie „dodano mleko” pozostaje prawdziwe na zawsze, nawet po
> ktoś wyjmuje mleko. Wyjęcie to *nowe* wydarzenie, a nie gumka.

## Polecenia: proś grzecznie

Nikt nie pisze bezpośrednio w pamiętniku. *Prosisz* o zmianę: „proszę dodać czekoladę do listy”.
To żądanie to **polecenie**. W przeciwieństwie do zdarzenia, polecenie można odrzucić: być może lista jest pełna,
Może czekolada jest zakazana do kolacji. Ktoś musi spojrzeć na aktualną listę i zastosować
zasady domowe i podejmij decyzję.

- Jeśli odpowiedź brzmi tak, decyzję zapisuje się jako jedno lub więcej zdarzeń ("dodano czekoladę").
- Jeśli odpowiedź brzmi „nie”, nic nie jest napisane i zostajesz poinformowany dlaczego.

Więc przepływ jest zawsze taki sam:

```
polecenie → decyzja (spójrz na stan bieżący, zastosuj zasady) → zdarzenia (lub odmowa)
```

W ReactiveCQRS polecenia są klasami przypadków Scala, takimi jak `AddItem(...)`, a decyzja jest funkcją
piszesz coś, co nazywa się **obsługą poleceń**, a zdarzenia są klasami przypadków, takimi jak `ItemAdded("milk")`.

## Agregaty: jeden dziennik na rzecz

Prawdziwa aplikacja nie ma jednej listy zakupów; ma tysiące koszyków zakupowych, po jednym na każdy
Klient. Każdy wózek ma swój własny dziennik. Rzecz, która jest właścicielem dziennika i zasady, które go strzegą,
nazywa się **agregatem**. Koszyk zakupowy jest agregatem, podobnie jak faktura, konto użytkownika czy
rada projektowa.

Każdy agregat ma:

- **id**, dzięki któremu można znaleźć jego dziennik (`AggregateId`, liczba),
- **wersja**: ile wydarzeń zawiera jej dziennik. Pomyśl o tym jak o numerze strony ostatniego wiersza.
  Nowy wózek zaczyna się od wersji 0, po pierwszym zdarzeniu jest w wersji 1 itd.
- **stan**: to, co otrzymujesz, odtwarzając zdarzenia (w przypadku koszyka: jego nazwa i przedmioty).

Dlaczego wyznaczać granicę wokół jednego wózka, a nie wokół „wszystkich wózków”? Ponieważ zasady dotyczące wózka obowiązują tylko
Potrzebuję tego jednego wózka. „Nie więcej niż pięć przedmiotów” można sprawdzić, patrząc na pojedynczy wózek.
małe agregaty oznaczają, że wiele z nich może zmieniać się w tym samym czasie, nie wchodząc sobie nawzajem w drogę
palce u stóp.

### Kiedy dwie osoby piszą jednocześnie

Alicja otwiera wózek na telefonie, Bob na laptopie. Oboje widzą wersję 3. Alicja dodaje mleko; wózek
Przechodzi do wersji 4. Bob, wciąż patrząc na wersję 3, dodaje chleb. Czy zmiana Boba powinna zostać wprowadzona?
Być może; być może zasady mówiłyby coś innego o wersji 4. ReactiveCQRS pozwala
wybierać:

- Zwykłym poleceniem mówisz „Zmieniam wersję 3”. Jeśli wózek w międzyczasie się przesunął,
  polecenie zostało odrzucone z odpowiedzią *współbieżna modyfikacja*, można więc sprawdzić jeszcze raz i spróbować jeszcze raz.
  To się nazywa **optymistyczne blokowanie**: nikt niczego nie blokuje z góry; kolizje są zauważane, gdy
  zdarzają się.
- W przypadku polecenia *współbieżnego* mówisz „jakakolwiek jest aktualna wersja”, a biblioteka próbuje ponownie
  na starciu.

Rozdział 5 pokazuje oba.

## CQRS: jeden model do pisania, inne do czytania

Odtwarzanie pamiętnika za każdym razem, gdy ktoś chce obejrzeć koszyk, działa, ale jest powolne, jeśli pytanie
„Pokaż mi każdy wózek, który zawiera czekoladę”. W przypadku takich pytań potrzebujesz tabeli, którą możesz…
wyszukiwanie, jeden wiersz na koszyk, na bieżąco aktualizowane.

Więc zachowujesz **dwie strony**:

- **strona pisemna** (polecenia, agregaty, zdarzenia), gdzie podejmowane są decyzje i ujawniana jest prawda
  przechowywane;
- **strona odczytu**, w której zdarzenia są przekształcane w kształt najłatwiejszy do zapytania: lista
  koszyki na klienta, liczba artykułów na produkt, indeks wyszukiwania.

Ten podział ma nazwę: **CQRS**, Command Query Responsibility Segregation. Wielkie słowa dla prostego
nawyk: *model, z którym piszesz, nie musi być modelem, z którym czytasz.*

Części strony odczytu nazywane są **projekcjami**. Projekcja nasłuchuje zdarzeń i aktualizuje
własne tabele. Wyobraź sobie bank: księga główna rejestruje każdą transakcję (wydarzenia), podczas gdy bank
Aplikacja pokazuje Ci przejrzyste podsumowanie Twoich kont (projekcję księgi głównej). Jeśli podsumowanie kiedykolwiek…
Jeśli dane zostaną uszkodzone, bank może je odtworzyć z rejestru. Dokładnie to samo można zrobić z projekcjami.
(rozdział 10).

> **Pamiętaj:** strona odczytu jest *ostatecznie* aktualna. Po pomyślnym wykonaniu polecenia jego zdarzenia docierają
> projekcje chwilę później, zazwyczaj w ciągu milisekund. Kiedy ekran musi wyświetlić Twój własny
> zmienić od razu, są do tego narzędzia (rozdziały 7 i 8).

## Co ReactiveCQRS dla Ciebie robi

Napisanie tego wszystkiego ręcznie wymaga sporo pracy: bezpiecznego przechowywania zdarzeń, obsługi dwóch pisarzy jednocześnie.
raz, dostarczając każde zdarzenie do każdej projekcji, nawet po awarii, odbudowując modele odczytu.
Biblioteka zajmuje się hydrauliką; Ty piszesz części dotyczące Twojej firmy.

**Piszesz:**

- stan agregatu (klasa przypadku),
- jego polecenia i zdarzenia (więcej klas przypadków),
- procedury obsługi poleceń (reguły) i procedury obsługi zdarzeń (sposób, w jaki każde zdarzenie zmienia stan),
- projekcje (jak zdarzenia stają się modelami odczytu),
- a gdy zadanie obejmuje kilka agregatów – sagi (rozdział 12).

**ReactiveCQRS dba o:**

- kierowanie każdego polecenia do właściwego agregatu i upewnianie się, że dwa polecenia nigdy nie mogą jednocześnie zapisać
  ta sama wersja,
- przechowywanie agregatów w pamięci, gdy są zajęte i uśpienie ich, gdy są bezczynne,
- przechowywanie zdarzeń w PostgreSQL w jednej transakcji, z blokadą optymistyczną,
- dostarczanie wydarzeń do projekcji i sag *przynajmniej raz, w kolejności zbiorczej*, przy czym każdy
  zdarzenie zastosowane *raz*, nawet w przypadku awarii i ponownych uruchomień,
- ponawianie prób, rejestrowanie i raportowanie błędów, dzięki czemu nic nie ginie bezpowrotnie,
- odbudowywanie projekcji, migawek dla długich historii, eliminowanie starej historii i uruchamianie
  klaster kilku maszyn.

Oto cała podróż jednego polecenia:

```
ty ──polecenie──▶ magistrala poleceń ──▶ właściciel agregatu ──wydarzenia──▶ PostgreSQL (wydarzenia + skrzynka nadawcza)
                                       │
                                       └──publikuj──▶ szyna zdarzeń ──┬──▶ projekcje ──▶ odczytaj modele
                                                                 └──▶ sagi
```

- **Magistrala poleceń** jest bramą wejściową dla jednego typu agregatu (jednego dla wózków, drugiego dla faktur).
- **Właściciel agregatu** dba o jeden agregat (jeden wózek): utrzymuje jego stan, zarządza nim
  program obsługi poleceń i przechowuje zdarzenia.
- **PostgreSQL** przechowuje zdarzenia, a także **skrzynkę nadawczą**: listę zadań do wykonania, którą niektórzy subskrybenci
  Nie zostało to jeszcze potwierdzone. W ten sposób nic nie ginie, gdy maszyna restartuje się w połowie.
- **Autobus zdarzeń** przekazuje zdarzenia wszystkim osobom, które się zapisały, i czeka, aż każde z nich je otrzyma.
  powiedz "zrozumiałem".

> **Pod maską:** pełna trasa z każdym stołem i wiadomością znajduje się w
> [ARCHITECTURE.md, sekcja 1](../ARCHITECTURE.md).

## Aktorzy w sześćdziesiąt sekund

ReactiveCQRS opiera się na aktorach **Apache Pekko**. Spotkasz ich, więc oto wszystko, czego potrzebujesz.
Teraz.

**Aktor** to mały pracownik ze skrzynką pocztową. Nigdy nie wywołujesz jego metod, tylko umieszczasz w nim wiadomość.
skrzynkę pocztową i obsługuje wiadomości pojedynczo, w kolejności ich nadejścia. Ponieważ zawsze działa tylko
jedna rzecz na raz, może zachować swój własny stan bez blokad.

Z aktorami rozmawia się na dwa sposoby:

- **tell**, napisane `actor ! message`: wrzuć wiadomość do skrzynki pocztowej i odejdź;
- **zapytaj**, napisane `aktor ? wiadomość`: wpisz wiadomość i otrzymaj `Przyszłość` z odpowiedzią.

```scala
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.duration._

niejawny limit czasu wartości: Limit czasu = Limit czasu (5 sekund)

val answer = commandBus ? AddItem(userId, cartId, AggregateVersion(1), "milk") // Przyszłość
```

Szyna poleceń, właściciele agregatów, szyna zdarzeń, projekcje i sagi – to wszystko są aktorzy.
Obsługujące polecenia i zdarzenia funkcje to zwykłe funkcje; aktorzy wywołują je za ciebie.

## Kiedy go używać, a kiedy nie

ReactiveCQRS sprawdza się, gdy:

- musisz wiedzieć, **jak** dane stały się takie, jakie są: audyty, „kto to zmienił?”, cofanie;
- kilka ekranów potrzebuje tych samych danych w **różnych kształtach**;
- reguły biznesowe dotyczą **rzeczy z tożsamością** (koszyk, zamówienie, projekt), które
  zmieniać się w czasie;
- chcesz modeli, które możesz **odbudować**, gdy wymagania się zmienią.

To prawdopodobnie przesada, gdy:

- aplikacja składa się głównie z prostych formularzy nad tabelą, bez zapisywania historii;
- wszędzie potrzebujesz transakcji krzyżowych („przesyłaj pieniądze między dowolnymi dwoma z miliona”
  konta, atomowo, cały czas"). Agregaty są spójne pojedynczo; działają w obrębie nich
  przechodzi przez sagi, które wymagają większej uwagi.

## Czego się nauczyłeś

- **Wydarzenie** to fakt w czasie przeszłym; dziennik wydarzeń jest źródłem prawdy.
- **Polecenie** to prośba, którą można odrzucić. O tym decyduje **osoba obsługująca polecenie**.
- **Agregat** to coś, co ma swój własny dziennik, identyfikator, wersję i stan.
- Obsługa dwóch pisarzy naraz odbywa się za pomocą **blokady optymistycznej** (lub za pomocą współbieżnych poleceń, które ponawiają próbę).
- **CQRS** przechowuje stronę zapisu i stronę odczytu; **projekcje** budują stronę odczytu na podstawie zdarzeń.
- ReactiveCQRS zajmuje się instalacją (trasowaniem, przechowywaniem, dostarczaniem, ponawianiem prób i odbudową) na Pekko
  aktorów i PostgreSQL; Ty piszesz części biznesowe.

Następnie: [rozdział 2](02-setting-up.md) zawiera narzędzia, które trafią na Twoje biurko.