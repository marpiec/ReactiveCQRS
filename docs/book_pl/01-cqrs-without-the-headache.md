# Rozdział 1. CQRS i event sourcing bez bólu głowy

Pod koniec tego rozdziału będziesz wiedzieć, czym są *polecenia*, *zdarzenia*, *agregaty* i *projekcje*,
po co w ogóle pisać oprogramowanie w ten sposób i które części pracy ReactiveCQRS wykonuje za ciebie.
Na razie nie ma tu kodu do wpisywania. Zrób sobie kawę.

---

## Tablica i pamiętnik

Wyobraź sobie listę zakupów na tablicy w kuchni. Ktoś pisze „mleko”, ktoś inny dopisuje „chleb”, a jeszcze
ktoś ściera „mleko”, bo kupił je w drodze do domu. W każdej chwili tablica pokazuje *bieżącą* listę.
Nie powie ci jednak, jak ta lista powstała: kto dopisał chleb? Czy rano było na niej mleko? Czy ktoś
kiedyś napisał „czekolada” i po cichu to starł?

Teraz wyobraź sobie, że zamiast tablicy rodzina prowadzi **pamiętnik** tej listy:

1. Poniedziałek 08:10, Alicja: *dodała mleko*
2. Poniedziałek 12:30, Bob: *dodał chleb*
3. Poniedziałek 18:05, Alicja: *usunęła mleko*

W pamiętniku nigdy nie zmienia się tego, co już zapisano; przybywają tylko nowe linijki na dole. A kiedy
potrzebujesz bieżącej listy, czytasz pamiętnik od góry i odtwarzasz linijkę po linijce: mleko, potem
chleb, potem już bez mleka. Wynik: chleb. Ta sama odpowiedź co na tablicy, a do tego cała historia.

Oto **event sourcing** w jednym akapicie. Każda linijka pamiętnika to **zdarzenie**: fakt o czymś, co się
wydarzyło, zapisany w czasie przeszłym. Bieżąca lista to **stan**, a dostajesz go, odtwarzając zdarzenia
po kolei. Pamiętnik jest źródłem prawdy; tablica to tylko wygodny obrazek, który w każdej chwili można
narysować od nowa.

> **Pamiętaj:** zdarzenia to fakty z przeszłości. „Dodano mleko” pozostaje prawdą na zawsze, nawet gdy
> ktoś później usunie mleko. Usunięcie to *nowe* zdarzenie, a nie gumka do mazania.

## Polecenia: grzeczne prośby

Nikt nie pisze w pamiętniku bezpośrednio. O zmianę się *prosi*: „dopisz, proszę, czekoladę do listy”.
Taka prośba to **polecenie**. W odróżnieniu od zdarzenia polecenie można odrzucić: może lista jest pełna,
a może czekolady nie wolno jeść przed obiadem. Ktoś musi spojrzeć na bieżącą listę, zastosować domowe
zasady i zdecydować.

- Jeśli odpowiedź brzmi „tak”, decyzja zostaje zapisana jako jedno lub więcej zdarzeń („dodano czekoladę”).
- Jeśli brzmi „nie”, nic nie zostaje zapisane, a ty dowiadujesz się, dlaczego.

Przebieg jest więc zawsze taki sam:

```
command  →  decision (look at the current state, apply the rules)  →  events (or a refusal)
```

W ReactiveCQRS polecenia to case classy Scali, na przykład `AddItem(...)`, decyzję podejmuje napisana
przez ciebie funkcja zwana **handlerem polecenia**, a zdarzenia to case classy, na przykład
`ItemAdded("milk")`.

## Agregaty: każda rzecz ma swój pamiętnik

Prawdziwa aplikacja nie ma jednej listy zakupów, tylko tysiące koszyków, po jednym na klienta. Każdy
koszyk dostaje własny pamiętnik. Rzecz, do której należy pamiętnik, razem z zasadami, które go strzegą,
to **agregat**. Koszyk zakupów jest agregatem; tak samo faktura, konto użytkownika czy tablica projektu.

Każdy agregat ma:

- **id**, po którym znajdziesz jego pamiętnik (`AggregateId`, czyli liczbę),
- **wersję**: liczbę zdarzeń w jego pamiętniku. Możesz ją traktować jak numer strony z ostatnią linijką.
  Nowy koszyk zaczyna od wersji 0, po pierwszym zdarzeniu ma wersję 1 i tak dalej,
- **stan**: to, co dostajesz, odtwarzając jego zdarzenia (w koszyku: nazwę i produkty).

Dlaczego granica biegnie wokół jednego koszyka, a nie wokół „wszystkich koszyków”? Bo zasady koszyka
zawsze potrzebują tylko tego jednego koszyka. „Nie więcej niż pięć produktów” da się sprawdzić, patrząc na
pojedynczy koszyk. A skoro agregaty są małe, wiele z nich może zmieniać się w tym samym czasie i nie
wchodzić sobie w drogę.

### Gdy dwie osoby piszą naraz

Alicja otwiera koszyk w telefonie, Bob w laptopie. Oboje widzą wersję 3. Alicja dodaje mleko; koszyk
przechodzi do wersji 4. Bob, wciąż patrząc na wersję 3, dodaje chleb. Czy zmiana Boba powinna przejść?
Może tak, a może zasady powiedziałyby coś innego o wersji 4. ReactiveCQRS pozwala ci wybrać:

- Zwykłym poleceniem mówisz: „zmieniam wersję 3”. Jeśli koszyk w międzyczasie poszedł dalej, polecenie
  zostaje odrzucone z odpowiedzią o *współbieżnej modyfikacji*, a ty możesz spojrzeć jeszcze raz
  i ponowić próbę. To się nazywa **blokowanie optymistyczne**: nikt niczego nie blokuje z góry, a konflikty
  wychodzą na jaw wtedy, gdy się zdarzą.
- Poleceniem *współbieżnym* mówisz: „jakakolwiek jest teraz wersja”, a w razie konfliktu biblioteka sama
  ponawia próbę.

Rozdział 5 pokazuje oba sposoby.

## CQRS: jeden model do zapisu, inne do odczytu

Odtwarzanie pamiętnika za każdym razem, gdy ktoś chce zajrzeć do koszyka, działa, ale jest powolne, gdy
pytanie brzmi: „pokaż mi wszystkie koszyki z czekoladą”. Do takich pytań przyda się tabela, którą da się
przeszukiwać, z jednym wierszem na koszyk, stale aktualna.

Dlatego prowadzisz **dwie strony**:

- **stronę zapisu** (polecenia, agregaty, zdarzenia), gdzie zapadają decyzje i gdzie przechowywana jest
  prawda;
- **stronę odczytu**, gdzie zdarzenia przybierają taki kształt, o jaki najłatwiej pytać: lista koszyków
  każdego klienta, liczba pozycji dla każdego produktu, indeks wyszukiwania.

Ten podział ma swoją nazwę: **CQRS**, Command Query Responsibility Segregation. Wielkie słowa na określenie
prostego nawyku: *model, którym zapisujesz, nie musi być modelem, z którego czytasz.*

Elementy strony odczytu nazywamy **projekcjami**. Projekcja nasłuchuje zdarzeń i aktualizuje własne
tabele. Pomyśl o banku: księga główna zapisuje każdą transakcję (zdarzenia), a aplikacja bankowa pokazuje
ci schludne podsumowanie twoich kont (projekcję księgi). Gdyby podsumowanie kiedyś się zepsuło, bank mógłby
je odtworzyć z księgi. Z projekcjami możesz zrobić dokładnie to samo (rozdział 10).

> **Pamiętaj:** strona odczytu nadąża za zmianami *z opóźnieniem*. Gdy polecenie się powiedzie, jego
> zdarzenia docierają do projekcji chwilę później, zwykle w ciągu milisekund. Kiedy ekran musi od razu
> pokazać twoją własną zmianę, są na to narzędzia (rozdziały 7 i 8).

## Co ReactiveCQRS robi za ciebie

Pisanie tego wszystkiego ręcznie to mnóstwo hydrauliki: bezpieczne przechowywanie zdarzeń, obsługa dwóch
piszących naraz, dostarczanie każdego zdarzenia do każdej projekcji nawet po awarii, przebudowa modeli
odczytu. Hydraulikę bierze na siebie biblioteka; ty piszesz te części, które dotyczą twojego biznesu.

**Ty piszesz:**

- stan agregatu (case class),
- jego polecenia i zdarzenia (kolejne case classy),
- handlery poleceń (zasady) i handlery zdarzeń (jak każde zdarzenie zmienia stan),
- projekcje (jak zdarzenia stają się modelami odczytu),
- a gdy zadanie obejmuje kilka agregatów, także sagi (rozdział 12).

**ReactiveCQRS zajmuje się:**

- kierowaniem każdego polecenia do właściwego agregatu i pilnowaniem, by dwa polecenia nigdy nie zapisały
  tej samej jego wersji,
- trzymaniem agregatów w pamięci, gdy mają co robić, i usypianiem ich, gdy są bezczynne,
- zapisywaniem zdarzeń w PostgreSQL w jednej transakcji, z blokadą optymistyczną,
- dostarczaniem zdarzeń do projekcji i sag *co najmniej raz, po kolei w obrębie każdego agregatu*, przy
  czym każde zdarzenie zostaje zastosowane *raz*, nawet mimo awarii i restartów,
- ponawianiem, zapisywaniem i zgłaszaniem błędów, żeby nic nie zginęło po cichu,
- przebudową projekcji, migawkami dla długich historii, zwijaniem starej historii (squash) i pracą
  w klastrze złożonym z kilku maszyn.

Oto cała podróż jednego polecenia:

```
you ──command──▶ command bus ──▶ aggregate owner ──events──▶ PostgreSQL (events + outbox)
                                       │
                                       └──publish──▶ event bus ──┬──▶ projections ──▶ read models
                                                                 └──▶ sagas
```

- **Szyna poleceń** to drzwi wejściowe dla jednego typu agregatu (osobna dla koszyków, osobna dla faktur).
- **Właściciel agregatu** opiekuje się jednym agregatem (jednym koszykiem): trzyma jego stan, uruchamia
  twój handler polecenia i zapisuje zdarzenia.
- **PostgreSQL** przechowuje zdarzenia, a do tego **outbox**: listę zadań ze zdarzeniami, których któryś
  subskrybent jeszcze nie potwierdził. Dzięki temu nic nie ginie, gdy maszyna zrestartuje się w połowie
  pracy.
- **Szyna zdarzeń** przekazuje zdarzenia wszystkim, którzy je subskrybują, i czeka, aż każdy z nich
  odpowie „mam”.

> **Pod maską:** pełny przegląd, z każdą tabelą i każdym komunikatem, znajdziesz w
> [ARCHITECTURE.md, w sekcji 1](../ARCHITECTURE.md).

## Aktorzy w sześćdziesiąt sekund

ReactiveCQRS jest zbudowany na aktorach **Apache Pekko**. Jeszcze ich spotkasz, więc oto wszystko, czego
na razie potrzebujesz.

**Aktor** to mały pracownik ze skrzynką pocztową. Nigdy nie wywołujesz jego metod; wrzucasz wiadomość do
jego skrzynki, a on obsługuje wiadomości jedną po drugiej, w kolejności, w jakiej przyszły. Ponieważ
zawsze robi tylko jedną rzecz naraz, może trzymać własny stan bez żadnych blokad.

Z aktorami rozmawia się na dwa sposoby:

- **tell**, w kodzie `actor ! message`: wrzucasz wiadomość do skrzynki i odchodzisz;
- **ask**, w kodzie `actor ? message`: wrzucasz wiadomość i dostajesz z powrotem `Future` z odpowiedzią.

```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(5.seconds)

val answer = commandBus ? AddItem(userId, cartId, AggregateVersion(1), "milk")  // a Future
```

Szyna poleceń, właściciele agregatów, szyna zdarzeń, projekcje i sagi – wszystko to aktorzy. Twoje
handlery poleceń i handlery zdarzeń to zwykłe funkcje; aktorzy wywołują je za ciebie.

## Kiedy warto, a kiedy nie

ReactiveCQRS świetnie się sprawdza, gdy:

- musisz wiedzieć, **jak** dane doszły do obecnej postaci: audyty, „kto to zmienił?”, cofanie zmian;
- kilka ekranów potrzebuje tych samych danych w **różnych kształtach**;
- reguły biznesowe kręcą się wokół **rzeczy z tożsamością** (koszyka, zamówienia, projektu), które
  zmieniają się w czasie;
- chcesz mieć modele odczytu, które da się **przebudować**, gdy zmienią się wymagania.

Prawdopodobnie to strzelanie z armaty do wróbla, gdy:

- aplikacja to głównie proste formularze nad tabelą i nie trzeba przechowywać żadnej historii;
- wszędzie potrzebujesz transakcji obejmujących wiele rekordów („przelewaj pieniądze między dowolnymi
  dwoma z miliona kont, atomowo, bez przerwy”). Każdy agregat jest spójny sam w sobie; praca obejmująca
  kilka z nich idzie przez sagi, które wymagają więcej staranności.

## Co już umiesz

- **Zdarzenie** to fakt w czasie przeszłym; pamiętnik zdarzeń jest źródłem prawdy.
- **Polecenie** to prośba, którą można odrzucić; decyduje **handler polecenia**.
- **Agregat** to jedna rzecz z własnym pamiętnikiem, id, wersją i stanem.
- Dwóch piszących naraz to zadanie dla **blokowania optymistycznego** (albo dla poleceń współbieżnych,
  które same ponawiają próbę).
- **CQRS** rozdziela stronę zapisu i stronę odczytu; **projekcje** budują stronę odczytu ze zdarzeń.
- ReactiveCQRS robi hydraulikę (kierowanie, zapisywanie, dostarczanie, ponawianie, przebudowę) na aktorach
  Pekko i PostgreSQL; ty piszesz części biznesowe.

Dalej: [rozdział 2](02-setting-up.md) rozkłada narzędzia na twoim biurku.
