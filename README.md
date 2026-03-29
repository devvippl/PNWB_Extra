# PNWB_Extra

Dodatek dla enova365 `2512.5.6`, rozszerzający obszar **Praca na wielu bazach** o własne analizy SQL.

## Funkcje

Menu dodatku:

- `Analizy baz danych Extra/Analizy baz danych Extra`
- `Analizy baz danych Extra/Systemy zewnętrzne cała enova`
- `Analizy baz danych Extra/Tokeny cała enova`
- `Analizy baz danych Extra/Dynamiczne dane cała enova`

### 1) Analizy baz danych Extra

Akcja `Oblicz Extra` liczy (SQL, zbiorczo po bazach) wybrane właściwości DBItems:

- `CalculatedProperties.StatusJPK`
- `CalculatedProperties.EDeklaracjaVATUE`
- `CalculatedProperties.DeklaracjaVATUE`
- `CalculatedProperties.EDeklaracjaPIT8AR`
- `CalculatedProperties.DeklaracjaPIT8AR`
- `CalculatedProperties.EDeklaracjaPIT4R`
- `CalculatedProperties.DeklaracjaPIT4R`
- `CalculatedProperties.EDeklaracjaCIT8`
- `CalculatedProperties.DeklaracjaCIT8`

### 2) Systemy zewnętrzne cała enova

Zbiorcza lista `SystemyZewn` (UNION ALL po bazach z DBItems) z filtrami:

- `Symbol`
- `Blokada`

Akcje:

- `Oblicz Extra` - ładowanie danych.
- `Zapisz zmiany` - zapis tylko zmienionych rekordów do SQL.

### 3) Tokeny cała enova

Zbiorcza lista `SysZewTokeny` (UNION ALL po bazach z DBItems).

Akcje:

- `Oblicz Extra` - ładowanie danych.
- `Zapisz zmiany` - zapis tylko zmienionych rekordów do SQL.

### 4) Dynamiczne dane cała enova

Dynamiczny podgląd dowolnej tabeli/widoku SQL po wszystkich bazach z DBItems.

Filtry:

- `Tabela / Widok`
- `Kolumna` (`*` lub konkretna kolumna)
- `Where kolumna`
- `Operator` (`=`, `<>`, `>`, `>=`, `<`, `<=`, `LIKE`, `NOT LIKE`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `IS NULL`, `IS NOT NULL`)
- `Wartość`

Akcje:

- `Oblicz Extra` - buduje i wykonuje dynamiczny UNION ALL.
- `Zapisz zmiany` - zapis tylko zmienionych rekordów do SQL, z warunkami bezpieczeństwa:
  - działa wyłącznie dla `Kolumna = *`,
  - wymaga klucza głównego (PK) w obiekcie SQL,
  - aktualizuje tylko kolumny faktycznie zmienione.

## Build

```powershell
dotnet build .\PNWB_Extra.sln -c Release
```

Pliki wyjściowe:

- `bin\Release\PNWB_Extra.dll`
- `bin\Release\PNWB_Extra.UI.dll`
- `bin\Release\PNWB_Extra.deps.json`
- `bin\Release\PNWB_Extra.UI.deps.json`

## Wdrożenie do enova

Skopiuj do katalogu dodatków enova:

- `PNWB_Extra.dll`
- `PNWB_Extra.UI.dll`

Opcjonalnie (gdy środowisko wymaga):

- `PNWB_Extra.deps.json`
- `PNWB_Extra.UI.deps.json`

Pliki `.pdb` są diagnostyczne i nie są wymagane do działania dodatku.
