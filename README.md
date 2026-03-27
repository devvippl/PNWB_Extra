# PNWB_Extra

Dodatek enova365 (2512.5.6) rozszerzający listę **Analizy baz danych Extra**.

## Co robi

- Rejestruje folder: `Analizy baz danych Extra/Analizy baz danych Extra`.
- Dodaje akcję `Oblicz Extra`.
- `Oblicz Extra` liczy dane metodą SQL (zbiorczo dla zaznaczonych baz) i wpisuje wyniki do kolumn listy:
  - `StatusJPK`
  - `EDeklaracjaVATUE`, `DeklaracjaVATUE`
  - `EDeklaracjaPIT8AR`, `DeklaracjaPIT8AR`
  - `EDeklaracjaPIT4R`, `DeklaracjaPIT4R`
  - `EDeklaracjaCIT8`, `DeklaracjaCIT8`

## Build

```powershell
dotnet build .\PNWB_Extra.sln -c Release
```

Pliki wyjściowe:

- `bin\Release\PNWB_Extra.dll`
- `bin\Release\PNWB_Extra.UI.dll`

## Wdrożenie do enova

Skopiuj do katalogu dodatków enova:

- `PNWB_Extra.dll`
- `PNWB_Extra.UI.dll`

Pliki `.pdb` są tylko diagnostyczne (debug symbols) i nie są wymagane do działania dodatku.

Pliki `.deps.json` opisują zależności .NET; zwykle dla dodatku enova wystarczą same DLL, ale jeśli środowisko będzie zgłaszać brak zależności, dołóż również odpowiadające im `.deps.json`.
