using Soneta.Business.UI;
using PNWB_Extra.UI.ViewInfos;
using PNWB_Extra.UI.Models;

[assembly: FolderView("Analizy baz danych Extra",
    Priority = 990,
    GroupIndex = 1,
    Description = "Analizy oraz czynności wykonywane na zarejestrowanych bazach danych",
    IconName = "baza_danych")]

[assembly: FolderView("Analizy baz danych Extra/Analizy baz danych Extra",
    Priority = 10,
    GroupIndex = 1,
    TableName = "DBItems",
    Description = "Analizy oraz czynności wykonywane na zarejestrowanych bazach danych",
    IconName = "wykres",
    ViewType = typeof(AnalizyBazDanychExtraViewInfo))]

[assembly: FolderView("Analizy baz danych Extra/Systemy zewnętrzne cała enova",
    Priority = 20,
    GroupIndex = 1,
    Description = "Zbiorczy widok tabeli SystemyZewn ze wszystkich baz enova",
    IconName = "global",
    ObjectType = typeof(SystemyZewnCalaEnovaRoot),
    ObjectPage = "SystemyZewnCalaEnovaRoot.Ogolne.pageform.xml",
    ReadOnlySession = true,
    ConfigSession = true)]

[assembly: FolderView("Analizy baz danych Extra/Tokeny cała enova",
    Priority = 30,
    GroupIndex = 1,
    Description = "Zbiorczy widok tabeli SysZewTokeny ze wszystkich baz enova",
    IconName = "klucz",
    ObjectType = typeof(TokenyCalaEnovaRoot),
    ObjectPage = "TokenyCalaEnovaRoot.Ogolne.pageform.xml",
    ReadOnlySession = true,
    ConfigSession = true)]

[assembly: FolderView("Analizy baz danych Extra/Dynamiczne dane cała enova",
    Priority = 40,
    GroupIndex = 1,
    Description = "Dynamiczny podgląd danych z wybranej tabeli lub widoku SQL ze wszystkich baz enova",
    IconName = "sql",
    ObjectType = typeof(DynamiczneDaneCalaEnovaRoot),
    ObjectPage = "DynamiczneDaneCalaEnovaRoot.Ogolne.pageform.xml",
    ReadOnlySession = true,
    ConfigSession = true)]

[assembly: FolderView("Analizy baz danych Extra/CfgAttributes cała enova",
    Priority = 50,
    GroupIndex = 1,
    Description = "Zbiorczy widok CfgAttributes dla wybranej ścieżki CfgNodes ze wszystkich baz enova",
    IconName = "ustawienia",
    ObjectType = typeof(CfgAttributesCalaEnovaRoot),
    ObjectPage = "CfgAttributesCalaEnovaRoot.Ogolne.pageform.xml",
    ReadOnlySession = true,
    ConfigSession = true)]

[assembly: FolderView("Analizy baz danych Extra/Unicode cała enova",
    Priority = 60,
    GroupIndex = 1,
    Description = "Zbiorczy status kolumny SystemInfos.Value (Unicode/VARCHAR) we wszystkich bazach enova",
    IconName = "sql",
    ObjectType = typeof(UnicodeCalaEnovaRoot),
    ObjectPage = "UnicodeCalaEnovaRoot.Ogolne.pageform.xml",
    ReadOnlySession = true,
    ConfigSession = true)]
namespace PNWB_Extra.UI.Navigation;
