using Soneta.Business.UI;
using PNWB_Extra.UI.ViewInfos;

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
namespace PNWB_Extra.UI.Navigation;
