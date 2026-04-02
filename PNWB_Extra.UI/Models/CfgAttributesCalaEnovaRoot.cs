using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Data;
using System.Linq;
using Microsoft.Data.SqlClient;
using Soneta.Business;
using Soneta.Business.App;
using Soneta.Business.Db;
using Soneta.Business.UI;
using Soneta.Types;

namespace PNWB_Extra.UI.Models;

public sealed class CfgAttributesCalaEnovaRoot : ISessionable
{
    private readonly List<CfgAttributesCalaEnovaRow> items = new List<CfgAttributesCalaEnovaRow>();
    private readonly BindingList<CfgAttributesCalaEnovaRow> visibleItems = new BindingList<CfgAttributesCalaEnovaRow>();
    private readonly Dictionary<string, List<CfgNodeMeta>> cfgNodesByDatabase = new Dictionary<string, List<CfgNodeMeta>>(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string[]> cfgAttributeNamesByNode = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
    private bool metadataLoaded;
    private string[] databaseOptions = Array.Empty<string>();
    private ViewInfo viewInfo;

    private string bazaZrodlowa;
    private string wezel1;
    private string wezel2;
    private string wezel3;
    private string wezel4;
    private string wezel5;
    private string wezel6;
    private string wezel7;
    private string wezel8;
    private string wezel9;
    private string wezel10;

    [Context]
    public Session Session { get; set; }

    [Caption("Baza źródłowa")]
    public string BazaZrodlowa
    {
        get => bazaZrodlowa;
        set
        {
            if (string.Equals(bazaZrodlowa, value, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            bazaZrodlowa = value;
            ClearLevelsFrom(1);
            Session?.InvokeChanged();
        }
    }

    [Caption("Węzeł 1")]
    public string Wezel1
    {
        get => wezel1;
        set => SetNodeLevel(ref wezel1, value, 1);
    }

    [Caption("Węzeł 2")]
    public string Wezel2
    {
        get => wezel2;
        set => SetNodeLevel(ref wezel2, value, 2);
    }

    [Caption("Węzeł 3")]
    public string Wezel3
    {
        get => wezel3;
        set => SetNodeLevel(ref wezel3, value, 3);
    }

    [Caption("Węzeł 4")]
    public string Wezel4
    {
        get => wezel4;
        set => SetNodeLevel(ref wezel4, value, 4);
    }

    [Caption("Węzeł 5")]
    public string Wezel5
    {
        get => wezel5;
        set => SetNodeLevel(ref wezel5, value, 5);
    }

    [Caption("Węzeł 6")]
    public string Wezel6
    {
        get => wezel6;
        set => SetNodeLevel(ref wezel6, value, 6);
    }

    [Caption("Węzeł 7")]
    public string Wezel7
    {
        get => wezel7;
        set => SetNodeLevel(ref wezel7, value, 7);
    }

    [Caption("Węzeł 8")]
    public string Wezel8
    {
        get => wezel8;
        set => SetNodeLevel(ref wezel8, value, 8);
    }

    [Caption("Węzeł 9")]
    public string Wezel9
    {
        get => wezel9;
        set => SetNodeLevel(ref wezel9, value, 9);
    }

    [Caption("Węzeł 10")]
    public string Wezel10
    {
        get => wezel10;
        set => SetNodeLevel(ref wezel10, value, 10);
    }

    public ViewInfo ViewInfo
    {
        get
        {
            if (viewInfo != null)
            {
                return viewInfo;
            }

            viewInfo = new ViewInfo
            {
                NonViewCollection = true,
                RowType = typeof(CfgAttributesCalaEnovaRow)
            };
            viewInfo.CreateView += (_, args) => { args.DataSource = visibleItems; };
            return viewInfo;
        }
    }

    public void ReplaceRows(IEnumerable<CfgAttributesCalaEnovaRow> rows)
    {
        items.Clear();
        if (rows != null)
        {
            items.AddRange(rows);
        }

        RefreshVisibleItems();
        Session?.InvokeChanged();
    }

    public CfgAttributesSelection GetSelection()
    {
        EnsureMetadataLoaded();
        if (databaseOptions.Length == 0)
        {
            throw new InvalidOperationException("Nie znaleziono baz w DBItems.");
        }

        string selectedDatabase = (BazaZrodlowa ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(selectedDatabase))
        {
            selectedDatabase = databaseOptions[0];
            bazaZrodlowa = selectedDatabase;
        }

        if (!databaseOptions.Contains(selectedDatabase, StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Wybrana baza źródłowa '{selectedDatabase}' nie istnieje na liście DBItems.");
        }

        string[] selectedPath = GetSelectedPath();
        if (selectedPath.Length == 0)
        {
            throw new InvalidOperationException("Wybierz co najmniej pierwszy poziom węzła.");
        }

        List<CfgNodeMeta> nodes = GetCfgNodesForDatabase(selectedDatabase);
        if (nodes.Count == 0)
        {
            throw new InvalidOperationException($"Nie udało się odczytać CfgNodes z bazy '{selectedDatabase}'.");
        }

        List<string> nodePath = new List<string>();
        string attributeName = null;
        int? currentNodeId = null;

        for (int i = 0; i < selectedPath.Length; i++)
        {
            string selectedName = selectedPath[i];
            CfgNodeMeta selectedNode = (i == 0)
                ? nodes.Where(n => IsRootParent(n.ParentId) && string.Equals(n.Name, selectedName, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(n => n.Id)
                    .FirstOrDefault()
                : nodes.Where(n => n.ParentId == currentNodeId && string.Equals(n.Name, selectedName, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(n => n.Id)
                    .FirstOrDefault();

            if (selectedNode != null)
            {
                currentNodeId = selectedNode.Id;
                nodePath.Add(selectedNode.Name);
                continue;
            }

            if (currentNodeId.HasValue)
            {
                string[] attributeNames = GetAttributeNameOptions(selectedDatabase, currentNodeId.Value);
                if (attributeNames.Contains(selectedName, StringComparer.OrdinalIgnoreCase))
                {
                    attributeName = selectedName;
                    break;
                }
            }

            throw new InvalidOperationException($"Nie udało się zmapować poziomu {i + 1} ('{selectedName}') do CfgNodes/CfgAttributes.");
        }

        if (nodePath.Count == 0)
        {
            throw new InvalidOperationException("Wybierz poprawną ścieżkę CfgNodes.");
        }

        string[] nodeLevels = GetNodeLevelValues()
            .Select(v => string.IsNullOrWhiteSpace(v) ? null : v.Trim())
            .ToArray();

        return new CfgAttributesSelection(selectedDatabase, nodePath, attributeName, nodeLevels);
    }

    public object GetListBazaZrodlowa()
    {
        EnsureMetadataLoaded();
        return databaseOptions;
    }

    public object GetListWezel1()
    {
        return GetNodeLevelOptions(1);
    }

    public object GetListWezel2()
    {
        return GetNodeLevelOptions(2);
    }

    public object GetListWezel3()
    {
        return GetNodeLevelOptions(3);
    }

    public object GetListWezel4()
    {
        return GetNodeLevelOptions(4);
    }

    public object GetListWezel5()
    {
        return GetNodeLevelOptions(5);
    }

    public object GetListWezel6()
    {
        return GetNodeLevelOptions(6);
    }

    public object GetListWezel7()
    {
        return GetNodeLevelOptions(7);
    }

    public object GetListWezel8()
    {
        return GetNodeLevelOptions(8);
    }

    public object GetListWezel9()
    {
        return GetNodeLevelOptions(9);
    }

    public object GetListWezel10()
    {
        return GetNodeLevelOptions(10);
    }

    private void EnsureMetadataLoaded()
    {
        if (metadataLoaded)
        {
            return;
        }

        if (Session?.Login?.Database is not SqlDatabase sqlDatabase)
        {
            databaseOptions = Array.Empty<string>();
            metadataLoaded = true;
            return;
        }

        string masterDatabaseName = ResolveMasterDatabaseName(Session, sqlDatabase);
        using SqlConnection connection = CreateSqlConnection(sqlDatabase, "PNWB_Extra.CfgAttributes.Meta");
        connection.Open();

        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = $@"
SELECT Name
FROM {QuoteSqlIdentifier(masterDatabaseName)}.dbo.DBItems
WHERE Name <> N'.'
ORDER BY Name;";

        List<string> dbs = new List<string>();
        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string dbName = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (!string.IsNullOrWhiteSpace(dbName))
            {
                dbs.Add(dbName);
            }
        }

        databaseOptions = dbs.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        metadataLoaded = true;
        if (string.IsNullOrWhiteSpace(bazaZrodlowa) && databaseOptions.Length > 0)
        {
            bazaZrodlowa = databaseOptions[0];
        }
    }

    private object GetNodeLevelOptions(int level)
    {
        EnsureMetadataLoaded();
        if (string.IsNullOrWhiteSpace(BazaZrodlowa))
        {
            return Array.Empty<string>();
        }

        List<CfgNodeMeta> nodes = GetCfgNodesForDatabase(BazaZrodlowa);
        if (nodes.Count == 0)
        {
            return Array.Empty<string>();
        }

        if (level == 1)
        {
            return nodes.Where(n => IsRootParent(n.ParentId))
                .Select(n => n.Name)
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(n => n)
                .ToArray();
        }

        if (!TryResolveParentNodeId(nodes, level - 1, out int parentId, out bool endedOnAttribute))
        {
            return Array.Empty<string>();
        }

        if (endedOnAttribute)
        {
            return Array.Empty<string>();
        }

        string[] childNodeOptions = nodes.Where(n => n.ParentId == parentId)
            .Select(n => n.Name)
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n)
            .ToArray();

        if (childNodeOptions.Length > 0)
        {
            return childNodeOptions;
        }

        return GetAttributeNameOptions(BazaZrodlowa, parentId);
    }

    private List<CfgNodeMeta> GetCfgNodesForDatabase(string databaseName)
    {
        if (cfgNodesByDatabase.TryGetValue(databaseName, out List<CfgNodeMeta> cached))
        {
            return cached;
        }

        List<CfgNodeMeta> rows = new List<CfgNodeMeta>();
        if (Session?.Login?.Database is not SqlDatabase sqlDatabase)
        {
            cfgNodesByDatabase[databaseName] = rows;
            return rows;
        }

        try
        {
            using SqlConnection connection = CreateSqlConnection(sqlDatabase, "PNWB_Extra.CfgAttributes.Nodes");
            connection.Open();

            using SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = $@"
SELECT TRY_CONVERT(int, ID) AS ID,
       TRY_CONVERT(int, Parent) AS ParentID,
       CONVERT(nvarchar(255), Name) AS Name
FROM {QuoteSqlIdentifier(databaseName)}.dbo.CfgNodes
WHERE Name IS NOT NULL
ORDER BY Name, ID;";

            using SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                if (reader.IsDBNull(0) || reader.IsDBNull(2))
                {
                    continue;
                }

                rows.Add(new CfgNodeMeta
                {
                    Id = reader.GetInt32(0),
                    ParentId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                    Name = reader.GetString(2)
                });
            }
        }
        catch
        {
            rows.Clear();
        }

        cfgNodesByDatabase[databaseName] = rows;
        return rows;
    }

    private bool TryResolveParentNodeId(IEnumerable<CfgNodeMeta> nodes, int maxLevel, out int parentId, out bool endedOnAttribute)
    {
        parentId = 0;
        endedOnAttribute = false;
        int? currentNodeId = null;
        string databaseName = BazaZrodlowa ?? string.Empty;

        for (int level = 1; level <= maxLevel; level++)
        {
            string selectedName = GetNodeLevelValue(level);
            if (string.IsNullOrWhiteSpace(selectedName))
            {
                return false;
            }

            CfgNodeMeta selectedNode = level == 1
                ? nodes.Where(n => IsRootParent(n.ParentId) && string.Equals(n.Name, selectedName, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(n => n.Id)
                    .FirstOrDefault()
                : nodes.Where(n => n.ParentId == currentNodeId && string.Equals(n.Name, selectedName, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(n => n.Id)
                    .FirstOrDefault();

            if (selectedNode == null)
            {
                if (!currentNodeId.HasValue)
                {
                    return false;
                }

                string[] attributeNames = GetAttributeNameOptions(databaseName, currentNodeId.Value);
                if (!attributeNames.Contains(selectedName, StringComparer.OrdinalIgnoreCase))
                {
                    return false;
                }

                parentId = currentNodeId.Value;
                endedOnAttribute = true;
                return true;
            }

            currentNodeId = selectedNode.Id;
        }

        if (!currentNodeId.HasValue)
        {
            return false;
        }

        parentId = currentNodeId.Value;
        return true;
    }

    private static bool IsRootParent(int? parentId)
    {
        return !parentId.HasValue || parentId.Value <= 1;
    }

    private string[] GetAttributeNameOptions(string databaseName, int nodeId)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return Array.Empty<string>();
        }

        string cacheKey = $"{databaseName}|{nodeId}";
        if (cfgAttributeNamesByNode.TryGetValue(cacheKey, out string[] cached))
        {
            return cached;
        }

        if (Session?.Login?.Database is not SqlDatabase sqlDatabase)
        {
            return CacheAttributeNames(cacheKey, Array.Empty<string>());
        }

        List<string> values = new List<string>();
        try
        {
            using SqlConnection connection = CreateSqlConnection(sqlDatabase, "PNWB_Extra.CfgAttributes.AttrNames");
            connection.Open();

            using SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = $@"
SELECT DISTINCT LTRIM(RTRIM(CONVERT(nvarchar(255), [Name]))) AS [Name]
FROM {QuoteSqlIdentifier(databaseName)}.dbo.CfgAttributes
WHERE TRY_CONVERT(int, [Node]) = @nodeId
  AND [Name] IS NOT NULL
  AND LTRIM(RTRIM(CONVERT(nvarchar(255), [Name]))) <> N''
ORDER BY [Name];";
            command.Parameters.Add("@nodeId", SqlDbType.Int).Value = nodeId;

            using SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                if (!reader.IsDBNull(0))
                {
                    values.Add(reader.GetString(0));
                }
            }
        }
        catch
        {
            values.Clear();
        }

        return CacheAttributeNames(cacheKey, values.Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(v => v).ToArray());
    }

    private string[] CacheAttributeNames(string cacheKey, string[] values)
    {
        cfgAttributeNamesByNode[cacheKey] = values;
        return values;
    }

    private string[] GetSelectedPath()
    {
        return GetNodeLevelValues()
            .TakeWhile(v => !string.IsNullOrWhiteSpace(v))
            .Select(v => v.Trim())
            .ToArray();
    }

    private string[] GetNodeLevelValues()
    {
        return new[]
        {
            Wezel1, Wezel2, Wezel3, Wezel4, Wezel5,
            Wezel6, Wezel7, Wezel8, Wezel9, Wezel10
        };
    }

    private string GetNodeLevelValue(int level)
    {
        return level switch
        {
            1 => Wezel1,
            2 => Wezel2,
            3 => Wezel3,
            4 => Wezel4,
            5 => Wezel5,
            6 => Wezel6,
            7 => Wezel7,
            8 => Wezel8,
            9 => Wezel9,
            10 => Wezel10,
            _ => null
        };
    }

    private void SetNodeLevel(ref string field, string value, int level)
    {
        if (string.Equals(field, value, StringComparison.Ordinal))
        {
            return;
        }

        field = value;
        ClearLevelsFrom(level + 1);
        Session?.InvokeChanged();
    }

    private void ClearLevelsFrom(int level)
    {
        if (level <= 1) wezel1 = null;
        if (level <= 2) wezel2 = null;
        if (level <= 3) wezel3 = null;
        if (level <= 4) wezel4 = null;
        if (level <= 5) wezel5 = null;
        if (level <= 6) wezel6 = null;
        if (level <= 7) wezel7 = null;
        if (level <= 8) wezel8 = null;
        if (level <= 9) wezel9 = null;
        if (level <= 10) wezel10 = null;
    }

    private void RefreshVisibleItems()
    {
        visibleItems.Clear();
        foreach (CfgAttributesCalaEnovaRow row in items)
        {
            visibleItems.Add(row);
        }
    }

    private static string ResolveMasterDatabaseName(Session session, SqlDatabase sqlDatabase)
    {
        string configuredMaster = BusinessModule.GetInstance(session).Config.DBItemsManager.MasterDatabaseName;
        if (!string.IsNullOrWhiteSpace(configuredMaster))
        {
            return configuredMaster;
        }

        return sqlDatabase.DatabaseName;
    }

    private static SqlConnection CreateSqlConnection(SqlDatabase sqlDatabase, string appName)
    {
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
        {
            DataSource = sqlDatabase.Server,
            InitialCatalog = sqlDatabase.DatabaseName,
            IntegratedSecurity = true,
            ApplicationName = appName,
            TrustServerCertificate = true
        };
        return new SqlConnection(builder.ConnectionString);
    }

    private static string QuoteSqlIdentifier(string value)
    {
        return $"[{value.Replace("]", "]]")}]";
    }
}

public sealed class CfgAttributesSelection
{
    public CfgAttributesSelection(string metadataDatabase, IReadOnlyList<string> nodePath, string attributeName, IReadOnlyList<string> nodeLevels)
    {
        MetadataDatabase = metadataDatabase;
        NodePath = new ReadOnlyCollection<string>(nodePath.ToArray());
        AttributeName = string.IsNullOrWhiteSpace(attributeName) ? null : attributeName.Trim();
        string[] normalizedLevels = (nodeLevels ?? Array.Empty<string>())
            .Take(10)
            .Select(v => string.IsNullOrWhiteSpace(v) ? null : v.Trim())
            .Concat(Enumerable.Repeat<string>(null, 10))
            .Take(10)
            .ToArray();
        NodeLevels = new ReadOnlyCollection<string>(normalizedLevels);
    }

    public string MetadataDatabase { get; }

    public IReadOnlyList<string> NodePath { get; }

    public string AttributeName { get; }

    public IReadOnlyList<string> NodeLevels { get; }
}

public sealed class CfgAttributesCalaEnovaRow
{
    [Caption("Nazwa firmy")]
    public string NazwaFirmy { get; set; }

    [Caption("ID")]
    public int? ID { get; set; }

    [Caption("NodeID")]
    public int? Node { get; set; }

    [Caption("NodeGuid")]
    public string NodeGuid { get; set; }

    [Caption("NodeParent")]
    public int? NodeParent { get; set; }

    [Caption("NodeName")]
    public string NodeName { get; set; }

    [Caption("NodeType")]
    public int? NodeType { get; set; }

    [Caption("Name")]
    public string Name { get; set; }

    [Caption("Type")]
    public int? Type { get; set; }

    [Caption("StrValue")]
    public string StrValue { get; set; }

    [Caption("MemoValue")]
    public string MemoValue { get; set; }

    [Caption("Stamp")]
    public string Stamp { get; set; }

    [Caption("Węzeł 1")]
    public string Wezel1 { get; set; }

    [Caption("Węzeł 2")]
    public string Wezel2 { get; set; }

    [Caption("Węzeł 3")]
    public string Wezel3 { get; set; }

    [Caption("Węzeł 4")]
    public string Wezel4 { get; set; }

    [Caption("Węzeł 5")]
    public string Wezel5 { get; set; }

    [Caption("Węzeł 6")]
    public string Wezel6 { get; set; }

    [Caption("Węzeł 7")]
    public string Wezel7 { get; set; }

    [Caption("Węzeł 8")]
    public string Wezel8 { get; set; }

    [Caption("Węzeł 9")]
    public string Wezel9 { get; set; }

    [Caption("Węzeł 10")]
    public string Wezel10 { get; set; }
}

internal sealed class CfgNodeMeta
{
    public int Id { get; set; }

    public int? ParentId { get; set; }

    public string Name { get; set; }
}
