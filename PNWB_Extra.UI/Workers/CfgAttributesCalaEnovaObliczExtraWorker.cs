using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Data.SqlClient;
using PNWB_Extra.UI.Models;
using Soneta.Business;
using Soneta.Business.App;
using Soneta.Business.Db;
using Soneta.Commands;

[assembly: Worker(typeof(PNWB_Extra.UI.Workers.CfgAttributesCalaEnovaObliczExtraWorker), typeof(PNWB_Extra.UI.Models.CfgAttributesCalaEnovaRoot))]

namespace PNWB_Extra.UI.Workers;

public sealed class CfgAttributesCalaEnovaObliczExtraWorker
{
    [Context]
    public CfgAttributesCalaEnovaRoot Root { get; set; }

    private Context context;

    [Context]
    public Context Context
    {
        set { context = value; }
    }

    [Action("Oblicz Extra", Priority = 2, Icon = ActionIcon.Play, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.SingleSession | ActionMode.ReadOnlySession | ActionMode.Progress | ActionMode.OnlyWinForms))]
    public void CalculateExtraForms()
    {
        CalculateExtraCore();
        context.InvokeChanged();
    }

    [Action("Oblicz Extra", Priority = 2, Icon = ActionIcon.Play, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.Progress | ActionMode.OnlyWebForms | ActionMode.NoSession | ActionMode.ReadOnlySession | ActionMode.SingleSession))]
    public void CalculateExtraWeb()
    {
        CalculateExtraCore();
        context.InvokeChanged();
    }

    private void CalculateExtraCore()
    {
        Session session = context.Session;
        bool ownsSession = false;
        if (session == null)
        {
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.CfgAttributesCalaEnova.ObliczExtra");
            ownsSession = true;
        }

        try
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            CfgAttributesSelection selection = Root.GetSelection();
            (List<CfgAttributesCalaEnovaRow> rows, List<string> errors) = LoadRowsFromSql(session, selection);
            Root.ReplaceRows(rows);
            stopwatch.Stop();

            int firmyCount = rows.Select(r => r.NazwaFirmy).Distinct(StringComparer.OrdinalIgnoreCase).Count();
            Log log = new Log("CfgAttributes cała enova", open: true);
            string pathText = string.Join(" / ", selection.NodePath);
            if (!string.IsNullOrWhiteSpace(selection.AttributeName))
            {
                pathText += " / " + selection.AttributeName;
            }

            log.WriteLine("Oblicz Extra SQL: ścieżka={0}, rekordy={1}, firmy={2}, czas={3} ms",
                pathText,
                rows.Count,
                firmyCount,
                stopwatch.ElapsedMilliseconds);
            if (errors.Count > 0)
            {
                foreach (string error in errors)
                {
                    log.WriteLine("Błąd bazy: {0}", error);
                }
            }
        }
        finally
        {
            if (ownsSession)
            {
                session.Dispose();
            }
        }
    }

    private static (List<CfgAttributesCalaEnovaRow> Rows, List<string> Errors) LoadRowsFromSql(Session session, CfgAttributesSelection selection)
    {
        if (session.Login.Database is not SqlDatabase sqlDatabase)
        {
            throw new InvalidOperationException("Bieżąca baza enova nie jest bazą SQL.");
        }

        string masterDatabaseName = ResolveMasterDatabaseName(session, sqlDatabase);

        using SqlConnection connection = CreateSqlConnection(sqlDatabase);
        connection.Open();

        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = BuildCfgAttributesBatchSql(masterDatabaseName, selection.NodePath, selection.AttributeName);

        List<CfgAttributesCalaEnovaRow> rows = new List<CfgAttributesCalaEnovaRow>();
        List<string> errors = new List<string>();
        IReadOnlyList<string> levels = selection.NodeLevels;
        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new CfgAttributesCalaEnovaRow
            {
                NazwaFirmy = ReadString(reader, 0),
                ID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                Node = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                NodeGuid = ReadString(reader, 3),
                NodeParent = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                NodeName = ReadString(reader, 5),
                NodeType = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                Name = ReadString(reader, 7),
                Type = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                StrValue = ReadString(reader, 9),
                MemoValue = ReadString(reader, 10),
                Stamp = ReadString(reader, 11),
                Wezel1 = GetNodeLevel(levels, 1),
                Wezel2 = GetNodeLevel(levels, 2),
                Wezel3 = GetNodeLevel(levels, 3),
                Wezel4 = GetNodeLevel(levels, 4),
                Wezel5 = GetNodeLevel(levels, 5),
                Wezel6 = GetNodeLevel(levels, 6),
                Wezel7 = GetNodeLevel(levels, 7),
                Wezel8 = GetNodeLevel(levels, 8),
                Wezel9 = GetNodeLevel(levels, 9),
                Wezel10 = GetNodeLevel(levels, 10)
            });
        }

        if (reader.NextResult())
        {
            while (reader.Read())
            {
                string dbName = ReadString(reader, 0) ?? "?";
                string message = ReadString(reader, 1) ?? "Brak opisu błędu";
                errors.Add($"{dbName}: {message}");
            }
        }

        return (rows, errors);
    }

    private static string BuildCfgAttributesBatchSql(string masterDatabaseName, IReadOnlyList<string> nodePath, string attributeName)
    {
        string masterDbQuoted = QuoteSqlIdentifier(masterDatabaseName);
        string nodeSelectSqlTemplate = BuildNodeSelectTemplate(nodePath, attributeName);

        return $@"
DECLARE @dbname NVARCHAR(128);

DECLARE @results TABLE (
    NazwaFirmy nvarchar(128) NOT NULL,
    ID int NULL,
    Node int NULL,
    NodeGuid nvarchar(36) NULL,
    NodeParent int NULL,
    NodeName nvarchar(255) NULL,
    NodeType int NULL,
    [Name] nvarchar(255) NULL,
    [Type] int NULL,
    StrValue nvarchar(max) NULL,
    MemoValue nvarchar(max) NULL,
    Stamp nvarchar(100) NULL
);

DECLARE @errors TABLE (
    DatabaseName nvarchar(128) NOT NULL,
    ErrorMessage nvarchar(4000) NULL
);

DECLARE @DBItems TABLE (DatabaseName NVARCHAR(128) NOT NULL);
INSERT INTO @DBItems (DatabaseName)
SELECT Name
FROM {masterDbQuoted}.dbo.DBItems
WHERE Name <> N'.';

DECLARE db_cursor CURSOR FAST_FORWARD FOR
    SELECT DatabaseName
    FROM @DBItems;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @dbname;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        IF DB_ID(@dbname) IS NOT NULL
           AND OBJECT_ID(QUOTENAME(@dbname) + N'.dbo.CfgNodes') IS NOT NULL
           AND OBJECT_ID(QUOTENAME(@dbname) + N'.dbo.CfgAttributes') IS NOT NULL
        BEGIN
            DECLARE @selectSql nvarchar(max) = N'{EscapeSqlLiteral(nodeSelectSqlTemplate)}';
            DECLARE @dbNameLiteral nvarchar(300) = REPLACE(@dbname, N'''', N'''''');
            SET @selectSql = REPLACE(@selectSql, N'__DBIDENT__', QUOTENAME(@dbname));
            SET @selectSql = REPLACE(@selectSql, N'__DBNAME__', @dbNameLiteral);

            INSERT INTO @results (NazwaFirmy, ID, Node, NodeGuid, NodeParent, NodeName, NodeType, [Name], [Type], StrValue, MemoValue, Stamp)
            EXEC sp_executesql @selectSql;
        END
    END TRY
    BEGIN CATCH
        INSERT INTO @errors (DatabaseName, ErrorMessage)
        VALUES (@dbname, ERROR_MESSAGE());
    END CATCH;

    FETCH NEXT FROM db_cursor INTO @dbname;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

SELECT NazwaFirmy, ID, Node, NodeGuid, NodeParent, NodeName, NodeType, [Name], [Type], StrValue, MemoValue, Stamp
FROM @results
ORDER BY NazwaFirmy, [Name], ID;

SELECT DatabaseName, ErrorMessage
FROM @errors
ORDER BY DatabaseName;";
    }

    private static string BuildNodeSelectTemplate(IReadOnlyList<string> nodePath, string attributeName)
    {
        const string dbNameToken = "__DBNAME__";
        const string dbIdentifierToken = "__DBIDENT__";
        string dbQuoted = dbIdentifierToken;
        int depth = nodePath?.Count ?? 0;
        if (depth <= 0)
        {
            throw new InvalidOperationException("Brak ścieżki węzłów do zapytania.");
        }

        string level1Name = EscapeSqlLiteral(nodePath[0] ?? string.Empty);
        StringBuilder fromBuilder = new StringBuilder();
        fromBuilder.AppendLine($"FROM {dbQuoted}.dbo.CfgNodes N1");

        for (int i = 2; i <= depth; i++)
        {
            string nodeNameLiteral = EscapeSqlLiteral(nodePath[i - 1] ?? string.Empty);
            fromBuilder.AppendLine($@"INNER JOIN {dbQuoted}.dbo.CfgNodes N{i}
        ON N{i}.Parent = N{i - 1}.ID
       AND LTRIM(RTRIM(CONVERT(nvarchar(255), N{i}.Name))) = N'{nodeNameLiteral}'");
        }

        string finalAlias = $"N{depth}";
        string attributeFilterSql = string.IsNullOrWhiteSpace(attributeName)
            ? string.Empty
            : $"  AND LTRIM(RTRIM(CONVERT(nvarchar(255), A.Name))) = N'{EscapeSqlLiteral(attributeName)}'{Environment.NewLine}";
        return $@"
DECLARE @nodes TABLE (ID int NOT NULL PRIMARY KEY);
INSERT INTO @nodes(ID)
SELECT DISTINCT TRY_CONVERT(int, {finalAlias}.ID) AS NodeID
    {fromBuilder}
    WHERE (N1.Parent IS NULL OR TRY_CONVERT(int, N1.Parent) <= 1)
      AND LTRIM(RTRIM(CONVERT(nvarchar(255), N1.Name))) = N'{level1Name}'
      AND TRY_CONVERT(int, {finalAlias}.ID) IS NOT NULL;

DECLARE @added int = 1;
WHILE @added > 0
BEGIN
    INSERT INTO @nodes(ID)
    SELECT C.ID
    FROM {dbQuoted}.dbo.CfgNodes C
    INNER JOIN @nodes P ON C.Parent = P.ID
    LEFT JOIN @nodes E ON E.ID = C.ID
    WHERE E.ID IS NULL;

    SET @added = @@ROWCOUNT;
END;

SELECT N'{EscapeSqlLiteral(dbNameToken)}' AS NazwaFirmy,
       TRY_CONVERT(int, A.ID) AS ID,
       TRY_CONVERT(int, A.Node) AS Node,
       CONVERT(nvarchar(36), N.Guid) AS NodeGuid,
       TRY_CONVERT(int, N.Parent) AS NodeParent,
       CONVERT(nvarchar(255), N.Name) AS NodeName,
       TRY_CONVERT(int, N.Type) AS NodeType,
       CONVERT(nvarchar(255), A.Name) AS [Name],
       TRY_CONVERT(int, A.Type) AS [Type],
       CONVERT(nvarchar(max), A.StrValue) AS StrValue,
       CONVERT(nvarchar(max), A.MemoValue) AS MemoValue,
       CASE WHEN A.Stamp IS NULL THEN NULL ELSE sys.fn_varbintohexstr(CONVERT(varbinary(8), A.Stamp)) END AS Stamp
FROM @nodes T
INNER JOIN {dbQuoted}.dbo.CfgAttributes A ON TRY_CONVERT(int, A.Node) = T.ID
INNER JOIN {dbQuoted}.dbo.CfgNodes N ON TRY_CONVERT(int, N.ID) = TRY_CONVERT(int, A.Node)
WHERE 1 = 1
{attributeFilterSql};";
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

    private static SqlConnection CreateSqlConnection(SqlDatabase sqlDatabase)
    {
        return PnwbSqlConnectionFactory.Create(sqlDatabase, "PNWB_Extra.CfgAttributesCalaEnova");
    }

    private static string ReadString(SqlDataReader reader, int index)
    {
        return reader.IsDBNull(index) ? null : reader.GetString(index);
    }

    private static string GetNodeLevel(IReadOnlyList<string> levels, int level)
    {
        if (levels == null || level <= 0 || level > levels.Count)
        {
            return null;
        }

        string value = levels[level - 1];
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static string EscapeSqlLiteral(string value)
    {
        return (value ?? string.Empty).Replace("'", "''");
    }

    private static string QuoteSqlIdentifier(string value)
    {
        return $"[{(value ?? string.Empty).Replace("]", "]]")}]";
    }
}
