using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using Microsoft.Data.SqlClient;
using PNWB_Extra.UI.Models;
using Soneta.Business;
using Soneta.Business.App;
using Soneta.Business.Db;
using Soneta.Commands;

[assembly: Worker(typeof(PNWB_Extra.UI.Workers.SystemyZewnCalaEnovaObliczExtraWorker), typeof(PNWB_Extra.UI.Models.SystemyZewnCalaEnovaRoot))]

namespace PNWB_Extra.UI.Workers;

public sealed class SystemyZewnCalaEnovaObliczExtraWorker
{
    [Context]
    public SystemyZewnCalaEnovaRoot Root { get; set; }

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

    [Action("Zapisz zmiany", Priority = 3, Icon = ActionIcon.Save, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.SingleSession | ActionMode.ReadOnlySession | ActionMode.Progress | ActionMode.OnlyWinForms))]
    public void SaveChangesForms()
    {
        Root.SaveAllRows();
        CalculateExtraCore();
        context.InvokeChanged();
    }

    [Action("Zapisz zmiany", Priority = 3, Icon = ActionIcon.Save, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.Progress | ActionMode.OnlyWebForms | ActionMode.NoSession | ActionMode.ReadOnlySession | ActionMode.SingleSession))]
    public void SaveChangesWeb()
    {
        Root.SaveAllRows();
        CalculateExtraCore();
        context.InvokeChanged();
    }

    private void CalculateExtraCore()
    {
        Session session = context.Session;
        bool ownsSession = false;
        if (session == null)
        {
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.SystemyZewnCalaEnova.ObliczExtra");
            ownsSession = true;
        }

        try
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            List<SystemyZewnCalaEnovaRow> rows = LoadRowsFromSql(session);

            Root.ReplaceRows(rows);
            stopwatch.Stop();

            int firmyCount = rows.Select(r => r.NazwaFirmy).Distinct(StringComparer.OrdinalIgnoreCase).Count();
            Log log = new Log("Systemy zewnętrzne cała enova", open: true);
            log.WriteLine("Oblicz Extra SQL: rekordy={0}, firmy={1}, czas={2} ms", rows.Count, firmyCount, stopwatch.ElapsedMilliseconds);
        }
        finally
        {
            if (ownsSession)
            {
                session.Dispose();
            }
        }
    }

    private List<SystemyZewnCalaEnovaRow> LoadRowsFromSql(Session session)
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
        command.CommandText = BuildSystemyZewnBatchSql(masterDatabaseName);

        List<SystemyZewnCalaEnovaRow> rows = new List<SystemyZewnCalaEnovaRow>();
        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new SystemyZewnCalaEnovaRow
            {
                NazwaFirmy = ReadString(reader, 0),
                ID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                Guid = ReadString(reader, 2),
                Typ = ReadString(reader, 3),
                Symbol = ReadString(reader, 4),
                Opis = ReadString(reader, 5),
                Blokada = reader.IsDBNull(6) ? null : reader.GetBoolean(6),
                Stamp = ReadString(reader, 7),
                Kontrahent = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                KontrahentType = ReadString(reader, 9),
                Domyslny = reader.IsDBNull(10) ? null : reader.GetBoolean(10)
            });
        }

        return rows;
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
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
        {
            DataSource = sqlDatabase.Server,
            InitialCatalog = sqlDatabase.DatabaseName,
            IntegratedSecurity = true,
            ApplicationName = "PNWB_Extra.SystemyZewnCalaEnova",
            TrustServerCertificate = true
        };
        return new SqlConnection(builder.ConnectionString);
    }

    private static string BuildSystemyZewnBatchSql(string masterDatabaseName)
    {
        string masterDbQuoted = QuoteSqlIdentifier(masterDatabaseName);
        return $@"
DECLARE @dbname NVARCHAR(128);
DECLARE @unionSql NVARCHAR(MAX) = N'';

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
    IF DB_ID(@dbname) IS NOT NULL
       AND OBJECT_ID(QUOTENAME(@dbname) + N'.dbo.SystemyZewn') IS NOT NULL
    BEGIN
        DECLARE @selectSql NVARCHAR(MAX) = N'
SELECT N''' + REPLACE(@dbname, '''', '''''') + N''' AS NazwaFirmy,
       TRY_CONVERT(int, S.ID) AS ID,
       CONVERT(nvarchar(36), S.Guid) AS [Guid],
       CONVERT(nvarchar(255), S.Typ) AS Typ,
       CONVERT(nvarchar(255), S.Symbol) AS Symbol,
       CONVERT(nvarchar(max), S.Opis) AS Opis,
       TRY_CONVERT(bit, S.Blokada) AS Blokada,
       CASE WHEN S.Stamp IS NULL THEN NULL ELSE sys.fn_varbintohexstr(CONVERT(varbinary(8), S.Stamp)) END AS Stamp,
       TRY_CONVERT(int, S.Kontrahent) AS Kontrahent,
       CONVERT(nvarchar(255), S.KontrahentType) AS KontrahentType,
       TRY_CONVERT(bit, S.Domyslny) AS Domyslny
FROM ' + QUOTENAME(@dbname) + N'.dbo.SystemyZewn AS S';

        SET @unionSql = CASE
            WHEN LEN(@unionSql) = 0 THEN @selectSql
            ELSE @unionSql + N' UNION ALL ' + @selectSql
        END;
    END;

    FETCH NEXT FROM db_cursor INTO @dbname;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

IF @unionSql = N''
BEGIN
    SELECT TOP (0)
        CAST(NULL AS nvarchar(128)) AS NazwaFirmy,
        CAST(NULL AS int) AS ID,
        CAST(NULL AS nvarchar(36)) AS [Guid],
        CAST(NULL AS nvarchar(255)) AS Typ,
        CAST(NULL AS nvarchar(255)) AS Symbol,
        CAST(NULL AS nvarchar(max)) AS Opis,
        CAST(NULL AS bit) AS Blokada,
        CAST(NULL AS nvarchar(100)) AS Stamp,
        CAST(NULL AS int) AS Kontrahent,
        CAST(NULL AS nvarchar(255)) AS KontrahentType,
        CAST(NULL AS bit) AS Domyslny;
END
ELSE
BEGIN
    EXEC sp_executesql @unionSql;
END";
    }

    private static string ReadString(SqlDataReader reader, int index)
    {
        return reader.IsDBNull(index) ? null : reader.GetString(index);
    }

    private static string QuoteSqlIdentifier(string value)
    {
        return $"[{value.Replace("]", "]]")}]";
    }
}
