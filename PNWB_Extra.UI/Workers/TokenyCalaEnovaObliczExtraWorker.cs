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

[assembly: Worker(typeof(PNWB_Extra.UI.Workers.TokenyCalaEnovaObliczExtraWorker), typeof(PNWB_Extra.UI.Models.TokenyCalaEnovaRoot))]

namespace PNWB_Extra.UI.Workers;

public sealed class TokenyCalaEnovaObliczExtraWorker
{
    [Context]
    public TokenyCalaEnovaRoot Root { get; set; }

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
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.TokenyCalaEnova.ObliczExtra");
            ownsSession = true;
        }

        try
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            List<TokenCalaEnovaRow> rows = LoadRowsFromSql(session);

            Root.ReplaceRows(rows);
            stopwatch.Stop();

            int firmyCount = rows.Select(r => r.NazwaFirmy).Distinct(StringComparer.OrdinalIgnoreCase).Count();
            Log log = new Log("Tokeny cała enova", open: true);
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

    private List<TokenCalaEnovaRow> LoadRowsFromSql(Session session)
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
        command.CommandText = BuildTokenyBatchSql(masterDatabaseName);

        List<TokenCalaEnovaRow> rows = new List<TokenCalaEnovaRow>();
        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new TokenCalaEnovaRow
            {
                NazwaFirmy = ReadString(reader, 0),
                ID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                Guid = ReadString(reader, 2),
                SystemZewn = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                Nazwa = ReadString(reader, 4),
                Token = ReadString(reader, 5),
                Pobieranie = reader.IsDBNull(6) ? null : reader.GetBoolean(6),
                Wysylanie = reader.IsDBNull(7) ? null : reader.GetBoolean(7),
                Stamp = ReadString(reader, 8),
                OperatorzyGuids = ReadString(reader, 9),
                RefreshTokenValidUntil = ReadString(reader, 10),
                RefreshTokenValue = ReadString(reader, 11),
                RefreshTokenRequestRefNumber = ReadString(reader, 12),
                RefreshTokenRequestTempTokenValue = ReadString(reader, 13),
                WersjaAPI = ReadString(reader, 14),
                Rodzaj = reader.IsDBNull(15) ? null : reader.GetInt32(15),
                Certyfikat = ReadString(reader, 16),
                CertyfikatKey = ReadString(reader, 17),
                CertyfikatHasloValue = ReadString(reader, 18),
                Przeznaczenie = reader.IsDBNull(19) ? null : reader.GetInt32(19)
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
        return PnwbSqlConnectionFactory.Create(sqlDatabase, "PNWB_Extra.TokenyCalaEnova");
    }

    private static string BuildTokenyBatchSql(string masterDatabaseName)
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
       AND OBJECT_ID(QUOTENAME(@dbname) + N'.dbo.SysZewTokeny') IS NOT NULL
    BEGIN
        DECLARE @selectSql NVARCHAR(MAX) = N'
SELECT N''' + REPLACE(@dbname, '''', '''''') + N''' AS NazwaFirmy,
       TRY_CONVERT(int, T.ID) AS ID,
       CONVERT(nvarchar(36), T.Guid) AS [Guid],
       TRY_CONVERT(int, T.SystemZewn) AS SystemZewn,
       CONVERT(nvarchar(255), T.Nazwa) AS Nazwa,
       CONVERT(nvarchar(max), T.Token) AS Token,
       TRY_CONVERT(bit, T.Pobieranie) AS Pobieranie,
       TRY_CONVERT(bit, T.Wysylanie) AS Wysylanie,
       CASE WHEN T.Stamp IS NULL THEN NULL ELSE sys.fn_varbintohexstr(CONVERT(varbinary(8), T.Stamp)) END AS Stamp,
       CONVERT(nvarchar(max), T.OperatorzyGuids) AS OperatorzyGuids,
       CONVERT(nvarchar(50), TRY_CONVERT(datetime2, T.RefreshTokenValidUntil), 121) AS RefreshTokenValidUntil,
       CASE WHEN T.RefreshTokenValue IS NULL THEN NULL ELSE sys.fn_varbintohexstr(CONVERT(varbinary(max), T.RefreshTokenValue)) END AS RefreshTokenValue,
       CONVERT(nvarchar(255), T.RefreshTokenRequestRefNumber) AS RefreshTokenRequestRefNumber,
       CASE WHEN T.RefreshTokenRequestTempTokenValue IS NULL THEN NULL ELSE sys.fn_varbintohexstr(CONVERT(varbinary(max), T.RefreshTokenRequestTempTokenValue)) END AS RefreshTokenRequestTempTokenValue,
       CONVERT(nvarchar(64), T.WersjaAPI) AS WersjaAPI,
       TRY_CONVERT(int, T.Rodzaj) AS Rodzaj,
       CASE WHEN T.Certyfikat IS NULL THEN NULL ELSE sys.fn_varbintohexstr(CONVERT(varbinary(max), T.Certyfikat)) END AS Certyfikat,
       CASE WHEN T.CertyfikatKey IS NULL THEN NULL ELSE sys.fn_varbintohexstr(CONVERT(varbinary(max), T.CertyfikatKey)) END AS CertyfikatKey,
       CASE WHEN T.CertyfikatHasloValue IS NULL THEN NULL ELSE sys.fn_varbintohexstr(CONVERT(varbinary(max), T.CertyfikatHasloValue)) END AS CertyfikatHasloValue,
       TRY_CONVERT(int, T.Przeznaczenie) AS Przeznaczenie
FROM ' + QUOTENAME(@dbname) + N'.dbo.SysZewTokeny AS T';

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
        CAST(NULL AS int) AS SystemZewn,
        CAST(NULL AS nvarchar(255)) AS Nazwa,
        CAST(NULL AS nvarchar(max)) AS Token,
        CAST(NULL AS bit) AS Pobieranie,
        CAST(NULL AS bit) AS Wysylanie,
        CAST(NULL AS nvarchar(100)) AS Stamp,
        CAST(NULL AS nvarchar(max)) AS OperatorzyGuids,
        CAST(NULL AS nvarchar(50)) AS RefreshTokenValidUntil,
        CAST(NULL AS nvarchar(max)) AS RefreshTokenValue,
        CAST(NULL AS nvarchar(255)) AS RefreshTokenRequestRefNumber,
        CAST(NULL AS nvarchar(max)) AS RefreshTokenRequestTempTokenValue,
        CAST(NULL AS nvarchar(64)) AS WersjaAPI,
        CAST(NULL AS int) AS Rodzaj,
        CAST(NULL AS nvarchar(max)) AS Certyfikat,
        CAST(NULL AS nvarchar(max)) AS CertyfikatKey,
        CAST(NULL AS nvarchar(max)) AS CertyfikatHasloValue,
        CAST(NULL AS int) AS Przeznaczenie;
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
