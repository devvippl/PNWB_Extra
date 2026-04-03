using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Microsoft.Data.SqlClient;
using PNWB_Extra.UI.Models;
using Soneta.Business;
using Soneta.Business.App;
using Soneta.Business.Db;
using Soneta.Commands;

[assembly: Worker(typeof(PNWB_Extra.UI.Workers.UnicodeCalaEnovaObliczExtraWorker), typeof(PNWB_Extra.UI.Models.UnicodeCalaEnovaRoot))]

namespace PNWB_Extra.UI.Workers;

public sealed class UnicodeCalaEnovaObliczExtraWorker
{
    [Context]
    public UnicodeCalaEnovaRoot Root { get; set; }

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

    [Action("Konwersja bazy do Unicode", Priority = 3, Icon = ActionIcon.Play, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.SingleSession | ActionMode.ReadOnlySession | ActionMode.Progress | ActionMode.OnlyWinForms))]
    public void ConvertSelectedForms()
    {
        ConvertSelectedCore();
        CalculateExtraCore();
        context.InvokeChanged();
    }

    [Action("Konwersja bazy do Unicode", Priority = 3, Icon = ActionIcon.Play, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.Progress | ActionMode.OnlyWebForms | ActionMode.NoSession | ActionMode.ReadOnlySession | ActionMode.SingleSession))]
    public void ConvertSelectedWeb()
    {
        ConvertSelectedCore();
        CalculateExtraCore();
        context.InvokeChanged();
    }

    private void CalculateExtraCore()
    {
        Session session = context.Session;
        bool ownsSession = false;
        if (session == null)
        {
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.UnicodeCalaEnova.ObliczExtra");
            ownsSession = true;
        }

        try
        {
            EnsureRootRowsLoaded(session);
            string[] selectedDatabaseNames = GetSelectedDatabaseNamesFromNavigator();
            string[] databaseNames = selectedDatabaseNames.Length > 0
                ? selectedDatabaseNames
                : Root.GetAllRows().Select(r => r.NazwaFirmy).ToArray();

            if (databaseNames.Length == 0)
            {
                return;
            }

            Stopwatch stopwatch = Stopwatch.StartNew();
            Dictionary<string, (string DataType, bool? IsUnicode, string ErrorMessage)> statuses = LoadStatusesFromSql(session, databaseNames);
            Root.ApplyStatuses(statuses);
            stopwatch.Stop();

            int unicodeCount = statuses.Count(kv => kv.Value.IsUnicode == true);
            int nonUnicodeCount = statuses.Count(kv => kv.Value.IsUnicode == false);
            int unknownCount = statuses.Count(kv => kv.Value.IsUnicode == null);

            Log log = new Log("Unicode cała enova", open: true);
            log.WriteLine("Oblicz Extra SQL: rekordy={0}, firmy={1}, unicode={2}, nonunicode={3}, nierozpoznane={4}, czas={5} ms",
                statuses.Count, databaseNames.Length, unicodeCount, nonUnicodeCount, unknownCount, stopwatch.ElapsedMilliseconds);
        }
        finally
        {
            if (ownsSession)
            {
                session.Dispose();
            }
        }
    }

    private void ConvertSelectedCore()
    {
        Session session = context.Session;
        bool ownsSession = false;
        if (session == null)
        {
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.UnicodeCalaEnova.Konwersja");
            ownsSession = true;
        }

        try
        {
            EnsureRootRowsLoaded(session);
            string[] selectedDatabaseNames = GetSelectedDatabaseNamesFromNavigator();
            if (selectedDatabaseNames.Length == 0)
            {
                throw new InvalidOperationException("Zaznacz co najmniej jedną bazę do konwersji Unicode.");
            }

            if (session.Login.Database is not SqlDatabase currentSqlDatabase)
            {
                throw new InvalidOperationException("Bieżąca baza enova nie jest bazą SQL.");
            }

            Log log = new Log("Unicode cała enova", open: true);
            int successCount = 0;
            int skippedCount = 0;

            foreach (string databaseName in selectedDatabaseNames)
            {
                try
                {
                    DBItem dbItem = FindDbItem(session, databaseName);
                    if (dbItem == null)
                    {
                        skippedCount++;
                        log.WriteLine("Pominięto bazę '{0}' - brak wpisu w DBItems.", databaseName);
                        continue;
                    }

                    PrepareDatabaseForUnicodeConversion(currentSqlDatabase, dbItem.Name, log);

                    using DatabaseHolder databaseHolder = dbItem.CreateDatabaseHolder(DBItemMode.Administration);
                    if (databaseHolder.GetDatabase() is not MsSqlDatabase msSqlDatabase)
                    {
                        skippedCount++;
                        log.WriteLine("Pominięto bazę '{0}' - baza nie jest typu MsSqlDatabase.", databaseName);
                        continue;
                    }

                    if (!MsSqlDatabaseUnicodeWorker.IsVisibleSwitchToUnicode(session.Login, msSqlDatabase))
                    {
                        skippedCount++;
                        log.WriteLine("Pominięto bazę '{0}' - brak uprawnień lub baza nie jest w stanie OK.", databaseName);
                        continue;
                    }

                    MsSqlDatabaseUnicodeWorker worker = new MsSqlDatabaseUnicodeWorker
                    {
                        Database = msSqlDatabase
                    };
                    worker.SwitchToUnicode(session.Login);
                    successCount++;
                }
                catch (Exception ex)
                {
                    skippedCount++;
                    log.WriteLine("Błąd konwersji bazy '{0}': {1}", databaseName, ex.Message);
                }
            }

            log.WriteLine("Konwersja Unicode zbiorczo: zaznaczone={0}, uruchomione={1}, pominięte={2}",
                selectedDatabaseNames.Length, successCount, skippedCount);
        }
        finally
        {
            if (ownsSession)
            {
                session.Dispose();
            }
        }
    }

    private void EnsureRootRowsLoaded(Session session)
    {
        string[] databaseNames = LoadDatabaseNames(session);
        Root.EnsureRowsLoaded(databaseNames);
    }

    private static string[] LoadDatabaseNames(Session session)
    {
        BusinessModule module = BusinessModule.GetInstance(session);
        return module.DBItems
            .Cast<DBItem>()
            .Select(i => i?.Name)
            .Where(n => !string.IsNullOrWhiteSpace(n) && n != ".")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private string[] GetSelectedDatabaseNamesFromNavigator()
    {
        List<string> names = new List<string>();

        if (context.Get<INavigatorContext>(out INavigatorContext navigator) && navigator != null)
        {
            if (navigator.SelectedRows != null)
            {
                foreach (object selectedRow in navigator.SelectedRows)
                {
                    string name = TryGetDatabaseName(selectedRow);
                    if (!string.IsNullOrWhiteSpace(name))
                    {
                        names.Add(name);
                    }
                }
            }

            if (names.Count == 0)
            {
                string focusedName = TryGetDatabaseName(navigator.FocusedRow);
                if (!string.IsNullOrWhiteSpace(focusedName))
                {
                    names.Add(focusedName);
                }
            }
        }

        if (names.Count == 0 && context.Get<UnicodeCalaEnovaRow>(out UnicodeCalaEnovaRow focused) && focused != null)
        {
            string focusedName = TryGetDatabaseName(focused);
            if (!string.IsNullOrWhiteSpace(focusedName))
            {
                names.Add(focusedName);
            }
        }

        return names
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string TryGetDatabaseName(object rowObject)
    {
        if (rowObject == null)
        {
            return null;
        }

        if (rowObject is UnicodeCalaEnovaRow unicodeRow)
        {
            return unicodeRow.NazwaFirmy;
        }

        if (rowObject is DBItem dbItem)
        {
            return dbItem.Name;
        }

        Type type = rowObject.GetType();
        PropertyInfo nazwaFirmyProperty = type.GetProperty("NazwaFirmy");
        if (nazwaFirmyProperty?.GetValue(rowObject) is string nazwaFirmy && !string.IsNullOrWhiteSpace(nazwaFirmy))
        {
            return nazwaFirmy;
        }

        PropertyInfo nameProperty = type.GetProperty("Name");
        if (nameProperty?.GetValue(rowObject) is string name && !string.IsNullOrWhiteSpace(name))
        {
            return name;
        }

        PropertyInfo rowProperty = type.GetProperty("Row");
        if (rowProperty != null)
        {
            object innerRow = rowProperty.GetValue(rowObject);
            if (!ReferenceEquals(innerRow, rowObject))
            {
                return TryGetDatabaseName(innerRow);
            }
        }

        return null;
    }

    private static DBItem FindDbItem(Session session, string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return null;
        }

        BusinessModule module = BusinessModule.GetInstance(session);
        try
        {
            DBItem fromKey = module.DBItems.ByName[databaseName];
            if (fromKey != null)
            {
                return fromKey;
            }
        }
        catch
        {
        }

        return module.DBItems.Cast<DBItem>().FirstOrDefault(i => string.Equals(i.Name, databaseName, StringComparison.OrdinalIgnoreCase));
    }

    private static Dictionary<string, (string DataType, bool? IsUnicode, string ErrorMessage)> LoadStatusesFromSql(Session session, IReadOnlyCollection<string> databaseNames)
    {
        Dictionary<string, (string DataType, bool? IsUnicode, string ErrorMessage)> result =
            new Dictionary<string, (string DataType, bool? IsUnicode, string ErrorMessage)>(StringComparer.OrdinalIgnoreCase);

        if (databaseNames == null || databaseNames.Count == 0)
        {
            return result;
        }

        if (session.Login.Database is not SqlDatabase sqlDatabase)
        {
            throw new InvalidOperationException("Bieżąca baza enova nie jest bazą SQL.");
        }

        using SqlConnection connection = CreateSqlConnection(sqlDatabase);
        connection.Open();

        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = BuildUnicodeStatusesBatchSql(databaseNames);

        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string databaseName = reader.GetString(0);
            result[databaseName] = (
                DataType: ReadString(reader, 1),
                IsUnicode: reader.IsDBNull(2) ? null : reader.GetBoolean(2),
                ErrorMessage: ReadString(reader, 3));
        }

        return result;
    }

    private static string BuildUnicodeStatusesBatchSql(IEnumerable<string> databaseNames)
    {
        List<string> parts = new List<string>();
        foreach (string databaseName in databaseNames)
        {
            string dbLiteral = EscapeSqlLiteral(databaseName);
            string dbQuoted = QuoteSqlIdentifier(databaseName);
            parts.Add($@"
BEGIN TRY
    IF DB_ID(N'{dbLiteral}') IS NULL
    BEGIN
        INSERT INTO @results(DatabaseName, DataType, IsUnicode, ErrorMessage)
        VALUES (N'{dbLiteral}', NULL, NULL, N'Brak bazy SQL o tej nazwie');
    END
    ELSE IF OBJECT_ID(N'{dbQuoted}.dbo.SystemInfos') IS NULL
    BEGIN
        INSERT INTO @results(DatabaseName, DataType, IsUnicode, ErrorMessage)
        VALUES (N'{dbLiteral}', NULL, NULL, N'Brak tabeli dbo.SystemInfos');
    END
    ELSE
    BEGIN
        INSERT INTO @results(DatabaseName, DataType, IsUnicode, ErrorMessage)
        SELECT N'{dbLiteral}',
               X.DATA_TYPE,
               CASE
                    WHEN X.DATA_TYPE = N'nvarchar' THEN CAST(1 AS bit)
                    WHEN X.DATA_TYPE = N'varchar' THEN CAST(0 AS bit)
                    ELSE CAST(NULL AS bit)
               END AS IsUnicode,
               CASE
                    WHEN X.DATA_TYPE IS NULL THEN N'Brak kolumny dbo.SystemInfos.Value'
                    ELSE NULL
               END AS ErrorMessage
        FROM (
            SELECT TOP (1) C.DATA_TYPE
            FROM {dbQuoted}.INFORMATION_SCHEMA.COLUMNS C
            WHERE C.TABLE_SCHEMA = N'dbo'
              AND C.TABLE_NAME = N'SystemInfos'
              AND C.COLUMN_NAME = N'Value'
        ) X
        RIGHT JOIN (SELECT 1 AS Dummy) D ON 1 = 1;
    END
END TRY
BEGIN CATCH
    INSERT INTO @results(DatabaseName, DataType, IsUnicode, ErrorMessage)
    VALUES (N'{dbLiteral}', NULL, NULL, ERROR_MESSAGE());
END CATCH");
        }

        return @$"
DECLARE @results TABLE (
    DatabaseName nvarchar(128) NOT NULL,
    DataType nvarchar(50) NULL,
    IsUnicode bit NULL,
    ErrorMessage nvarchar(4000) NULL
);
{string.Join(Environment.NewLine, parts)}
SELECT DatabaseName, DataType, IsUnicode, ErrorMessage
FROM @results;";
    }

    private static void PrepareDatabaseForUnicodeConversion(SqlDatabase sqlDatabase, string databaseName, Log log)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return;
        }

        using SqlConnection connection = CreateMasterSqlConnection(sqlDatabase);
        connection.Open();

        int currentSessionId = GetCurrentSessionId(connection);
        List<DatabaseSessionInfo> sessions = GetUserSessionsForDatabase(connection, databaseName, currentSessionId);
        if (sessions.Count > 0)
        {
            string sessionSummary = string.Join(", ", sessions.Select(s => $"{s.SessionId}:{s.LoginName}@{s.HostName}/{s.ProgramName}"));
            log.WriteLine("Baza '{0}': znaleziono sesje do ubicia ({1}): {2}", databaseName, sessions.Count, sessionSummary);
        }

        foreach (DatabaseSessionInfo sessionInfo in sessions)
        {
            using SqlCommand kill = connection.CreateCommand();
            kill.CommandType = CommandType.Text;
            kill.CommandText = $"KILL {sessionInfo.SessionId};";
            kill.ExecuteNonQuery();
        }

        using SqlCommand setMultiUser = connection.CreateCommand();
        setMultiUser.CommandType = CommandType.Text;
        setMultiUser.CommandText = $"ALTER DATABASE {QuoteSqlIdentifier(databaseName)} SET MULTI_USER WITH ROLLBACK IMMEDIATE;";
        setMultiUser.ExecuteNonQuery();
    }

    private static int GetCurrentSessionId(SqlConnection connection)
    {
        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = "SELECT @@SPID;";
        object value = command.ExecuteScalar();
        if (value == null || value == DBNull.Value)
        {
            throw new InvalidOperationException("Nie udało się odczytać @@SPID.");
        }

        return Convert.ToInt32(value);
    }

    private static List<DatabaseSessionInfo> GetUserSessionsForDatabase(SqlConnection connection, string databaseName, int currentSessionId)
    {
        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = @"
SELECT
    s.session_id,
    s.login_name,
    s.host_name,
    s.program_name
FROM sys.dm_exec_sessions s
WHERE s.is_user_process = 1
  AND DB_NAME(s.database_id) = @DatabaseName
  AND s.session_id <> @CurrentSessionId
ORDER BY s.session_id;";
        command.Parameters.Add(new SqlParameter("@DatabaseName", SqlDbType.NVarChar, 128) { Value = databaseName });
        command.Parameters.Add(new SqlParameter("@CurrentSessionId", SqlDbType.Int) { Value = currentSessionId });

        List<DatabaseSessionInfo> result = new List<DatabaseSessionInfo>();
        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            result.Add(new DatabaseSessionInfo
            {
                SessionId = reader.GetInt16(0),
                LoginName = ReadString(reader, 1),
                HostName = ReadString(reader, 2),
                ProgramName = ReadString(reader, 3)
            });
        }

        return result;
    }

    private static SqlConnection CreateSqlConnection(SqlDatabase sqlDatabase)
    {
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
        {
            DataSource = sqlDatabase.Server,
            InitialCatalog = sqlDatabase.DatabaseName,
            IntegratedSecurity = true,
            ApplicationName = "PNWB_Extra.UnicodeCalaEnova",
            TrustServerCertificate = true
        };
        return new SqlConnection(builder.ConnectionString);
    }

    private static SqlConnection CreateMasterSqlConnection(SqlDatabase sqlDatabase)
    {
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
        {
            DataSource = sqlDatabase.Server,
            InitialCatalog = "master",
            IntegratedSecurity = true,
            ApplicationName = "PNWB_Extra.UnicodeCalaEnova.Prepare",
            TrustServerCertificate = true
        };
        return new SqlConnection(builder.ConnectionString);
    }

    private static string ReadString(SqlDataReader reader, int index)
    {
        return reader.IsDBNull(index) ? null : reader.GetString(index);
    }

    private static string EscapeSqlLiteral(string value)
    {
        return (value ?? string.Empty).Replace("'", "''");
    }

    private static string QuoteSqlIdentifier(string value)
    {
        return $"[{(value ?? string.Empty).Replace("]", "]]")}]";
    }

    private sealed class DatabaseSessionInfo
    {
        public int SessionId { get; set; }

        public string LoginName { get; set; }

        public string HostName { get; set; }

        public string ProgramName { get; set; }
    }
}
