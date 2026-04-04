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

[assembly: Worker(typeof(PNWB_Extra.UI.Workers.OptymalizacjaRozmiaruBazyMsSqlCalaEnovaObliczExtraWorker), typeof(PNWB_Extra.UI.Models.OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRoot))]

namespace PNWB_Extra.UI.Workers;

public sealed class OptymalizacjaRozmiaruBazyMsSqlCalaEnovaObliczExtraWorker
{
    [Context]
    public OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRoot Root { get; set; }

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

    [Action("Optymalizacja rozmiaru bazy Extra", Priority = 3, Icon = ActionIcon.Play, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.SingleSession | ActionMode.ReadOnlySession | ActionMode.Progress | ActionMode.OnlyWinForms))]
    public void OptimizeSelectedForms()
    {
        OptimizeSelectedCore();
        CalculateExtraCore();
        context.InvokeChanged();
    }

    [Action("Optymalizacja rozmiaru bazy Extra", Priority = 3, Icon = ActionIcon.Play, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.Progress | ActionMode.OnlyWebForms | ActionMode.NoSession | ActionMode.ReadOnlySession | ActionMode.SingleSession))]
    public void OptimizeSelectedWeb()
    {
        OptimizeSelectedCore();
        CalculateExtraCore();
        context.InvokeChanged();
    }

    private void CalculateExtraCore()
    {
        Session session = context.Session;
        bool ownsSession = false;
        if (session == null)
        {
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.OptymalizacjaRozmiaruBazyMsSqlCalaEnova.ObliczExtra");
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
            Dictionary<string, (decimal? DataSizeMb, decimal? LogSizeMb, string ErrorMessage)> sizes = LoadSizesFromSql(session, databaseNames);
            Root.ApplySizes(sizes);
            stopwatch.Stop();

            int withErrors = sizes.Count(kv => !string.IsNullOrWhiteSpace(kv.Value.ErrorMessage));
            Log log = new Log("Optymalizacja rozmiaru bazy MS SQL cała enova", open: true);
            log.WriteLine("Oblicz Extra SQL: rekordy={0}, firmy={1}, z błędami={2}, czas={3} ms",
                sizes.Count, databaseNames.Length, withErrors, stopwatch.ElapsedMilliseconds);
        }
        finally
        {
            if (ownsSession)
            {
                session.Dispose();
            }
        }
    }

    private void OptimizeSelectedCore()
    {
        Session session = context.Session;
        bool ownsSession = false;
        if (session == null)
        {
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.OptymalizacjaRozmiaruBazyMsSqlCalaEnova.Optymalizacja");
            ownsSession = true;
        }

        try
        {
            EnsureRootRowsLoaded(session);
            string[] selectedDatabaseNames = GetSelectedDatabaseNamesFromNavigator();
            if (selectedDatabaseNames.Length == 0)
            {
                throw new InvalidOperationException("Zaznacz co najmniej jedną bazę do optymalizacji rozmiaru.");
            }

            Log log = new Log("Optymalizacja rozmiaru bazy MS SQL cała enova", open: true);
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

                    using DatabaseHolder databaseHolder = dbItem.CreateDatabaseHolder(DBItemMode.Administration);
                    if (databaseHolder.GetDatabase() is not MsSqlDatabase msSqlDatabase)
                    {
                        skippedCount++;
                        log.WriteLine("Pominięto bazę '{0}' - baza nie jest typu MsSqlDatabase.", databaseName);
                        continue;
                    }

                    if (!IsOptimizationAllowed(msSqlDatabase, session.Login))
                    {
                        skippedCount++;
                        log.WriteLine("Pominięto bazę '{0}' - baza nie jest aktywna lub ma nieprawidłowy stan.", databaseName);
                        continue;
                    }

                    // Odtwarzamy dokładnie sekwencję z oryginalnej akcji enova:
                    // 1) przebudowa indeksów, 2) shrink bazy (bez tworzenia pliku backupu).
                    msSqlDatabase.RepairIndexes();
                    msSqlDatabase.Backup(new DatabaseBackupArguments
                    {
                        FileName = null,
                        Shrink = true
                    });
                    successCount++;
                }
                catch (Exception ex)
                {
                    skippedCount++;
                    log.WriteLine("Błąd optymalizacji bazy '{0}': {1}", databaseName, ex.Message);
                }
            }

            log.WriteLine("Optymalizacja rozmiaru bazy zbiorczo: zaznaczone={0}, uruchomione={1}, pominięte={2}",
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

        if (names.Count == 0 && context.Get<OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow>(out OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow focused) && focused != null)
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

        if (rowObject is OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow row)
        {
            return row.NazwaFirmy;
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

    private static bool IsOptimizationAllowed(Database database, Login login)
    {
        if (database == null || !database.Active || login == null)
        {
            return false;
        }

        if (database.State == DatabaseState.NoDatabase || database.State == DatabaseState.Fail)
        {
            return false;
        }

        return true;
    }

    private static Dictionary<string, (decimal? DataSizeMb, decimal? LogSizeMb, string ErrorMessage)> LoadSizesFromSql(Session session, IReadOnlyCollection<string> databaseNames)
    {
        Dictionary<string, (decimal? DataSizeMb, decimal? LogSizeMb, string ErrorMessage)> result =
            new Dictionary<string, (decimal? DataSizeMb, decimal? LogSizeMb, string ErrorMessage)>(StringComparer.OrdinalIgnoreCase);

        if (databaseNames == null || databaseNames.Count == 0)
        {
            return result;
        }

        if (session.Login.Database is not SqlDatabase sqlDatabase)
        {
            throw new InvalidOperationException("Bieżąca baza enova nie jest bazą SQL.");
        }

        using SqlConnection connection = CreateMasterSqlConnection(sqlDatabase);
        connection.Open();

        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = BuildSizesBatchSql(databaseNames);

        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string databaseName = reader.GetString(0);
            result[databaseName] = (
                DataSizeMb: ReadNullableDecimal(reader, 1),
                LogSizeMb: ReadNullableDecimal(reader, 2),
                ErrorMessage: ReadString(reader, 3));
        }

        return result;
    }

    private static string BuildSizesBatchSql(IEnumerable<string> databaseNames)
    {
        List<string> parts = new List<string>();
        foreach (string databaseName in databaseNames)
        {
            string dbLiteral = EscapeSqlLiteral(databaseName);
            parts.Add($@"
BEGIN TRY
    IF DB_ID(N'{dbLiteral}') IS NULL
    BEGIN
        INSERT INTO @results(DatabaseName, DataSizeMb, LogSizeMb, ErrorMessage)
        VALUES (N'{dbLiteral}', NULL, NULL, N'Brak bazy SQL o tej nazwie');
    END
    ELSE
    BEGIN
        INSERT INTO @results(DatabaseName, DataSizeMb, LogSizeMb, ErrorMessage)
        SELECT N'{dbLiteral}',
               CAST(SUM(CASE WHEN mf.type_desc = N'ROWS' THEN mf.size ELSE 0 END) * 8.0 / 1024 AS decimal(18,2)) AS DataSizeMb,
               CAST(SUM(CASE WHEN mf.type_desc = N'LOG' THEN mf.size ELSE 0 END) * 8.0 / 1024 AS decimal(18,2)) AS LogSizeMb,
               NULL
        FROM sys.master_files mf
        WHERE mf.database_id = DB_ID(N'{dbLiteral}');
    END
END TRY
BEGIN CATCH
    INSERT INTO @results(DatabaseName, DataSizeMb, LogSizeMb, ErrorMessage)
    VALUES (N'{dbLiteral}', NULL, NULL, ERROR_MESSAGE());
END CATCH");
        }

        return @$"
DECLARE @results TABLE (
    DatabaseName nvarchar(128) NOT NULL,
    DataSizeMb decimal(18,2) NULL,
    LogSizeMb decimal(18,2) NULL,
    ErrorMessage nvarchar(4000) NULL
);
{string.Join(Environment.NewLine, parts)}
SELECT DatabaseName, DataSizeMb, LogSizeMb, ErrorMessage
FROM @results;";
    }

    private static SqlConnection CreateMasterSqlConnection(SqlDatabase sqlDatabase)
    {
        return PnwbSqlConnectionFactory.Create(sqlDatabase, "master", "PNWB_Extra.OptymalizacjaRozmiaruBazyMsSqlCalaEnova");
    }

    private static decimal? ReadNullableDecimal(SqlDataReader reader, int index)
    {
        if (reader.IsDBNull(index))
        {
            return null;
        }

        object value = reader.GetValue(index);
        return Convert.ToDecimal(value);
    }

    private static string ReadString(SqlDataReader reader, int index)
    {
        return reader.IsDBNull(index) ? null : reader.GetString(index);
    }

    private static string EscapeSqlLiteral(string value)
    {
        return (value ?? string.Empty).Replace("'", "''");
    }
}
