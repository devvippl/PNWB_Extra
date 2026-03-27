using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using PNWB_Extra.UI.ViewInfos;
using Microsoft.Data.SqlClient;
using Soneta.Business;
using Soneta.Business.App;
using Soneta.Business.Db;
using Soneta.Commands;
using Soneta.Types;

[assembly: Worker(typeof(PNWB_Extra.UI.Workers.DBItemsObliczExtraWorker), typeof(DBItems))]

namespace PNWB_Extra.UI.Workers;

public sealed class DBItemsObliczExtraWorker
{
    private sealed class DbCalculatedStatuses
    {
        public int? StatusJpkInt { get; init; }

        public bool? DeklaracjaVatueBufor { get; init; }

        public int? EDeklaracjaVatueInt { get; init; }

        public bool? DeklaracjaPit8ArBufor { get; init; }

        public int? EDeklaracjaPit8ArInt { get; init; }

        public bool? DeklaracjaPit4RBufor { get; init; }

        public int? EDeklaracjaPit4RInt { get; init; }

        public bool? DeklaracjaCit8Bufor { get; init; }

        public int? EDeklaracjaCit8Int { get; init; }
    }

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
    }

    [Action("Oblicz Extra", Priority = 2, Icon = ActionIcon.Play, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.Progress | ActionMode.OnlyWebForms | ActionMode.NoSession))]
    public void CalculateExtraWeb()
    {
        CalculateExtraCore();
        context.Session.InvokeChanged();
    }

    public static bool IsVisibleObliczExtra(Context cx)
    {
        return cx.Contains(typeof(AnalizyBazDanychExtraContextMarker));
    }

    public static void InitializeCalculatedPlaceholders(DBItem item)
    {
        SetCalculatedValues(item, new Dictionary<string, string>(StringComparer.Ordinal), Date.Empty, FromTo.Empty, updateCalculationMetadata: false);
    }

    private void CalculateExtraCore()
    {
        Stopwatch totalStopwatch = Stopwatch.StartNew();
        OkresContext okresContext = (OkresContext)context[typeof(OkresContext)];
        DBItem[] items = GetItems();

        Stopwatch sqlStopwatch = Stopwatch.StartNew();
        Dictionary<string, DbCalculatedStatuses> statusesByDatabase = LoadCalculatedStatusesFromSql(items, okresContext.Okres);
        sqlStopwatch.Stop();

        int statusJpkCount = 0;
        int deklaracjaVatueCount = 0;
        int eDeklaracjaVatueCount = 0;
        int deklaracjaPit8ArCount = 0;
        int eDeklaracjaPit8ArCount = 0;
        int deklaracjaPit4RCount = 0;
        int eDeklaracjaPit4RCount = 0;
        int deklaracjaCit8Count = 0;
        int eDeklaracjaCit8Count = 0;

        Stopwatch uiStopwatch = Stopwatch.StartNew();

        foreach (DBItem item in items)
        {
            DbCalculatedStatuses row = statusesByDatabase.TryGetValue(item.Name, out DbCalculatedStatuses value)
                ? value
                : new DbCalculatedStatuses();

            string statusJpk = ToStatusCaption(row.StatusJpkInt);
            string deklaracjaVatue = ToDeklaracjaStatus(row.DeklaracjaVatueBufor);
            string eDeklaracjaVatue = ToEDeklaracjaStatusCaption(row.EDeklaracjaVatueInt);
            string deklaracjaPit8Ar = ToDeklaracjaStatus(row.DeklaracjaPit8ArBufor);
            string eDeklaracjaPit8Ar = ToEDeklaracjaStatusCaption(row.EDeklaracjaPit8ArInt);
            string deklaracjaPit4R = ToDeklaracjaStatus(row.DeklaracjaPit4RBufor);
            string eDeklaracjaPit4R = ToEDeklaracjaStatusCaption(row.EDeklaracjaPit4RInt);
            string deklaracjaCit8 = ToDeklaracjaStatus(row.DeklaracjaCit8Bufor);
            string eDeklaracjaCit8 = ToEDeklaracjaStatusCaption(row.EDeklaracjaCit8Int);

            if (statusJpk != "Brak") statusJpkCount++;
            if (deklaracjaVatue != "Brak") deklaracjaVatueCount++;
            if (eDeklaracjaVatue != "Brak") eDeklaracjaVatueCount++;
            if (deklaracjaPit8Ar != "Brak") deklaracjaPit8ArCount++;
            if (eDeklaracjaPit8Ar != "Brak") eDeklaracjaPit8ArCount++;
            if (deklaracjaPit4R != "Brak") deklaracjaPit4RCount++;
            if (eDeklaracjaPit4R != "Brak") eDeklaracjaPit4RCount++;
            if (deklaracjaCit8 != "Brak") deklaracjaCit8Count++;
            if (eDeklaracjaCit8 != "Brak") eDeklaracjaCit8Count++;

            Dictionary<string, string> valuesToSet = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["StatusJPK"] = statusJpk,
                ["DeklaracjaVATUE"] = deklaracjaVatue,
                ["EDeklaracjaVATUE"] = eDeklaracjaVatue,
                ["DeklaracjaPIT8AR"] = deklaracjaPit8Ar,
                ["EDeklaracjaPIT8AR"] = eDeklaracjaPit8Ar,
                ["DeklaracjaPIT4R"] = deklaracjaPit4R,
                ["EDeklaracjaPIT4R"] = eDeklaracjaPit4R,
                ["DeklaracjaCIT8"] = deklaracjaCit8,
                ["EDeklaracjaCIT8"] = eDeklaracjaCit8
            };

            using ITransaction transaction = context.Session.Logout(editMode: true);
            SetCalculatedValues(item, valuesToSet, okresContext.Aktualny, okresContext.Okres);
            transaction.CommitUI();
        }

        uiStopwatch.Stop();
        totalStopwatch.Stop();

        Log log = new Log("Analizy baz danych Extra SQL", open: true);
        log.WriteLine("Oblicz Extra SQL: bazy={0}, SQL={1} ms, UI={2} ms, razem={3} ms",
            items.Length, sqlStopwatch.ElapsedMilliseconds, uiStopwatch.ElapsedMilliseconds, totalStopwatch.ElapsedMilliseconds);
        log.WriteLine("Wyniki != Brak: StatusJPK={0}, DeklaracjaVATUE={1}, EDeklaracjaVATUE={2}, DeklaracjaPIT8AR={3}, EDeklaracjaPIT8AR={4}, DeklaracjaPIT4R={5}, EDeklaracjaPIT4R={6}, DeklaracjaCIT8={7}, EDeklaracjaCIT8={8}",
            statusJpkCount, deklaracjaVatueCount, eDeklaracjaVatueCount, deklaracjaPit8ArCount, eDeklaracjaPit8ArCount, deklaracjaPit4RCount, eDeklaracjaPit4RCount, deklaracjaCit8Count, eDeklaracjaCit8Count);
    }

    private DBItem[] GetItems()
    {
        if (context.Get<DBItem[]>(out DBItem[] selected))
        {
            return selected;
        }

        return ((View)context[typeof(View)]).Cast<DBItem>().ToArray();
    }

    private Dictionary<string, DbCalculatedStatuses> LoadCalculatedStatusesFromSql(DBItem[] items, FromTo okres)
    {
        Dictionary<string, DbCalculatedStatuses> result = new Dictionary<string, DbCalculatedStatuses>(StringComparer.OrdinalIgnoreCase);
        string[] databaseNames = items.Select(i => i.Name).Where(n => !string.IsNullOrWhiteSpace(n)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        foreach (string databaseName in databaseNames)
        {
            result[databaseName] = new DbCalculatedStatuses();
        }

        if (databaseNames.Length == 0)
        {
            return result;
        }

        if (context.Session.Login.Database is not SqlDatabase sqlDatabase)
        {
            throw new InvalidOperationException("Bieżąca baza enova nie jest bazą SQL.");
        }

        using SqlConnection connection = CreateSqlConnection(sqlDatabase);
        connection.Open();

        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = BuildCalculatedStatusesBatchSql(databaseNames);
        command.Parameters.Add(new SqlParameter("@OkresFrom", SqlDbType.Date)
        {
            Value = (DateTime)okres.From
        });
        command.Parameters.Add(new SqlParameter("@OkresTo", SqlDbType.Date)
        {
            Value = (DateTime)okres.To
        });
        command.Parameters.Add(new SqlParameter("@TypVATUE", SqlDbType.Int) { Value = GetTypDeklaracjiValue("VATUE") });
        command.Parameters.Add(new SqlParameter("@TypPIT8A", SqlDbType.Int) { Value = GetTypDeklaracjiValue("PIT8A") });
        command.Parameters.Add(new SqlParameter("@TypPIT4", SqlDbType.Int) { Value = GetTypDeklaracjiValue("PIT4") });
        command.Parameters.Add(new SqlParameter("@TypCIT8", SqlDbType.Int) { Value = GetTypDeklaracjiValue("CIT8") });

        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string databaseName = reader.GetString(0);
            result[databaseName] = new DbCalculatedStatuses
            {
                StatusJpkInt = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                DeklaracjaVatueBufor = reader.IsDBNull(2) ? null : reader.GetBoolean(2),
                EDeklaracjaVatueInt = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                DeklaracjaPit8ArBufor = reader.IsDBNull(4) ? null : reader.GetBoolean(4),
                EDeklaracjaPit8ArInt = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                DeklaracjaPit4RBufor = reader.IsDBNull(6) ? null : reader.GetBoolean(6),
                EDeklaracjaPit4RInt = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                DeklaracjaCit8Bufor = reader.IsDBNull(8) ? null : reader.GetBoolean(8),
                EDeklaracjaCit8Int = reader.IsDBNull(9) ? null : reader.GetInt32(9)
            };
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
            ApplicationName = "PNWB_Extra.ObliczExtraSQL",
            TrustServerCertificate = true
        };
        return new SqlConnection(builder.ConnectionString);
    }

    private static string BuildCalculatedStatusesBatchSql(IEnumerable<string> databaseNames)
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
        INSERT INTO @results(DatabaseName, StatusJPKInt, DeklaracjaVATUEBufor, EDeklaracjaVATUEInt, DeklaracjaPIT8ARBufor, EDeklaracjaPIT8ARInt, DeklaracjaPIT4RBufor, EDeklaracjaPIT4RInt, DeklaracjaCIT8Bufor, EDeklaracjaCIT8Int)
        VALUES (N'{dbLiteral}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    END
    ELSE
    BEGIN
        INSERT INTO @results(DatabaseName, StatusJPKInt, DeklaracjaVATUEBufor, EDeklaracjaVATUEInt, DeklaracjaPIT8ARBufor, EDeklaracjaPIT8ARInt, DeklaracjaPIT4RBufor, EDeklaracjaPIT4RInt, DeklaracjaCIT8Bufor, EDeklaracjaCIT8Int)
        SELECT N'{dbLiteral}',
               (
                   SELECT TOP (1) j.StatusJPK
                   FROM {dbQuoted}.dbo.JednolitePK j
                   INNER JOIN {dbQuoted}.dbo.Slowniki s ON s.ID = j.Rodzaj
                   WHERE RTRIM(s.Kategoria) = N'RodzajJPK'
                     AND RTRIM(s.Nazwa) = N'JPK_VAT'
                   AND j.OkresTo = @OkresTo
                   ORDER BY j.ID DESC
               ),
               (
                   SELECT TOP (1) CAST(d.Bufor AS bit)
                   FROM {dbQuoted}.dbo.Deklaracje d
                   WHERE d.Typ = @TypVATUE
                     AND d.Zrodlo IS NULL
                     AND d.OkresTo >= @OkresFrom
                     AND d.OkresTo <= @OkresTo
                   ORDER BY d.OkresTo DESC, d.ID DESC
               ),
               (
                   SELECT TOP (1) e.StatusEDeklaracji
                   FROM {dbQuoted}.dbo.EDeklaracje e
                   WHERE e.TypDeklaracji = @TypVATUE
                     AND e.OkresDeklaracjiTo >= @OkresFrom
                     AND e.OkresDeklaracjiTo <= @OkresTo
                   ORDER BY e.OkresDeklaracjiTo DESC, e.ID DESC
               ),
               (
                   SELECT TOP (1) CAST(d.Bufor AS bit)
                   FROM {dbQuoted}.dbo.Deklaracje d
                   WHERE d.Typ = @TypPIT8A
                     AND d.Zrodlo IS NULL
                     AND d.OkresTo >= @OkresFrom
                     AND d.OkresTo <= @OkresTo
                   ORDER BY d.OkresTo DESC, d.ID DESC
               ),
               (
                   SELECT TOP (1) e.StatusEDeklaracji
                   FROM {dbQuoted}.dbo.EDeklaracje e
                   WHERE e.TypDeklaracji = @TypPIT8A
                     AND e.OkresDeklaracjiTo >= @OkresFrom
                     AND e.OkresDeklaracjiTo <= @OkresTo
                   ORDER BY e.OkresDeklaracjiTo DESC, e.ID DESC
               ),
               (
                   SELECT TOP (1) CAST(d.Bufor AS bit)
                   FROM {dbQuoted}.dbo.Deklaracje d
                   WHERE d.Typ = @TypPIT4
                     AND d.Zrodlo IS NULL
                     AND d.OkresTo >= @OkresFrom
                     AND d.OkresTo <= @OkresTo
                   ORDER BY d.OkresTo DESC, d.ID DESC
               ),
               (
                   SELECT TOP (1) e.StatusEDeklaracji
                   FROM {dbQuoted}.dbo.EDeklaracje e
                   WHERE e.TypDeklaracji = @TypPIT4
                     AND e.OkresDeklaracjiTo >= @OkresFrom
                     AND e.OkresDeklaracjiTo <= @OkresTo
                   ORDER BY e.OkresDeklaracjiTo DESC, e.ID DESC
               ),
               (
                   SELECT TOP (1) CAST(d.Bufor AS bit)
                   FROM {dbQuoted}.dbo.Deklaracje d
                   WHERE d.Typ = @TypCIT8
                     AND d.Zrodlo IS NULL
                     AND d.OkresTo >= @OkresFrom
                     AND d.OkresTo <= @OkresTo
                   ORDER BY d.OkresTo DESC, d.ID DESC
               ),
               (
                   SELECT TOP (1) e.StatusEDeklaracji
                   FROM {dbQuoted}.dbo.EDeklaracje e
                   WHERE e.TypDeklaracji = @TypCIT8
                     AND e.OkresDeklaracjiTo >= @OkresFrom
                     AND e.OkresDeklaracjiTo <= @OkresTo
                   ORDER BY e.OkresDeklaracjiTo DESC, e.ID DESC
               );
    END
END TRY
BEGIN CATCH
    INSERT INTO @results(DatabaseName, StatusJPKInt, DeklaracjaVATUEBufor, EDeklaracjaVATUEInt, DeklaracjaPIT8ARBufor, EDeklaracjaPIT8ARInt, DeklaracjaPIT4RBufor, EDeklaracjaPIT4RInt, DeklaracjaCIT8Bufor, EDeklaracjaCIT8Int)
    VALUES (N'{dbLiteral}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
END CATCH");
        }

        return @$"
DECLARE @results TABLE (
    DatabaseName nvarchar(128) NOT NULL,
    StatusJPKInt int NULL,
    DeklaracjaVATUEBufor bit NULL,
    EDeklaracjaVATUEInt int NULL,
    DeklaracjaPIT8ARBufor bit NULL,
    EDeklaracjaPIT8ARInt int NULL,
    DeklaracjaPIT4RBufor bit NULL,
    EDeklaracjaPIT4RInt int NULL,
    DeklaracjaCIT8Bufor bit NULL,
    EDeklaracjaCIT8Int int NULL
);
{string.Join(Environment.NewLine, parts)}
SELECT DatabaseName, StatusJPKInt, DeklaracjaVATUEBufor, EDeklaracjaVATUEInt, DeklaracjaPIT8ARBufor, EDeklaracjaPIT8ARInt, DeklaracjaPIT4RBufor, EDeklaracjaPIT4RInt, DeklaracjaCIT8Bufor, EDeklaracjaCIT8Int
FROM @results;";
    }

    private static string EscapeSqlLiteral(string value)
    {
        return value.Replace("'", "''");
    }

    private static string QuoteSqlIdentifier(string value)
    {
        return $"[{value.Replace("]", "]]")}]";
    }

    private static int GetTypDeklaracjiValue(string enumName)
    {
        Type type = Type.GetType("Soneta.Deklaracje.TypDeklaracji,Soneta.Deklaracje", throwOnError: false)
            ?? throw new InvalidOperationException("Nie znaleziono typu Soneta.Deklaracje.TypDeklaracji.");
        object value = Enum.Parse(type, enumName, ignoreCase: false);
        return Convert.ToInt32(value);
    }

    private static string ToStatusCaption(int? statusCode)
    {
        return statusCode switch
        {
            10 => "Utworzony",
            15 => "Oczekuje na podpis",
            20 => "Gotowy do wysyłki",
            30 => "Wysyłanie plików...",
            40 => "Przetwarzanie (MF)...",
            100 => "Pobrano UPO",
            200 => "Błąd przetwarzania",
            _ => "Brak"
        };
    }

    private static string ToDeklaracjaStatus(bool? bufor)
    {
        if (!bufor.HasValue)
        {
            return "Brak";
        }

        return bufor.Value ? "Bufor" : "Zatwierdzona";
    }

    private static string ToEDeklaracjaStatusCaption(int? statusCode)
    {
        if (!statusCode.HasValue)
        {
            return "Brak";
        }

        Type type = Type.GetType("Soneta.Deklaracje.StatusEDeklaracji,Soneta.Deklaracje", throwOnError: false);
        if (type == null)
        {
            return statusCode.Value.ToString();
        }

        object value = Enum.ToObject(type, statusCode.Value);
        if (value is Enum enumValue)
        {
            return CaptionAttribute.EnumToString(enumValue);
        }

        return statusCode.Value.ToString();
    }

    private static void SetCalculatedValues(DBItem item, IReadOnlyDictionary<string, string> valuesToSet, Date actual, FromTo okres, bool updateCalculationMetadata = true)
    {
        object extender = item.CalculatedProperties;
        Type extenderType = extender.GetType();

        FieldInfo valuesField = extenderType.GetField("_values", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingFieldException(extenderType.FullName, "_values");
        IDictionary valuesDictionary = (IDictionary)(valuesField.GetValue(extender)
            ?? throw new InvalidOperationException("Brak słownika wartości wyliczanych."));
        valuesDictionary.Clear();

        object manager = BusinessModule.GetInstance(item).Config.DBItemsManager;
        MethodInfo getDescriptors = manager.GetType().GetMethod("GetExtenderCalculatorDescriptors", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(manager.GetType().FullName, "GetExtenderCalculatorDescriptors");
        PropertyDescriptor[] descriptors = (PropertyDescriptor[])getDescriptors.Invoke(manager, null);

        Type calculatedValueType = extenderType.GetNestedType("CalculatedValue", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new MissingMemberException(extenderType.FullName, "CalculatedValue");
        ConstructorInfo calculatedValueCtor = calculatedValueType.GetConstructor(new[] { typeof(PropertyDescriptor), typeof(object) })
            ?? throw new MissingMethodException(calculatedValueType.FullName, ".ctor(PropertyDescriptor, object)");

        foreach (PropertyDescriptor descriptor in descriptors)
        {
            string value = valuesToSet.TryGetValue(descriptor.Name, out string selectedValue) ? selectedValue : null;
            object calculatedValue = calculatedValueCtor.Invoke(new object[] { descriptor, value });
            valuesDictionary[descriptor.Name] = calculatedValue;
        }

        if (updateCalculationMetadata)
        {
            TrySetProperty(extenderType, extender, "LastCalculationActual", actual);
            TrySetProperty(extenderType, extender, "LastCalculationFromTo", okres);
            TrySetProperty(extenderType, extender, "LastCalculationTime", Date.Now);
        }
    }

    private static void TrySetProperty(Type type, object target, string propertyName, object value)
    {
        try
        {
            type.GetProperty(propertyName)?.SetValue(target, value);
        }
        catch
        {
        }
    }
}
