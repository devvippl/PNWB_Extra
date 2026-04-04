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

[assembly: Worker(typeof(PNWB_Extra.UI.Workers.DynamiczneDaneCalaEnovaObliczExtraWorker), typeof(PNWB_Extra.UI.Models.DynamiczneDaneCalaEnovaRoot))]

namespace PNWB_Extra.UI.Workers;

public sealed class DynamiczneDaneCalaEnovaObliczExtraWorker
{
    [Context]
    public DynamiczneDaneCalaEnovaRoot Root { get; set; }

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
        SaveChangesCore();
        CalculateExtraCore();
        context.InvokeChanged();
    }

    [Action("Zapisz zmiany", Priority = 3, Icon = ActionIcon.Save, Target = ActionTarget.ToolbarWithText, Mode = (ActionMode.Progress | ActionMode.OnlyWebForms | ActionMode.NoSession | ActionMode.ReadOnlySession | ActionMode.SingleSession))]
    public void SaveChangesWeb()
    {
        SaveChangesCore();
        CalculateExtraCore();
        context.InvokeChanged();
    }

    private void CalculateExtraCore()
    {
        Session session = context.Session;
        bool ownsSession = false;
        if (session == null)
        {
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.DynamiczneDaneCalaEnova.ObliczExtra");
            ownsSession = true;
        }

        try
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            DynamiczneDaneSelection selection = Root.GetSelection();
            DataTable table = LoadTableFromSql(session, selection);

            Root.SetLoadedSelection(selection);
            Root.ReplaceTable(table);
            stopwatch.Stop();

            int rowsCount = table.Rows.Count;
            int firmyCount = table.AsEnumerable()
                .Select(r => r["NazwaFirmy"]?.ToString())
                .Where(v => !string.IsNullOrWhiteSpace(v))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Count();
            Log log = new Log("Dynamiczne dane cała enova", open: true);
            log.WriteLine("Oblicz Extra SQL: obiekt={0}, kolumna={1}, rekordy={2}, firmy={3}, czas={4} ms",
                selection.DisplayObjectName,
                selection.ColumnName,
                rowsCount,
                firmyCount,
                stopwatch.ElapsedMilliseconds);
        }
        finally
        {
            if (ownsSession)
            {
                session.Dispose();
            }
        }
    }

    private static DataTable LoadTableFromSql(Session session, DynamiczneDaneSelection selection)
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
        command.CommandText = BuildBatchSql(masterDatabaseName, selection);

        using SqlDataAdapter adapter = new SqlDataAdapter(command);
        DataTable table = new DataTable("DynamiczneDane");
        adapter.Fill(table);
        return table;
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
        return PnwbSqlConnectionFactory.Create(sqlDatabase, "PNWB_Extra.DynamiczneDaneCalaEnova");
    }

    private static string BuildBatchSql(string masterDatabaseName, DynamiczneDaneSelection selection)
    {
        string masterDbQuoted = QuoteSqlIdentifier(masterDatabaseName);
        string escapedSchema = selection.SchemaName.Replace("'", "''");
        string escapedObject = selection.ObjectName.Replace("'", "''");
        string escapedObjectDisplay = selection.DisplayObjectName.Replace("'", "''");
        string escapedColumn = selection.ColumnName.Replace("'", "''");
        string escapedWhereColumn = (selection.WhereColumnName ?? string.Empty).Replace("'", "''");
        string escapedWhereOperator = (selection.WhereOperator ?? string.Empty).ToUpperInvariant().Replace("'", "''");
        string escapedWhereValue = (selection.WhereValue ?? string.Empty).Replace("'", "''");
        string selectAllBit = selection.SelectAllColumns ? "1" : "0";
        string whereEnabledBit = selection.HasWhereClause ? "1" : "0";

        string whereSqlBuilder = BuildWhereSqlBuilderScript();

        return $@"
DECLARE @dbname NVARCHAR(128);
DECLARE @unionSql NVARCHAR(MAX) = N'';
DECLARE @schemaName NVARCHAR(128) = N'{escapedSchema}';
DECLARE @objectName NVARCHAR(128) = N'{escapedObject}';
DECLARE @columnName NVARCHAR(128) = N'{escapedColumn}';
DECLARE @selectAll bit = {selectAllBit};
DECLARE @displayObject NVARCHAR(256) = N'{escapedObjectDisplay}';
DECLARE @whereEnabled bit = {whereEnabledBit};
DECLARE @whereColumnName NVARCHAR(128) = N'{escapedWhereColumn}';
DECLARE @whereOperator NVARCHAR(32) = N'{escapedWhereOperator}';
DECLARE @whereValue NVARCHAR(MAX) = N'{escapedWhereValue}';

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
       AND OBJECT_ID(QUOTENAME(@dbname) + N'.' + QUOTENAME(@schemaName) + N'.' + QUOTENAME(@objectName)) IS NOT NULL
       AND (@selectAll = 1 OR COL_LENGTH(@dbname + N'.' + @schemaName + N'.' + @objectName, @columnName) IS NOT NULL)
       AND (@whereEnabled = 0 OR COL_LENGTH(@dbname + N'.' + @schemaName + N'.' + @objectName, @whereColumnName) IS NOT NULL)
    BEGIN
        DECLARE @selectSql NVARCHAR(MAX);
        DECLARE @whereSql NVARCHAR(MAX) = N'';
{whereSqlBuilder}

        IF @selectAll = 1
        BEGIN
            SET @selectSql = N'
SELECT N''' + REPLACE(@dbname, '''', '''''') + N''' AS NazwaFirmy,
       SRC.*
FROM ' + QUOTENAME(@dbname) + N'.' + QUOTENAME(@schemaName) + N'.' + QUOTENAME(@objectName) + N' AS SRC' + @whereSql;
        END
        ELSE
        BEGIN
            SET @selectSql = N'
SELECT N''' + REPLACE(@dbname, '''', '''''') + N''' AS NazwaFirmy,
       TRY_CONVERT(nvarchar(max), SRC.' + QUOTENAME(@columnName) + N') AS ' + QUOTENAME(@columnName) + N'
FROM ' + QUOTENAME(@dbname) + N'.' + QUOTENAME(@schemaName) + N'.' + QUOTENAME(@objectName) + N' AS SRC' + @whereSql;
        END

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
    DECLARE @emptySql NVARCHAR(MAX) = N'SELECT TOP (0) CAST(NULL AS nvarchar(128)) AS [NazwaFirmy]';
    IF @selectAll = 1
    BEGIN
        SELECT @emptySql = @emptySql + N', CAST(NULL AS nvarchar(max)) AS ' + QUOTENAME(C.name)
        FROM sys.columns C
        INNER JOIN sys.objects O ON O.object_id = C.object_id
        INNER JOIN sys.schemas S ON S.schema_id = O.schema_id
        WHERE S.name = @schemaName
          AND O.name = @objectName
          AND O.type IN ('U', 'V')
        ORDER BY C.column_id;
    END
    ELSE
    BEGIN
        SET @emptySql = @emptySql + N', CAST(NULL AS nvarchar(max)) AS ' + QUOTENAME(@columnName);
    END
    EXEC sp_executesql @emptySql;
END
ELSE
BEGIN
    EXEC sp_executesql @unionSql;
END";
    }

    private static string BuildWhereSqlBuilderScript()
    {
        return @"
        IF @whereEnabled = 1 AND @whereColumnName <> N''
        BEGIN
            IF @whereOperator = N'IS NULL'
            BEGIN
                SET @whereSql = N' WHERE SRC.' + QUOTENAME(@whereColumnName) + N' IS NULL';
            END
            ELSE IF @whereOperator = N'IS NOT NULL'
            BEGIN
                SET @whereSql = N' WHERE SRC.' + QUOTENAME(@whereColumnName) + N' IS NOT NULL';
            END
            ELSE IF @whereOperator = N'CONTAINS'
            BEGIN
                SET @whereSql = N' WHERE TRY_CONVERT(nvarchar(max), SRC.' + QUOTENAME(@whereColumnName) + N') LIKE N''%' + REPLACE(@whereValue, '''', '''''') + N'%''';
            END
            ELSE IF @whereOperator = N'STARTS WITH'
            BEGIN
                SET @whereSql = N' WHERE TRY_CONVERT(nvarchar(max), SRC.' + QUOTENAME(@whereColumnName) + N') LIKE N''' + REPLACE(@whereValue, '''', '''''') + N'%''';
            END
            ELSE IF @whereOperator = N'ENDS WITH'
            BEGIN
                SET @whereSql = N' WHERE TRY_CONVERT(nvarchar(max), SRC.' + QUOTENAME(@whereColumnName) + N') LIKE N''%' + REPLACE(@whereValue, '''', '''''') + N'''';
            END
            ELSE IF @whereOperator IN (N'=', N'<>', N'>', N'>=', N'<', N'<=')
            BEGIN
                SET @whereSql = N' WHERE TRY_CONVERT(nvarchar(max), SRC.' + QUOTENAME(@whereColumnName) + N') ' + @whereOperator + N' N''' + REPLACE(@whereValue, '''', '''''') + N'''';
            END
            ELSE IF @whereOperator IN (N'LIKE', N'NOT LIKE')
            BEGIN
                SET @whereSql = N' WHERE TRY_CONVERT(nvarchar(max), SRC.' + QUOTENAME(@whereColumnName) + N') ' + @whereOperator + N' N''' + REPLACE(@whereValue, '''', '''''') + N'''';
            END
        END";
    }

    private static string QuoteSqlIdentifier(string value)
    {
        return $"[{value.Replace("]", "]]")}]";
    }

    private void SaveChangesCore()
    {
        Session session = context.Session;
        bool ownsSession = false;
        if (session == null)
        {
            session = context.Login.CreateSession(readOnly: true, config: true, "PNWB_Extra.DynamiczneDaneCalaEnova.SaveChanges");
            ownsSession = true;
        }

        try
        {
            DynamiczneDaneSelection selection = Root.GetLoadedSelection();
            if (selection == null)
            {
                throw new InvalidOperationException("Najpierw wykonaj 'Oblicz Extra', aby zainicjalizować dane do zapisu.");
            }

            if (!selection.SelectAllColumns)
            {
                throw new InvalidOperationException("Zapisz zmiany działa tylko dla trybu 'Kolumna = *'.");
            }

            IReadOnlyList<DynamicEditedRow> editedRows = Root.GetEditedRows();
            if (editedRows.Count == 0)
            {
                Log noChangesLog = new Log("Dynamiczne dane cała enova", open: true);
                noChangesLog.WriteLine("Zapisz zmiany: brak zmian do zapisania.");
                return;
            }

            if (session.Login.Database is not SqlDatabase sqlDatabase)
            {
                throw new InvalidOperationException("Bieżąca baza enova nie jest bazą SQL.");
            }

            using SqlConnection connection = CreateSqlConnection(sqlDatabase);
            connection.Open();

            TableMetadata metadata = LoadTableMetadata(connection, selection);
            if (metadata.PrimaryKeys.Count == 0)
            {
                throw new InvalidOperationException($"Obiekt '{selection.DisplayObjectName}' nie ma klucza głównego (PK). Zapis dynamiczny został zablokowany dla bezpieczeństwa.");
            }

            int touchedRows = 0;
            int updatedRows = 0;
            int skippedRows = 0;

            foreach (DynamicEditedRow row in editedRows)
            {
                RowUpdatePlan plan = BuildUpdatePlan(row, metadata);
                if (plan.SetColumns.Count == 0 || plan.MissingPrimaryKeys.Count > 0)
                {
                    skippedRows++;
                    continue;
                }

                touchedRows++;
                updatedRows += ExecuteRowUpdate(connection, selection, row, plan);
            }

            Log log = new Log("Dynamiczne dane cała enova", open: true);
            log.WriteLine("Zapisz zmiany: zmienione={0}, zapisane={1}, pominięte={2}", touchedRows, updatedRows, skippedRows);
        }
        finally
        {
            if (ownsSession)
            {
                session.Dispose();
            }
        }
    }

    private static int ExecuteRowUpdate(SqlConnection connection, DynamiczneDaneSelection selection, DynamicEditedRow row, RowUpdatePlan plan)
    {
        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;

        string dbQuoted = QuoteSqlIdentifier(row.DatabaseName);
        string objectPath = $"{dbQuoted}.{QuoteSqlIdentifier(selection.SchemaName)}.{QuoteSqlIdentifier(selection.ObjectName)}";

        StringBuilder setBuilder = new StringBuilder();
        for (int i = 0; i < plan.SetColumns.Count; i++)
        {
            ColumnMetadata column = plan.SetColumns[i];
            if (i > 0)
            {
                setBuilder.Append(",\n    ");
            }

            string parameterName = $"@set{i}";
            string sqlType = BuildSqlTypeDeclaration(column);
            setBuilder.Append(QuoteSqlIdentifier(column.Name))
                .Append(" = CASE WHEN ")
                .Append(parameterName)
                .Append(" IS NULL THEN NULL ELSE TRY_CONVERT(")
                .Append(sqlType)
                .Append(", ")
                .Append(parameterName)
                .Append(") END");

            string rawValue = TryGet(row.CurrentValues, column.Name);
            command.Parameters.Add(new SqlParameter(parameterName, SqlDbType.NVarChar, -1)
            {
                Value = DbValue(NormalizeValueForSql(column, rawValue))
            });
        }

        StringBuilder whereBuilder = new StringBuilder();
        for (int i = 0; i < plan.PrimaryKeys.Count; i++)
        {
            ColumnMetadata pk = plan.PrimaryKeys[i];
            string parameterName = $"@pk{i}";
            string sqlType = BuildSqlTypeDeclaration(pk);
            if (i > 0)
            {
                whereBuilder.Append("\n  AND ");
            }

            whereBuilder.Append("((")
                .Append(parameterName)
                .Append(" IS NULL AND T.")
                .Append(QuoteSqlIdentifier(pk.Name))
                .Append(" IS NULL) OR T.")
                .Append(QuoteSqlIdentifier(pk.Name))
                .Append(" = TRY_CONVERT(")
                .Append(sqlType)
                .Append(", ")
                .Append(parameterName)
                .Append("))");

            string rawValue = TryGet(row.OriginalValues, pk.Name);
            command.Parameters.Add(new SqlParameter(parameterName, SqlDbType.NVarChar, -1)
            {
                Value = DbValue(NormalizeValueForSql(pk, rawValue))
            });
        }

        command.CommandText = $@"
UPDATE T
SET {setBuilder}
FROM {objectPath} AS T
WHERE {whereBuilder};

SELECT @@ROWCOUNT;";

        object scalar = command.ExecuteScalar();
        return scalar == null || scalar == DBNull.Value ? 0 : Convert.ToInt32(scalar);
    }

    private static RowUpdatePlan BuildUpdatePlan(DynamicEditedRow row, TableMetadata metadata)
    {
        List<ColumnMetadata> setColumns = row.ChangedColumns
            .Where(c => !string.Equals(c, "NazwaFirmy", StringComparison.OrdinalIgnoreCase))
            .Select(c => metadata.TryGetColumn(c, out ColumnMetadata meta) ? meta : null)
            .Where(c => c != null)
            .Where(c => !c.IsPrimaryKey && c.IsUpdatable)
            .Distinct(new ColumnMetadataNameComparer())
            .ToList();

        List<ColumnMetadata> missingPrimaryKeys = metadata.PrimaryKeys
            .Where(pk => !row.OriginalValues.ContainsKey(pk.Name) || string.IsNullOrWhiteSpace(TryGet(row.OriginalValues, pk.Name)))
            .ToList();

        return new RowUpdatePlan(setColumns, metadata.PrimaryKeys, missingPrimaryKeys);
    }

    private static TableMetadata LoadTableMetadata(SqlConnection connection, DynamiczneDaneSelection selection)
    {
        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = @"
SELECT
    C.name AS ColumnName,
    T.name AS TypeName,
    C.max_length AS MaxLength,
    C.precision AS [Precision],
    C.scale AS [Scale],
    C.is_identity AS IsIdentity,
    C.is_computed AS IsComputed,
    CASE WHEN PK.column_id IS NULL THEN 0 ELSE 1 END AS IsPrimaryKey
FROM sys.columns C
INNER JOIN sys.objects O ON O.object_id = C.object_id
INNER JOIN sys.schemas S ON S.schema_id = O.schema_id
INNER JOIN sys.types T ON T.user_type_id = C.user_type_id
LEFT JOIN (
    SELECT IC.object_id, IC.column_id
    FROM sys.indexes I
    INNER JOIN sys.index_columns IC ON IC.object_id = I.object_id AND IC.index_id = I.index_id
    WHERE I.is_primary_key = 1
) PK ON PK.object_id = C.object_id AND PK.column_id = C.column_id
WHERE S.name = @SchemaName
  AND O.name = @ObjectName
  AND O.type IN ('U', 'V')
ORDER BY C.column_id;";
        command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar, 128) { Value = selection.SchemaName });
        command.Parameters.Add(new SqlParameter("@ObjectName", SqlDbType.NVarChar, 128) { Value = selection.ObjectName });

        List<ColumnMetadata> columns = new List<ColumnMetadata>();
        using SqlDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string columnName = reader.GetString(0);
            string typeName = reader.GetString(1);
            short maxLength = reader.GetInt16(2);
            byte precision = reader.GetByte(3);
            byte scale = reader.GetByte(4);
            bool isIdentity = reader.GetBoolean(5);
            bool isComputed = reader.GetBoolean(6);
            bool isPrimaryKey = reader.GetInt32(7) == 1;

            bool isRowVersion = string.Equals(typeName, "timestamp", StringComparison.OrdinalIgnoreCase)
                                || string.Equals(typeName, "rowversion", StringComparison.OrdinalIgnoreCase);
            bool isUpdatable = !isIdentity && !isComputed && !isRowVersion;

            columns.Add(new ColumnMetadata(columnName, typeName, maxLength, precision, scale, isPrimaryKey, isUpdatable));
        }

        return new TableMetadata(columns);
    }

    private static string BuildSqlTypeDeclaration(ColumnMetadata column)
    {
        string type = column.TypeName;
        switch (type.ToLowerInvariant())
        {
            case "nvarchar":
            case "nchar":
                return $"{type}({(column.MaxLength == -1 ? "max" : Math.Max(1, (int)column.MaxLength / 2).ToString())})";
            case "varchar":
            case "char":
            case "varbinary":
            case "binary":
                return $"{type}({(column.MaxLength == -1 ? "max" : Math.Max(1, (int)column.MaxLength).ToString())})";
            case "decimal":
            case "numeric":
                return $"{type}({column.Precision},{column.Scale})";
            case "datetime2":
            case "datetimeoffset":
            case "time":
                return $"{type}({column.Scale})";
            default:
                return type;
        }
    }

    private static object DbValue(string value)
    {
        return value == null ? DBNull.Value : value;
    }

    private static string NormalizeValueForSql(ColumnMetadata column, string rawValue)
    {
        if (rawValue == null)
        {
            return null;
        }

        if (string.Equals(column.TypeName, "bit", StringComparison.OrdinalIgnoreCase))
        {
            if (bool.TryParse(rawValue, out bool boolValue))
            {
                return boolValue ? "1" : "0";
            }
        }

        return rawValue;
    }

    private static string TryGet(IReadOnlyDictionary<string, string> values, string key)
    {
        return values != null && values.TryGetValue(key, out string value) ? value : null;
    }

    private sealed class TableMetadata
    {
        private readonly Dictionary<string, ColumnMetadata> byName;

        public TableMetadata(IEnumerable<ColumnMetadata> columns)
        {
            Columns = columns.ToList();
            PrimaryKeys = Columns.Where(c => c.IsPrimaryKey).ToList();
            byName = Columns.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);
        }

        public IReadOnlyList<ColumnMetadata> Columns { get; }

        public IReadOnlyList<ColumnMetadata> PrimaryKeys { get; }

        public bool TryGetColumn(string name, out ColumnMetadata metadata)
        {
            return byName.TryGetValue(name, out metadata);
        }
    }

    private sealed class ColumnMetadata
    {
        public ColumnMetadata(string name, string typeName, short maxLength, byte precision, byte scale, bool isPrimaryKey, bool isUpdatable)
        {
            Name = name;
            TypeName = typeName;
            MaxLength = maxLength;
            Precision = precision;
            Scale = scale;
            IsPrimaryKey = isPrimaryKey;
            IsUpdatable = isUpdatable;
        }

        public string Name { get; }

        public string TypeName { get; }

        public short MaxLength { get; }

        public byte Precision { get; }

        public byte Scale { get; }

        public bool IsPrimaryKey { get; }

        public bool IsUpdatable { get; }
    }

    private sealed class ColumnMetadataNameComparer : IEqualityComparer<ColumnMetadata>
    {
        public bool Equals(ColumnMetadata x, ColumnMetadata y)
        {
            return string.Equals(x?.Name, y?.Name, StringComparison.OrdinalIgnoreCase);
        }

        public int GetHashCode(ColumnMetadata obj)
        {
            return (obj?.Name ?? string.Empty).ToUpperInvariant().GetHashCode();
        }
    }

    private sealed class RowUpdatePlan
    {
        public RowUpdatePlan(IReadOnlyList<ColumnMetadata> setColumns, IReadOnlyList<ColumnMetadata> primaryKeys, IReadOnlyList<ColumnMetadata> missingPrimaryKeys)
        {
            SetColumns = setColumns;
            PrimaryKeys = primaryKeys;
            MissingPrimaryKeys = missingPrimaryKeys;
        }

        public IReadOnlyList<ColumnMetadata> SetColumns { get; }

        public IReadOnlyList<ColumnMetadata> PrimaryKeys { get; }

        public IReadOnlyList<ColumnMetadata> MissingPrimaryKeys { get; }
    }
}
