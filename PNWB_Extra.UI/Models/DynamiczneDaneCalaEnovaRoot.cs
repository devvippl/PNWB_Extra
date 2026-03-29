using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Soneta.Business;
using Soneta.Business.App;
using Soneta.Business.UI;
using Soneta.Types;

namespace PNWB_Extra.UI.Models;

public sealed class DynamiczneDaneCalaEnovaRoot : ISessionable
{
    private IList dynamicRows = Array.Empty<object>();
    private Type dynamicRowType = typeof(DynamicEmptyRow);
    private readonly Dictionary<object, Dictionary<string, string>> originalRows = new Dictionary<object, Dictionary<string, string>>(ReferenceObjectComparer.Instance);
    private ViewInfo viewInfo;
    private bool metadataLoaded;
    private string[] objectOptions = Array.Empty<string>();
    private readonly Dictionary<string, string[]> columnOptions = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
    private string obiektSql;
    private string kolumnaSql = "*";
    private string whereKolumna;
    private string whereOperator = "=";
    private string whereWartosc;
    private DynamiczneDaneSelection loadedSelection;

    private static readonly string[] WhereOperators = new[]
    {
        "=",
        "<>",
        ">",
        ">=",
        "<",
        "<=",
        "LIKE",
        "NOT LIKE",
        "CONTAINS",
        "STARTS WITH",
        "ENDS WITH",
        "IS NULL",
        "IS NOT NULL"
    };

    [Context]
    public Session Session { get; set; }

    [Caption("Tabela / Widok")]
    public string ObiektSql
    {
        get => obiektSql;
        set
        {
            if (string.Equals(obiektSql, value, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            obiektSql = value;
            kolumnaSql = "*";
            whereKolumna = null;
            whereOperator = "=";
            whereWartosc = null;
            Session?.InvokeChanged();
        }
    }

    [Caption("Kolumna")]
    public string KolumnaSql
    {
        get => kolumnaSql;
        set
        {
            if (string.Equals(kolumnaSql, value, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            kolumnaSql = value;
            Session?.InvokeChanged();
        }
    }

    [Caption("Where kolumna")]
    public string WhereKolumna
    {
        get => whereKolumna;
        set
        {
            if (string.Equals(whereKolumna, value, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            whereKolumna = value;
            Session?.InvokeChanged();
        }
    }

    [Caption("Where operator")]
    public string WhereOperator
    {
        get => whereOperator;
        set
        {
            if (string.Equals(whereOperator, value, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            whereOperator = value;
            Session?.InvokeChanged();
        }
    }

    [Caption("Where wartość")]
    public string WhereWartosc
    {
        get => whereWartosc;
        set
        {
            if (string.Equals(whereWartosc, value, StringComparison.Ordinal))
            {
                return;
            }

            whereWartosc = value;
            Session?.InvokeChanged();
        }
    }

    public ViewInfo ViewInfo
    {
        get
        {
            if (viewInfo != null)
            {
                return viewInfo;
            }

            viewInfo = new DynamiczneDaneViewInfo(() => dynamicRowType)
            {
                NonViewCollection = true,
                RowType = dynamicRowType
            };
            viewInfo.CreateView += (_, args) =>
            {
                viewInfo.RowType = dynamicRowType;
                args.DataSource = dynamicRows;
            };
            viewInfo.Action += (_, args) =>
            {
                if (args.Action == ActionEventArgs.Actions.Edit)
                {
                    args.Cancel = true;
                    object current = args.OriginalFocusedData ?? args.FocusedData;
                    if (current == null)
                    {
                        return;
                    }

                    object editor = CloneRowObject(current);
                    args.FocusedData = new FormActionResult
                    {
                        EditValue = editor,
                        CommittedHandler = _ =>
                        {
                            CopyRowValues(editor, current);
                            Session?.InvokeChanged();
                            return null;
                        }
                    };
                    return;
                }

                if (args.Action == ActionEventArgs.Actions.Update)
                {
                    args.Cancel = true;
                    Session?.InvokeChanged();
                }
            };
            return viewInfo;
        }
    }

    public object GetListObiektSql()
    {
        EnsureMetadataLoaded();
        return objectOptions;
    }

    public object GetListKolumnaSql()
    {
        EnsureMetadataLoaded();
        if (string.IsNullOrWhiteSpace(ObiektSql))
        {
            return new[] { "*" };
        }

        return GetColumnsForObject(ObiektSql);
    }

    public object GetListWhereKolumna()
    {
        EnsureMetadataLoaded();
        if (string.IsNullOrWhiteSpace(ObiektSql))
        {
            return Array.Empty<string>();
        }

        return GetColumnsForObject(ObiektSql)
            .Where(c => !string.Equals(c, "*", StringComparison.Ordinal))
            .ToArray();
    }

    public object GetListWhereOperator()
    {
        return WhereOperators;
    }

    public void ReplaceTable(DataTable table)
    {
        if (table == null)
        {
            dynamicRows = Array.Empty<object>();
            dynamicRowType = typeof(DynamicEmptyRow);
            originalRows.Clear();
            viewInfo = null;
            Session?.InvokeChanged();
            return;
        }

        dynamicRows = DynamicSqlRowFactory.CreateRows(table, out Type rowType);
        dynamicRowType = rowType ?? typeof(DynamicEmptyRow);
        CaptureOriginalRows();
        viewInfo = null;

        Session?.InvokeChanged();
    }

    public void SetLoadedSelection(DynamiczneDaneSelection selection)
    {
        loadedSelection = selection;
    }

    public DynamiczneDaneSelection GetLoadedSelection()
    {
        return loadedSelection;
    }

    public IReadOnlyList<DynamicEditedRow> GetEditedRows()
    {
        List<DynamicEditedRow> edited = new List<DynamicEditedRow>();
        foreach (object row in EnumerateRows())
        {
            Dictionary<string, string> current = CaptureRowValues(row);
            if (!originalRows.TryGetValue(row, out Dictionary<string, string> original))
            {
                original = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            string[] changedColumns = current.Keys
                .Union(original.Keys, StringComparer.OrdinalIgnoreCase)
                .Where(k => !string.Equals(GetValue(original, k), GetValue(current, k), StringComparison.Ordinal))
                .ToArray();

            if (changedColumns.Length == 0)
            {
                continue;
            }

            string dbName = GetValue(current, "NazwaFirmy");
            if (string.IsNullOrWhiteSpace(dbName))
            {
                continue;
            }

            edited.Add(new DynamicEditedRow(
                dbName,
                new Dictionary<string, string>(original, StringComparer.OrdinalIgnoreCase),
                current,
                changedColumns));
        }

        return edited;
    }

    public DynamiczneDaneSelection GetSelection()
    {
        EnsureMetadataLoaded();

        if (objectOptions.Length == 0)
        {
            throw new InvalidOperationException("Nie znaleziono dostępnych tabel/widoków SQL.");
        }

        string selectedObject = (ObiektSql ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(selectedObject))
        {
            selectedObject = objectOptions[0];
            obiektSql = selectedObject;
        }

        if (!objectOptions.Contains(selectedObject, StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Wybrany obiekt SQL '{selectedObject}' nie znajduje się na liście.");
        }

        string[] availableColumns = GetColumnsForObject(selectedObject);
        string selectedColumn = (KolumnaSql ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(selectedColumn))
        {
            selectedColumn = "*";
            kolumnaSql = "*";
        }

        if (!availableColumns.Contains(selectedColumn, StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Wybrana kolumna '{selectedColumn}' nie istnieje w obiekcie '{selectedObject}'.");
        }

        string selectedWhereColumn = (WhereKolumna ?? string.Empty).Trim();
        string selectedWhereOperator = (WhereOperator ?? string.Empty).Trim().ToUpperInvariant();
        string selectedWhereValue = WhereWartosc;

        bool hasWhere = !string.IsNullOrWhiteSpace(selectedWhereColumn);
        if (hasWhere)
        {
            string[] whereColumns = availableColumns
                .Where(c => !string.Equals(c, "*", StringComparison.Ordinal))
                .ToArray();

            if (!whereColumns.Contains(selectedWhereColumn, StringComparer.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Wybrana kolumna WHERE '{selectedWhereColumn}' nie istnieje w obiekcie '{selectedObject}'.");
            }

            if (!WhereOperators.Contains(selectedWhereOperator, StringComparer.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Operator WHERE '{selectedWhereOperator}' nie jest obsługiwany.");
            }

            bool requiresValue = selectedWhereOperator is not "IS NULL" and not "IS NOT NULL";
            if (requiresValue && string.IsNullOrWhiteSpace(selectedWhereValue))
            {
                throw new InvalidOperationException("Dla wybranego operatora WHERE musisz podać wartość.");
            }

            if (!requiresValue)
            {
                selectedWhereValue = null;
            }
        }

        ParseObjectName(selectedObject, out string schemaName, out string objectName);
        return new DynamiczneDaneSelection(
            selectedObject,
            schemaName,
            objectName,
            selectedColumn,
            string.Equals(selectedColumn, "*", StringComparison.Ordinal),
            hasWhere,
            selectedWhereColumn,
            selectedWhereOperator,
            selectedWhereValue);
    }

    private void EnsureMetadataLoaded()
    {
        if (metadataLoaded)
        {
            return;
        }

        if (Session?.Login?.Database is not SqlDatabase sqlDatabase)
        {
            objectOptions = Array.Empty<string>();
            metadataLoaded = true;
            return;
        }

        using SqlConnection connection = CreateSqlConnection(sqlDatabase, "PNWB_Extra.DynamiczneDane.Meta");
        connection.Open();

        List<string> objects = new List<string>();
        using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandType = CommandType.Text;
            command.CommandText = @"
SELECT CASE
           WHEN S.name = N'dbo' THEN O.name
           ELSE S.name + N'.' + O.name
       END AS ObjectName
FROM sys.objects O
INNER JOIN sys.schemas S ON S.schema_id = O.schema_id
WHERE O.type IN ('U', 'V')
  AND O.is_ms_shipped = 0
ORDER BY CASE WHEN S.name = N'dbo' THEN 0 ELSE 1 END, S.name, O.name;";

            using SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                string objectName = reader.IsDBNull(0) ? null : reader.GetString(0);
                if (!string.IsNullOrWhiteSpace(objectName))
                {
                    objects.Add(objectName);
                }
            }
        }

        objectOptions = objects.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        metadataLoaded = true;

        if (string.IsNullOrWhiteSpace(obiektSql) && objectOptions.Length > 0)
        {
            obiektSql = objectOptions[0];
        }
    }

    private string[] GetColumnsForObject(string fullObjectName)
    {
        if (string.IsNullOrWhiteSpace(fullObjectName))
        {
            return new[] { "*" };
        }

        if (columnOptions.TryGetValue(fullObjectName, out string[] cached))
        {
            return cached;
        }

        ParseObjectName(fullObjectName, out string schemaName, out string objectName);

        if (Session?.Login?.Database is not SqlDatabase sqlDatabase)
        {
            string[] fallback = new[] { "*" };
            columnOptions[fullObjectName] = fallback;
            return fallback;
        }

        using SqlConnection connection = CreateSqlConnection(sqlDatabase, "PNWB_Extra.DynamiczneDane.Columns");
        connection.Open();

        List<string> columns = new List<string> { "*" };
        using (SqlCommand command = connection.CreateCommand())
        {
            command.CommandType = CommandType.Text;
            command.CommandText = @"
SELECT C.name
FROM sys.columns C
INNER JOIN sys.objects O ON O.object_id = C.object_id
INNER JOIN sys.schemas S ON S.schema_id = O.schema_id
WHERE S.name = @SchemaName
  AND O.name = @ObjectName
  AND O.type IN ('U', 'V')
ORDER BY C.column_id;";
            command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar, 128) { Value = schemaName });
            command.Parameters.Add(new SqlParameter("@ObjectName", SqlDbType.NVarChar, 128) { Value = objectName });

            using SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                string columnName = reader.IsDBNull(0) ? null : reader.GetString(0);
                if (!string.IsNullOrWhiteSpace(columnName))
                {
                    columns.Add(columnName);
                }
            }
        }

        string[] resolved = columns.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        columnOptions[fullObjectName] = resolved;
        return resolved;
    }

    private static void ParseObjectName(string fullObjectName, out string schemaName, out string objectName)
    {
        string value = (fullObjectName ?? string.Empty).Trim();
        string[] parts = value.Split(new[] { '.' }, 2, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 2)
        {
            schemaName = parts[0];
            objectName = parts[1];
            return;
        }

        schemaName = "dbo";
        objectName = value;
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

    private static object CloneRowObject(object source)
    {
        if (source == null)
        {
            return null;
        }

        Type rowType = source.GetType();
        object clone = Activator.CreateInstance(rowType);
        CopyRowValues(source, clone);
        return clone;
    }

    private static void CopyRowValues(object source, object target)
    {
        if (source == null || target == null)
        {
            return;
        }

        Type rowType = source.GetType();
        foreach (PropertyInfo property in rowType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetMethod == null || property.SetMethod == null)
            {
                continue;
            }

            object value = property.GetValue(source);
            property.SetValue(target, value);
        }
    }

    private void CaptureOriginalRows()
    {
        originalRows.Clear();
        foreach (object row in EnumerateRows())
        {
            originalRows[row] = CaptureRowValues(row);
        }
    }

    private IEnumerable<object> EnumerateRows()
    {
        if (dynamicRows == null)
        {
            yield break;
        }

        foreach (object row in dynamicRows)
        {
            if (row != null)
            {
                yield return row;
            }
        }
    }

    private static Dictionary<string, string> CaptureRowValues(object row)
    {
        Dictionary<string, string> values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (row == null)
        {
            return values;
        }

        foreach (PropertyInfo property in row.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetMethod == null)
            {
                continue;
            }

            string sourceColumn = GetSourceColumnName(property);
            if (string.IsNullOrWhiteSpace(sourceColumn))
            {
                continue;
            }

            object value = property.GetValue(row);
            values[sourceColumn] = value == null ? null : Convert.ToString(value, CultureInfo.CurrentCulture);
        }

        return values;
    }

    private static string GetSourceColumnName(PropertyInfo property)
    {
        CaptionAttribute caption = property.GetCustomAttribute<CaptionAttribute>();
        if (caption != null && !string.IsNullOrWhiteSpace(caption.Text))
        {
            return caption.Text;
        }

        DisplayNameAttribute displayName = property.GetCustomAttribute<DisplayNameAttribute>();
        if (displayName != null && !string.IsNullOrWhiteSpace(displayName.DisplayName))
        {
            return displayName.DisplayName;
        }

        return property.Name;
    }

    private static string GetValue(IReadOnlyDictionary<string, string> values, string key)
    {
        return values != null && values.TryGetValue(key, out string value) ? value : null;
    }
}

internal sealed class DynamiczneDaneViewInfo : ViewInfo
{
    private readonly Func<Type> rowTypeProvider;

    public DynamiczneDaneViewInfo(Func<Type> rowTypeProvider)
    {
        this.rowTypeProvider = rowTypeProvider;
    }

    public override void InitializeViewForm(Context context, DataForm form, bool isSubView = false)
    {
        base.InitializeViewForm(context, form, isSubView);
        GridElement grid = FindGridElement(form, "ListDynamiczneDaneV3")
                           ?? FindGridElement(form, "ListDynamiczneDaneV2")
                           ?? FindFirstGridElement(form);
        if (grid == null)
        {
            return;
        }

        grid.IsToolbarVisible = true;
        grid.NewButton = CollectionButtonState.None;
        grid.EditButton = CollectionButtonState.Auto;
        grid.UpdateButton = CollectionButtonState.None;
        grid.RemoveButton = CollectionButtonState.None;
        grid.EditInPlace = true;
        grid.ForceEditInPlace = true;
        grid.AllowCellSelection = true;
        grid.SortAfterEditInPlace = true;

        grid.Elements.Clear();

        Type rowType = rowTypeProvider?.Invoke();
        if (rowType == null)
        {
            return;
        }

        foreach (PropertyInfo property in rowType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetMethod == null)
            {
                continue;
            }

            string caption = GetCaption(property);
            grid.Elements.Add(new FieldElement
            {
                CaptionHtml = caption,
                EditValue = $"{{{property.Name}}}",
                IsReadOnly = "false",
                Footer = AggregationType.None
            });
        }
    }

    private static GridElement FindGridElement(DataForm form, string gridName)
    {
        if (form?.Elements == null || string.IsNullOrWhiteSpace(gridName))
        {
            return null;
        }

        return form.Elements.Find(e => e is GridElement g && string.Equals(g.Name, gridName, StringComparison.OrdinalIgnoreCase)) as GridElement;
    }

    private static GridElement FindFirstGridElement(DataForm form)
    {
        if (form?.Elements == null)
        {
            return null;
        }

        return form.Elements.Find(e => e is GridElement) as GridElement;
    }

    private static string GetCaption(PropertyInfo property)
    {
        CaptionAttribute caption = property.GetCustomAttribute<CaptionAttribute>();
        if (caption != null && !string.IsNullOrWhiteSpace(caption.Text))
        {
            return caption.TranslatedText;
        }

        DisplayNameAttribute displayName = property.GetCustomAttribute<DisplayNameAttribute>();
        if (displayName != null && !string.IsNullOrWhiteSpace(displayName.DisplayName))
        {
            return displayName.DisplayName;
        }

        return property.Name;
    }
}

public sealed class DynamiczneDaneSelection
{
    public DynamiczneDaneSelection(
        string displayObjectName,
        string schemaName,
        string objectName,
        string columnName,
        bool selectAllColumns,
        bool hasWhereClause,
        string whereColumnName,
        string whereOperator,
        string whereValue)
    {
        DisplayObjectName = displayObjectName;
        SchemaName = schemaName;
        ObjectName = objectName;
        ColumnName = columnName;
        SelectAllColumns = selectAllColumns;
        HasWhereClause = hasWhereClause;
        WhereColumnName = whereColumnName;
        WhereOperator = whereOperator;
        WhereValue = whereValue;
    }

    public string DisplayObjectName { get; }

    public string SchemaName { get; }

    public string ObjectName { get; }

    public string ColumnName { get; }

    public bool SelectAllColumns { get; }

    public bool HasWhereClause { get; }

    public string WhereColumnName { get; }

    public string WhereOperator { get; }

    public string WhereValue { get; }
}

public sealed class DynamicEditedRow
{
    public DynamicEditedRow(
        string databaseName,
        IReadOnlyDictionary<string, string> originalValues,
        IReadOnlyDictionary<string, string> currentValues,
        IReadOnlyList<string> changedColumns)
    {
        DatabaseName = databaseName;
        OriginalValues = originalValues;
        CurrentValues = currentValues;
        ChangedColumns = changedColumns;
    }

    public string DatabaseName { get; }

    public IReadOnlyDictionary<string, string> OriginalValues { get; }

    public IReadOnlyDictionary<string, string> CurrentValues { get; }

    public IReadOnlyList<string> ChangedColumns { get; }
}

internal sealed class ReferenceObjectComparer : IEqualityComparer<object>
{
    public static readonly ReferenceObjectComparer Instance = new ReferenceObjectComparer();

    public new bool Equals(object x, object y)
    {
        return ReferenceEquals(x, y);
    }

    public int GetHashCode(object obj)
    {
        return RuntimeHelpers.GetHashCode(obj);
    }
}

internal sealed class DynamicEmptyRow
{
    [Caption("NazwaFirmy")]
    public string NazwaFirmy { get; set; }
}

internal static class DynamicSqlRowFactory
{
    private static readonly ConcurrentDictionary<string, GeneratedTypeInfo> cache = new ConcurrentDictionary<string, GeneratedTypeInfo>(StringComparer.Ordinal);
    private static readonly AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("PNWB_Extra.DynamicRows"), AssemblyBuilderAccess.Run);
    private static readonly ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule("Main");
    private static int typeCounter;

    public static IList CreateRows(DataTable table, out Type rowType)
    {
        if (table == null || table.Columns.Count == 0)
        {
            rowType = typeof(DynamicEmptyRow);
            Type emptyListType = typeof(BindingList<>).MakeGenericType(rowType);
            return (IList)Activator.CreateInstance(emptyListType);
        }

        string[] columnNames = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
        string key = string.Join("|", columnNames);
        GeneratedTypeInfo typeInfo = cache.GetOrAdd(key, _ => CreateTypeInfo(columnNames));

        Type listType = typeof(BindingList<>).MakeGenericType(typeInfo.RowType);
        IList rows = (IList)Activator.CreateInstance(listType);
        foreach (DataRow row in table.Rows)
        {
            object instance = Activator.CreateInstance(typeInfo.RowType);
            for (int i = 0; i < typeInfo.ColumnNames.Length; i++)
            {
                object value = row[typeInfo.ColumnNames[i]];
                string textValue = value == null || value == DBNull.Value
                    ? null
                    : Convert.ToString(value, CultureInfo.CurrentCulture);
                typeInfo.Setters[i](instance, textValue);
            }

            rows.Add(instance);
        }

        rowType = typeInfo.RowType;
        return rows;
    }

    private static GeneratedTypeInfo CreateTypeInfo(string[] columnNames)
    {
        string typeName = $"DynamicSqlRow_{System.Threading.Interlocked.Increment(ref typeCounter)}";
        TypeBuilder typeBuilder = moduleBuilder.DefineType(
            typeName,
            TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.Sealed);

        HashSet<string> usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        string[] propertyNames = new string[columnNames.Length];
        PropertyInfo[] runtimeProperties = new PropertyInfo[columnNames.Length];

        ConstructorInfo captionCtor = typeof(CaptionAttribute).GetConstructor(new[] { typeof(string) });
        ConstructorInfo displayCtor = typeof(DisplayNameAttribute).GetConstructor(new[] { typeof(string) });
        ConstructorInfo readOnlyCtor = typeof(ReadOnlyAttribute).GetConstructor(new[] { typeof(bool) });

        for (int i = 0; i < columnNames.Length; i++)
        {
            string originalColumnName = columnNames[i];
            string propertyName = SanitizePropertyName(originalColumnName, usedNames);
            propertyNames[i] = propertyName;

            FieldBuilder field = typeBuilder.DefineField($"_{propertyName}", typeof(string), FieldAttributes.Private);
            PropertyBuilder property = typeBuilder.DefineProperty(propertyName, PropertyAttributes.None, typeof(string), Type.EmptyTypes);

            MethodBuilder getter = typeBuilder.DefineMethod(
                $"get_{propertyName}",
                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                typeof(string),
                Type.EmptyTypes);
            ILGenerator getterIl = getter.GetILGenerator();
            getterIl.Emit(OpCodes.Ldarg_0);
            getterIl.Emit(OpCodes.Ldfld, field);
            getterIl.Emit(OpCodes.Ret);

            MethodBuilder setter = typeBuilder.DefineMethod(
                $"set_{propertyName}",
                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                null,
                new[] { typeof(string) });
            ILGenerator setterIl = setter.GetILGenerator();
            setterIl.Emit(OpCodes.Ldarg_0);
            setterIl.Emit(OpCodes.Ldarg_1);
            setterIl.Emit(OpCodes.Stfld, field);
            setterIl.Emit(OpCodes.Ret);

            property.SetGetMethod(getter);
            property.SetSetMethod(setter);

            if (captionCtor != null)
            {
                property.SetCustomAttribute(new CustomAttributeBuilder(captionCtor, new object[] { originalColumnName }));
            }

            if (displayCtor != null)
            {
                property.SetCustomAttribute(new CustomAttributeBuilder(displayCtor, new object[] { originalColumnName }));
            }

            if (readOnlyCtor != null)
            {
                property.SetCustomAttribute(new CustomAttributeBuilder(readOnlyCtor, new object[] { false }));
            }
        }

        Type runtimeType = typeBuilder.CreateType();
        for (int i = 0; i < propertyNames.Length; i++)
        {
            runtimeProperties[i] = runtimeType.GetProperty(propertyNames[i]);
        }

        Action<object, string>[] setters = runtimeProperties.Select(BuildSetter).ToArray();
        return new GeneratedTypeInfo(runtimeType, columnNames, setters);
    }

    private static Action<object, string> BuildSetter(PropertyInfo property)
    {
        ParameterExpression target = Expression.Parameter(typeof(object), "target");
        ParameterExpression value = Expression.Parameter(typeof(string), "value");
        UnaryExpression castTarget = Expression.Convert(target, property.DeclaringType);
        MethodCallExpression body = Expression.Call(castTarget, property.SetMethod, value);
        return Expression.Lambda<Action<object, string>>(body, target, value).Compile();
    }

    private static string SanitizePropertyName(string original, ISet<string> used)
    {
        string input = string.IsNullOrWhiteSpace(original) ? "Column" : original.Trim();
        char[] chars = input.Select(ch => char.IsLetterOrDigit(ch) ? ch : '_').ToArray();
        string baseName = new string(chars);
        if (baseName.Length == 0)
        {
            baseName = "Column";
        }

        if (char.IsDigit(baseName[0]))
        {
            baseName = "_" + baseName;
        }

        string candidate = baseName;
        int suffix = 2;
        while (!used.Add(candidate))
        {
            candidate = $"{baseName}_{suffix++}";
        }

        return candidate;
    }

    private sealed class GeneratedTypeInfo
    {
        public GeneratedTypeInfo(Type rowType, string[] columnNames, Action<object, string>[] setters)
        {
            RowType = rowType;
            ColumnNames = columnNames;
            Setters = setters;
        }

        public Type RowType { get; }

        public string[] ColumnNames { get; }

        public Action<object, string>[] Setters { get; }
    }
}
