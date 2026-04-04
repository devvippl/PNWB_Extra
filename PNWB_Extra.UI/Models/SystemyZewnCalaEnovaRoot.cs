using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using Microsoft.Data.SqlClient;
using Soneta.Business.App;
using Soneta.Business;
using Soneta.Business.UI;
using Soneta.Types;

namespace PNWB_Extra.UI.Models;

public sealed class SystemyZewnCalaEnovaRoot : ISessionable
{
    private readonly List<SystemyZewnCalaEnovaRow> items = new List<SystemyZewnCalaEnovaRow>();
    private readonly BindingList<SystemyZewnCalaEnovaRow> visibleItems = new BindingList<SystemyZewnCalaEnovaRow>();
    private readonly Dictionary<string, string> originalState = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    private ViewInfo viewInfo;
    private string symbolFilter;
    private BlokadaFilterOption blokadaFilter = BlokadaFilterOption.Wszystkie;

    [Context]
    public Session Session { get; set; }

    [Caption("Symbol")]
    public string SymbolFilter
    {
        get => symbolFilter;
        set
        {
            if (symbolFilter == value)
            {
                return;
            }

            symbolFilter = value;
            RefreshVisibleItems();
            Session?.InvokeChanged();
        }
    }

    [Caption("Blokada")]
    public BlokadaFilterOption BlokadaFilter
    {
        get => blokadaFilter;
        set
        {
            if (blokadaFilter == value)
            {
                return;
            }

            blokadaFilter = value;
            RefreshVisibleItems();
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

            viewInfo = new SystemyZewnCalaEnovaViewInfo
            {
                NonViewCollection = true,
                RowType = typeof(SystemyZewnCalaEnovaRow)
            };
            viewInfo.CreateView += (_, args) =>
            {
                args.DataSource = visibleItems;
            };
            viewInfo.Action += (_, args) =>
            {
                if (args.Action == ActionEventArgs.Actions.Edit)
                {
                    args.Cancel = true;
                    SystemyZewnCalaEnovaRow current = args.OriginalFocusedData as SystemyZewnCalaEnovaRow
                                                     ?? args.FocusedData as SystemyZewnCalaEnovaRow;
                    if (current == null)
                    {
                        return;
                    }

                    SystemyZewnCalaEnovaRow editor = current.Clone();
                    args.FocusedData = new FormActionResult
                    {
                        EditValue = editor,
                        CommittedHandler = _ =>
                        {
                            current.CopyFrom(editor);
                            RefreshVisibleItems();
                            Session?.InvokeChanged();
                            return null;
                        }
                    };
                    return;
                }

                if (args.Action != ActionEventArgs.Actions.Update)
                {
                    return;
                }
                args.Cancel = true;

                IEnumerable<SystemyZewnCalaEnovaRow> rows = (args.SelectedData?.OfType<SystemyZewnCalaEnovaRow>().Any() ?? false)
                    ? args.SelectedData.OfType<SystemyZewnCalaEnovaRow>()
                    : (args.FocusedData is SystemyZewnCalaEnovaRow single ? new[] { single } : Enumerable.Empty<SystemyZewnCalaEnovaRow>());

                // Zmiany po Ctrl+Shift+H / edycji w siatce zostają lokalnie w UI.
                // Do SQL zapisujemy dopiero przez akcję "Zapisz zmiany".
                _ = rows.ToArray();
                RefreshVisibleItems();
                Session?.InvokeChanged();
            };
            return viewInfo;
        }
    }

    public void ReplaceRows(IEnumerable<SystemyZewnCalaEnovaRow> rows)
    {
        items.Clear();
        originalState.Clear();
        if (rows == null)
        {
            RefreshVisibleItems();
            return;
        }

        items.AddRange(rows);
        foreach (SystemyZewnCalaEnovaRow row in items)
        {
            originalState[BuildRowKey(row)] = BuildEditableFingerprint(row);
        }
        RefreshVisibleItems();
        Session?.InvokeChanged();
    }

    private IEnumerable<SystemyZewnCalaEnovaRow> FilteredItems()
    {
        IEnumerable<SystemyZewnCalaEnovaRow> query = items;

        if (!string.IsNullOrWhiteSpace(SymbolFilter))
        {
            query = query.Where(r => !string.IsNullOrWhiteSpace(r.Symbol)
                                     && r.Symbol.Contains(SymbolFilter, System.StringComparison.OrdinalIgnoreCase));
        }

        query = BlokadaFilter switch
        {
            BlokadaFilterOption.TylkoZablokowane => query.Where(r => r.Blokada == true),
            BlokadaFilterOption.TylkoOdblokowane => query.Where(r => r.Blokada == false),
            _ => query
        };

        return query;
    }

    private void RefreshVisibleItems()
    {
        SystemyZewnCalaEnovaRow[] filtered = FilteredItems().ToArray();
        visibleItems.Clear();
        foreach (SystemyZewnCalaEnovaRow row in filtered)
        {
            visibleItems.Add(row);
        }
    }

    private void PersistEditedRows(IEnumerable<SystemyZewnCalaEnovaRow> rows)
    {
        SystemyZewnCalaEnovaRow[] toSave = rows.Where(r => r != null && !string.IsNullOrWhiteSpace(r.NazwaFirmy)).ToArray();
        if (toSave.Length == 0)
        {
            return;
        }
        Log log = new Log("Systemy zewnętrzne cała enova", open: true);
        log.WriteLine("PersistEditedRows: rows={0}", toSave.Length);

        if (Session?.Login?.Database is not SqlDatabase sqlDatabase)
        {
            throw new InvalidOperationException("Bieżąca baza enova nie jest bazą SQL.");
        }

        using SqlConnection connection = CreateSqlConnection(sqlDatabase, "PNWB_Extra.SystemyZewnCalaEnova.Save");
        connection.Open();

        foreach (SystemyZewnCalaEnovaRow row in toSave)
        {
            SaveSingleRow(connection, row);
            originalState[BuildRowKey(row)] = BuildEditableFingerprint(row);
        }
    }

    public void SaveAllRows()
    {
        SystemyZewnCalaEnovaRow[] changed = items.Where(IsChanged).ToArray();
        PersistEditedRows(changed);
        RefreshVisibleItems();
        Session?.InvokeChanged();
    }

    private bool IsChanged(SystemyZewnCalaEnovaRow row)
    {
        string key = BuildRowKey(row);
        string current = BuildEditableFingerprint(row);
        return !originalState.TryGetValue(key, out string initial)
               || !string.Equals(initial, current, StringComparison.Ordinal);
    }

    private static string BuildRowKey(SystemyZewnCalaEnovaRow row)
    {
        return $"{row?.NazwaFirmy}|{row?.Guid}|{row?.ID}";
    }

    private static string BuildEditableFingerprint(SystemyZewnCalaEnovaRow row)
    {
        if (row == null)
        {
            return string.Empty;
        }

        return string.Join("|",
            row.Typ ?? string.Empty,
            row.Symbol ?? string.Empty,
            row.Opis ?? string.Empty,
            row.Blokada?.ToString() ?? string.Empty,
            row.Kontrahent?.ToString() ?? string.Empty,
            row.KontrahentType ?? string.Empty,
            row.Domyslny?.ToString() ?? string.Empty);
    }

    private static void SaveSingleRow(SqlConnection connection, SystemyZewnCalaEnovaRow row)
    {
        string dbQuoted = QuoteSqlIdentifier(row.NazwaFirmy);
        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = $@"
DECLARE @updated TABLE (Stamp varbinary(8));

UPDATE S
SET S.Typ = @Typ,
    S.Symbol = @Symbol,
    S.Opis = @Opis,
    S.Blokada = @Blokada,
    S.Kontrahent = @Kontrahent,
    S.KontrahentType = @KontrahentType,
    S.Domyslny = @Domyslny
OUTPUT inserted.Stamp INTO @updated(Stamp)
FROM {dbQuoted}.dbo.SystemyZewn S
WHERE (TRY_CONVERT(uniqueidentifier, @Guid) IS NOT NULL AND S.Guid = TRY_CONVERT(uniqueidentifier, @Guid)
       OR (TRY_CONVERT(uniqueidentifier, @Guid) IS NULL AND @ID IS NOT NULL AND S.ID = @ID))
  AND (@OldStamp IS NULL OR S.Stamp = @OldStamp);

SELECT TOP (1) CONVERT(varchar(18), Stamp, 1) AS StampHex
FROM @updated;";

        command.Parameters.Add(new SqlParameter("@Typ", SqlDbType.NVarChar, 255) { Value = DbValue(row.Typ) });
        command.Parameters.Add(new SqlParameter("@Symbol", SqlDbType.NVarChar, 255) { Value = DbValue(row.Symbol) });
        command.Parameters.Add(new SqlParameter("@Opis", SqlDbType.NVarChar) { Value = DbValue(row.Opis) });
        command.Parameters.Add(new SqlParameter("@Blokada", SqlDbType.Bit) { Value = DbValue(row.Blokada) });
        command.Parameters.Add(new SqlParameter("@Kontrahent", SqlDbType.Int) { Value = DbValue(row.Kontrahent) });
        command.Parameters.Add(new SqlParameter("@KontrahentType", SqlDbType.NVarChar, 255) { Value = DbValue(row.KontrahentType) });
        command.Parameters.Add(new SqlParameter("@Domyslny", SqlDbType.Bit) { Value = DbValue(row.Domyslny) });
        command.Parameters.Add(new SqlParameter("@Guid", SqlDbType.NVarChar, 64) { Value = DbValue(row.Guid) });
        command.Parameters.Add(new SqlParameter("@ID", SqlDbType.Int) { Value = DbValue(row.ID) });
        command.Parameters.Add(new SqlParameter("@OldStamp", SqlDbType.Binary, 8) { Value = DbValue(ParseHexStamp(row.Stamp)) });

        object newStamp = command.ExecuteScalar();
        if (newStamp == null || newStamp == DBNull.Value)
        {
            throw new InvalidOperationException($"Nie zapisano rekordu SystemyZewn: baza='{row.NazwaFirmy}', ID='{row.ID}', Guid='{row.Guid}'.");
        }

        row.Stamp = newStamp.ToString();
    }

    private static SqlConnection CreateSqlConnection(SqlDatabase sqlDatabase, string appName)
    {
        return PnwbSqlConnectionFactory.Create(sqlDatabase, appName);
    }

    private static object DbValue(object value)
    {
        return value ?? DBNull.Value;
    }

    private static byte[] ParseHexStamp(string stampHex)
    {
        if (string.IsNullOrWhiteSpace(stampHex))
        {
            return null;
        }

        string hex = stampHex.StartsWith("0x", System.StringComparison.OrdinalIgnoreCase)
            ? stampHex.Substring(2)
            : stampHex;

        if (hex.Length == 0 || (hex.Length % 2) != 0)
        {
            return null;
        }

        byte[] bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = byte.Parse(hex.Substring(i * 2, 2), System.Globalization.NumberStyles.HexNumber);
        }

        return bytes;
    }

    private static string QuoteSqlIdentifier(string value)
    {
        return $"[{value.Replace("]", "]]")}]";
    }
}

internal sealed class SystemyZewnCalaEnovaViewInfo : ViewInfo
{
    public override void InitializeViewForm(Context context, DataForm form, bool isSubView = false)
    {
        base.InitializeViewForm(context, form, isSubView);
        GridElement grid = FindGridElement(form, "ListSystemyZewnV3")
                           ?? FindGridElement(form, "ListSystemyZewnV2")
                           ?? FindFirstGridElement(form);
        if (grid == null)
        {
            return;
        }

        grid.IsToolbarVisible = true;
        grid.NewButton = CollectionButtonState.None;
        grid.RemoveButton = CollectionButtonState.None;
        grid.EditButton = CollectionButtonState.Auto;
        grid.UpdateButton = CollectionButtonState.None;
        grid.EditInPlace = true;
        grid.ForceEditInPlace = true;
        grid.AllowCellSelection = true;
        grid.SortAfterEditInPlace = true;
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
}

public enum BlokadaFilterOption
{
    [Caption("Wszystkie")]
    Wszystkie = 0,
    [Caption("Zablokowane")]
    TylkoZablokowane = 1,
    [Caption("Odblokowane")]
    TylkoOdblokowane = 2
}

public sealed class SystemyZewnCalaEnovaRow
{
    [Caption("Nazwa firmy")]
    [ReadOnly(true)]
    public string NazwaFirmy { get; set; }

    [ReadOnly(true)]
    public int? ID { get; set; }

    [ReadOnly(true)]
    public string Guid { get; set; }

    [ReadOnly(false)]
    public string Typ { get; set; }

    [ReadOnly(false)]
    public string Symbol { get; set; }

    [ReadOnly(false)]
    public string Opis { get; set; }

    [ReadOnly(false)]
    public bool? Blokada { get; set; }

    [ReadOnly(true)]
    public string Stamp { get; set; }

    [ReadOnly(false)]
    public int? Kontrahent { get; set; }

    [ReadOnly(false)]
    public string KontrahentType { get; set; }

    [ReadOnly(false)]
    public bool? Domyslny { get; set; }

    public SystemyZewnCalaEnovaRow Clone()
    {
        return new SystemyZewnCalaEnovaRow
        {
            NazwaFirmy = NazwaFirmy,
            ID = ID,
            Guid = Guid,
            Typ = Typ,
            Symbol = Symbol,
            Opis = Opis,
            Blokada = Blokada,
            Stamp = Stamp,
            Kontrahent = Kontrahent,
            KontrahentType = KontrahentType,
            Domyslny = Domyslny
        };
    }

    public void CopyFrom(SystemyZewnCalaEnovaRow source)
    {
        if (source == null)
        {
            return;
        }

        Typ = source.Typ;
        Symbol = source.Symbol;
        Opis = source.Opis;
        Blokada = source.Blokada;
        Kontrahent = source.Kontrahent;
        KontrahentType = source.KontrahentType;
        Domyslny = source.Domyslny;
    }
}
