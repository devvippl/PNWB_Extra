using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Soneta.Business;
using Soneta.Business.Db;
using Soneta.Business.UI;
using Soneta.Types;

namespace PNWB_Extra.UI.Models;

public sealed class UnicodeCalaEnovaRoot : ISessionable
{
    private readonly List<UnicodeCalaEnovaRow> items = new List<UnicodeCalaEnovaRow>();
    private readonly BindingList<UnicodeCalaEnovaRow> visibleItems = new BindingList<UnicodeCalaEnovaRow>();
    private ViewInfo viewInfo;
    private bool rowsInitialized;

    [Context]
    public Session Session { get; set; }

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
                RowType = typeof(UnicodeCalaEnovaRow)
            };
            viewInfo.CreateView += (_, args) =>
            {
                EnsureRowsLoaded();
                args.DataSource = visibleItems;
            };
            return viewInfo;
        }
    }

    public void EnsureRowsLoaded()
    {
        if (rowsInitialized || Session == null)
        {
            return;
        }

        rowsInitialized = true;
        BusinessModule module = BusinessModule.GetInstance(Session);
        string[] names = module.DBItems
            .Cast<DBItem>()
            .Select(i => i?.Name)
            .Where(n => !string.IsNullOrWhiteSpace(n) && n != ".")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        EnsureRowsLoaded(names);
    }

    public void EnsureRowsLoaded(IEnumerable<string> databaseNames)
    {
        string[] names = (databaseNames ?? Array.Empty<string>())
            .Where(n => !string.IsNullOrWhiteSpace(n) && n != ".")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        Dictionary<string, UnicodeCalaEnovaRow> existing = items
            .Where(r => r != null && !string.IsNullOrWhiteSpace(r.NazwaFirmy))
            .ToDictionary(r => r.NazwaFirmy, StringComparer.OrdinalIgnoreCase);

        items.Clear();
        foreach (string name in names)
        {
            if (existing.TryGetValue(name, out UnicodeCalaEnovaRow current))
            {
                items.Add(current);
                continue;
            }

            items.Add(new UnicodeCalaEnovaRow
            {
                NazwaFirmy = name,
                DataType = "Brak danych",
                CzyUnicode = null,
                ErrorMessage = string.Empty
            });
        }

        rowsInitialized = true;
        RefreshVisibleItems();
        Session?.InvokeChanged();
    }

    public UnicodeCalaEnovaRow[] GetAllRows() => items.Where(r => r != null && !string.IsNullOrWhiteSpace(r.NazwaFirmy)).ToArray();

    public void ApplyStatuses(IReadOnlyDictionary<string, (string DataType, bool? IsUnicode, string ErrorMessage)> statuses)
    {
        if (statuses == null)
        {
            return;
        }

        foreach (UnicodeCalaEnovaRow row in items)
        {
            if (row == null || string.IsNullOrWhiteSpace(row.NazwaFirmy))
            {
                continue;
            }

            if (!statuses.TryGetValue(row.NazwaFirmy, out (string DataType, bool? IsUnicode, string ErrorMessage) status))
            {
                continue;
            }

            row.DataType = string.IsNullOrWhiteSpace(status.DataType) ? "Brak" : status.DataType;
            row.CzyUnicode = status.IsUnicode;
            row.ErrorMessage = status.ErrorMessage ?? string.Empty;
        }

        RefreshVisibleItems();
        Session?.InvokeChanged();
    }

    private void RefreshVisibleItems()
    {
        visibleItems.Clear();
        foreach (UnicodeCalaEnovaRow row in items)
        {
            visibleItems.Add(row);
        }
    }
}

public sealed class UnicodeCalaEnovaRow
{
    [Caption("Nazwa firmy")]
    [ReadOnly(true)]
    public string NazwaFirmy { get; set; }

    [Caption("Typ kolumny SystemInfos.Value")]
    [ReadOnly(true)]
    public string DataType { get; set; }

    [Caption("Teksty Unicode")]
    [ReadOnly(true)]
    public bool? CzyUnicode { get; set; }

    [Caption("Informacja")]
    [ReadOnly(true)]
    public string ErrorMessage { get; set; }
}
