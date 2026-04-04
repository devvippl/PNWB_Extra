using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Soneta.Business;
using Soneta.Business.Db;
using Soneta.Business.UI;
using Soneta.Types;

namespace PNWB_Extra.UI.Models;

public sealed class OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRoot : ISessionable
{
    private readonly List<OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow> items = new List<OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow>();
    private readonly BindingList<OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow> visibleItems = new BindingList<OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow>();
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
                RowType = typeof(OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow)
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

        Dictionary<string, OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow> existing = items
            .Where(r => r != null && !string.IsNullOrWhiteSpace(r.NazwaFirmy))
            .ToDictionary(r => r.NazwaFirmy, StringComparer.OrdinalIgnoreCase);

        items.Clear();
        foreach (string name in names)
        {
            if (existing.TryGetValue(name, out OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow current))
            {
                items.Add(current);
                continue;
            }

            items.Add(new OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow
            {
                NazwaFirmy = name,
                RozmiarBazyMB = null,
                RozmiarLogMB = null,
                ErrorMessage = string.Empty
            });
        }

        rowsInitialized = true;
        RefreshVisibleItems();
        Session?.InvokeChanged();
    }

    public OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow[] GetAllRows() => items.Where(r => r != null && !string.IsNullOrWhiteSpace(r.NazwaFirmy)).ToArray();

    public void ApplySizes(IReadOnlyDictionary<string, (decimal? DataSizeMb, decimal? LogSizeMb, string ErrorMessage)> sizes)
    {
        if (sizes == null)
        {
            return;
        }

        foreach (OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow row in items)
        {
            if (row == null || string.IsNullOrWhiteSpace(row.NazwaFirmy))
            {
                continue;
            }

            if (!sizes.TryGetValue(row.NazwaFirmy, out (decimal? DataSizeMb, decimal? LogSizeMb, string ErrorMessage) size))
            {
                continue;
            }

            row.RozmiarBazyMB = size.DataSizeMb;
            row.RozmiarLogMB = size.LogSizeMb;
            row.ErrorMessage = size.ErrorMessage ?? string.Empty;
        }

        RefreshVisibleItems();
        Session?.InvokeChanged();
    }

    private void RefreshVisibleItems()
    {
        visibleItems.Clear();
        foreach (OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow row in items)
        {
            visibleItems.Add(row);
        }
    }
}

public sealed class OptymalizacjaRozmiaruBazyMsSqlCalaEnovaRow
{
    [Caption("Nazwa firmy")]
    [ReadOnly(true)]
    public string NazwaFirmy { get; set; }

    [Caption("Rozmiar bazy [MB]")]
    [ReadOnly(true)]
    public decimal? RozmiarBazyMB { get; set; }

    [Caption("Rozmiar log [MB]")]
    [ReadOnly(true)]
    public decimal? RozmiarLogMB { get; set; }

    [Caption("Informacja")]
    [ReadOnly(true)]
    public string ErrorMessage { get; set; }
}
