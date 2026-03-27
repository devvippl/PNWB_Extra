using System;
using System.Linq;
using Soneta.Business;
using Soneta.Business.Db;
using Soneta.Tools;
using PNWB_Extra.UI.Workers;

namespace PNWB_Extra.UI.ViewInfos;

public sealed class AnalizyBazDanychExtraViewInfo : ViewInfo
{
    public AnalizyBazDanychExtraViewInfo()
    {
        MakeConfigSession = (ThreeStateBoolean)2;
        ResourceName = "DBItemsView";
        CreateView += AnalizyBazDanychExtraViewInfo_CreateView;
        InitContext += AnalizyBazDanychExtraViewInfo_InitContext;
        Action += AnalizyBazDanychExtraViewInfo_Action;
    }

    private static void AnalizyBazDanychExtraViewInfo_InitContext(object sender, ContextEventArgs args)
    {
        args.Context.TryAdd(() => new DBItems.Params(args.Context));
        args.Context.TryAdd(() => new AnalizyBazDanychExtraContextMarker());
    }

    private static void AnalizyBazDanychExtraViewInfo_CreateView(object sender, CreateViewEventArgs args)
    {
        DBItems.Params parameters = (DBItems.Params)args.Context[typeof(DBItems.Params)];
        args.View = parameters.CreateView();
        args.View.AllowNew = false;
        args.View.AllowRemove = false;
        foreach (DBItem item in args.View.Cast<DBItem>())
        {
            DBItemsObliczExtraWorker.InitializeCalculatedPlaceholders(item);
        }
    }

    private static void AnalizyBazDanychExtraViewInfo_Action(object sender, ActionEventArgs e)
    {
        if ((int)e.Action != 2)
        {
            return;
        }

        DBItem focused = e.FocusedData as DBItem;
        if (focused == null)
        {
            return;
        }

        Session session = focused.Session.Login.CreateSession(readOnly: false, config: false, CoreTools.TranslateIgnore("Open DBItem for analise extra"));
        Context clonedContext = e.Context.Clone(session);
        DBItem dbItemInClonedSession = session.Get<DBItem>(focused);
        clonedContext.Set(dbItemInClonedSession);
        DBItems.Params parameters = (DBItems.Params)clonedContext[typeof(DBItems.Params)];
        dbItemInClonedSession.CalculatedProperties.Calculate(parameters.Aktualny, parameters.Okres);
        e.FocusedData = dbItemInClonedSession;
    }
}

public sealed class AnalizyBazDanychExtraContextMarker
{
}
