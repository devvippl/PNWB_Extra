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

public sealed class TokenyCalaEnovaRoot : ISessionable
{
    private readonly List<TokenCalaEnovaRow> items = new List<TokenCalaEnovaRow>();
    private readonly BindingList<TokenCalaEnovaRow> visibleItems = new BindingList<TokenCalaEnovaRow>();
    private readonly Dictionary<string, string> originalState = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    private ViewInfo viewInfo;

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
                RowType = typeof(TokenCalaEnovaRow)
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
                    TokenCalaEnovaRow current = args.OriginalFocusedData as TokenCalaEnovaRow
                                                ?? args.FocusedData as TokenCalaEnovaRow;
                    if (current == null)
                    {
                        return;
                    }

                    TokenCalaEnovaRow editor = current.Clone();
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

                IEnumerable<TokenCalaEnovaRow> rows = (args.SelectedData?.OfType<TokenCalaEnovaRow>().Any() ?? false)
                    ? args.SelectedData.OfType<TokenCalaEnovaRow>()
                    : (args.FocusedData is TokenCalaEnovaRow single ? new[] { single } : Enumerable.Empty<TokenCalaEnovaRow>());

                // Zmiany po Ctrl+Shift+H / edycji w siatce zostają lokalnie w UI.
                // Do SQL zapisujemy dopiero przez akcję "Zapisz zmiany".
                _ = rows.ToArray();
                RefreshVisibleItems();
                Session?.InvokeChanged();
            };
            return viewInfo;
        }
    }

    public void ReplaceRows(IEnumerable<TokenCalaEnovaRow> rows)
    {
        items.Clear();
        originalState.Clear();
        if (rows != null)
        {
            items.AddRange(rows);
            foreach (TokenCalaEnovaRow row in items)
            {
                originalState[BuildRowKey(row)] = BuildEditableFingerprint(row);
            }
        }
        RefreshVisibleItems();

        Session?.InvokeChanged();
    }

    private void RefreshVisibleItems()
    {
        visibleItems.Clear();
        foreach (TokenCalaEnovaRow row in items)
        {
            visibleItems.Add(row);
        }
    }

    private void PersistEditedRows(IEnumerable<TokenCalaEnovaRow> rows)
    {
        TokenCalaEnovaRow[] toSave = rows.Where(r => r != null && !string.IsNullOrWhiteSpace(r.NazwaFirmy)).ToArray();
        if (toSave.Length == 0)
        {
            return;
        }
        Log log = new Log("Tokeny cała enova", open: true);
        log.WriteLine("PersistEditedRows: rows={0}", toSave.Length);

        if (Session?.Login?.Database is not SqlDatabase sqlDatabase)
        {
            throw new InvalidOperationException("Bieżąca baza enova nie jest bazą SQL.");
        }

        using SqlConnection connection = CreateSqlConnection(sqlDatabase, "PNWB_Extra.TokenyCalaEnova.Save");
        connection.Open();

        foreach (TokenCalaEnovaRow row in toSave)
        {
            SaveSingleRow(connection, row);
            originalState[BuildRowKey(row)] = BuildEditableFingerprint(row);
        }
    }

    public void SaveAllRows()
    {
        TokenCalaEnovaRow[] changed = items.Where(IsChanged).ToArray();
        PersistEditedRows(changed);
        RefreshVisibleItems();
        Session?.InvokeChanged();
    }

    private bool IsChanged(TokenCalaEnovaRow row)
    {
        string key = BuildRowKey(row);
        string current = BuildEditableFingerprint(row);
        return !originalState.TryGetValue(key, out string initial)
               || !string.Equals(initial, current, StringComparison.Ordinal);
    }

    private static string BuildRowKey(TokenCalaEnovaRow row)
    {
        return $"{row?.NazwaFirmy}|{row?.Guid}|{row?.ID}";
    }

    private static string BuildEditableFingerprint(TokenCalaEnovaRow row)
    {
        if (row == null)
        {
            return string.Empty;
        }

        return string.Join("|",
            row.SystemZewn?.ToString() ?? string.Empty,
            row.Nazwa ?? string.Empty,
            row.Token ?? string.Empty,
            row.Pobieranie?.ToString() ?? string.Empty,
            row.Wysylanie?.ToString() ?? string.Empty,
            row.OperatorzyGuids ?? string.Empty,
            row.RefreshTokenValidUntil ?? string.Empty,
            row.RefreshTokenRequestRefNumber ?? string.Empty,
            row.WersjaAPI ?? string.Empty,
            row.Rodzaj?.ToString() ?? string.Empty,
            row.Przeznaczenie?.ToString() ?? string.Empty);
    }

    private static void SaveSingleRow(SqlConnection connection, TokenCalaEnovaRow row)
    {
        string dbQuoted = QuoteSqlIdentifier(row.NazwaFirmy);
        using SqlCommand command = connection.CreateCommand();
        command.CommandType = CommandType.Text;
        command.CommandText = $@"
DECLARE @updated TABLE (Stamp varbinary(8));

UPDATE T
SET T.SystemZewn = @SystemZewn,
    T.Nazwa = @Nazwa,
    T.Token = @Token,
    T.Pobieranie = @Pobieranie,
    T.Wysylanie = @Wysylanie,
    T.OperatorzyGuids = @OperatorzyGuids,
    T.RefreshTokenValidUntil = TRY_CONVERT(datetime2, @RefreshTokenValidUntil, 121),
    T.RefreshTokenRequestRefNumber = @RefreshTokenRequestRefNumber,
    T.WersjaAPI = @WersjaAPI,
    T.Rodzaj = @Rodzaj,
    T.Przeznaczenie = @Przeznaczenie
OUTPUT inserted.Stamp INTO @updated(Stamp)
FROM {dbQuoted}.dbo.SysZewTokeny T
WHERE (TRY_CONVERT(uniqueidentifier, @Guid) IS NOT NULL AND T.Guid = TRY_CONVERT(uniqueidentifier, @Guid)
       OR (TRY_CONVERT(uniqueidentifier, @Guid) IS NULL AND @ID IS NOT NULL AND T.ID = @ID))
  AND (@OldStamp IS NULL OR T.Stamp = @OldStamp);

SELECT TOP (1) CONVERT(varchar(18), Stamp, 1) AS StampHex
FROM @updated;";

        command.Parameters.Add(new SqlParameter("@SystemZewn", SqlDbType.Int) { Value = DbValue(row.SystemZewn) });
        command.Parameters.Add(new SqlParameter("@Nazwa", SqlDbType.NVarChar, 255) { Value = DbValue(row.Nazwa) });
        command.Parameters.Add(new SqlParameter("@Token", SqlDbType.NVarChar) { Value = DbValue(row.Token) });
        command.Parameters.Add(new SqlParameter("@Pobieranie", SqlDbType.Bit) { Value = DbValue(row.Pobieranie) });
        command.Parameters.Add(new SqlParameter("@Wysylanie", SqlDbType.Bit) { Value = DbValue(row.Wysylanie) });
        command.Parameters.Add(new SqlParameter("@OperatorzyGuids", SqlDbType.NVarChar) { Value = DbValue(row.OperatorzyGuids) });
        command.Parameters.Add(new SqlParameter("@RefreshTokenValidUntil", SqlDbType.NVarChar, 50) { Value = DbValue(row.RefreshTokenValidUntil) });
        command.Parameters.Add(new SqlParameter("@RefreshTokenRequestRefNumber", SqlDbType.NVarChar, 255) { Value = DbValue(row.RefreshTokenRequestRefNumber) });
        command.Parameters.Add(new SqlParameter("@WersjaAPI", SqlDbType.NVarChar, 64) { Value = DbValue(row.WersjaAPI) });
        command.Parameters.Add(new SqlParameter("@Rodzaj", SqlDbType.Int) { Value = DbValue(row.Rodzaj) });
        command.Parameters.Add(new SqlParameter("@Przeznaczenie", SqlDbType.Int) { Value = DbValue(row.Przeznaczenie) });
        command.Parameters.Add(new SqlParameter("@Guid", SqlDbType.NVarChar, 64) { Value = DbValue(row.Guid) });
        command.Parameters.Add(new SqlParameter("@ID", SqlDbType.Int) { Value = DbValue(row.ID) });
        command.Parameters.Add(new SqlParameter("@OldStamp", SqlDbType.Binary, 8) { Value = DbValue(ParseHexStamp(row.Stamp)) });

        object newStamp = command.ExecuteScalar();
        if (newStamp == null || newStamp == DBNull.Value)
        {
            throw new InvalidOperationException($"Nie zapisano rekordu SysZewTokeny: baza='{row.NazwaFirmy}', ID='{row.ID}', Guid='{row.Guid}'.");
        }

        row.Stamp = newStamp.ToString();
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

public sealed class TokenCalaEnovaRow
{
    [Caption("Nazwa firmy")]
    [ReadOnly(true)]
    public string NazwaFirmy { get; set; }

    [ReadOnly(true)]
    public int? ID { get; set; }

    [ReadOnly(true)]
    public string Guid { get; set; }

    public int? SystemZewn { get; set; }

    public string Nazwa { get; set; }

    public string Token { get; set; }

    public bool? Pobieranie { get; set; }

    public bool? Wysylanie { get; set; }

    [ReadOnly(true)]
    public string Stamp { get; set; }

    public string OperatorzyGuids { get; set; }

    public string RefreshTokenValidUntil { get; set; }

    [ReadOnly(true)]
    public string RefreshTokenValue { get; set; }

    public string RefreshTokenRequestRefNumber { get; set; }

    [ReadOnly(true)]
    public string RefreshTokenRequestTempTokenValue { get; set; }

    public string WersjaAPI { get; set; }

    public int? Rodzaj { get; set; }

    [ReadOnly(true)]
    public string Certyfikat { get; set; }

    [ReadOnly(true)]
    public string CertyfikatKey { get; set; }

    [ReadOnly(true)]
    public string CertyfikatHasloValue { get; set; }

    public int? Przeznaczenie { get; set; }

    public TokenCalaEnovaRow Clone()
    {
        return new TokenCalaEnovaRow
        {
            NazwaFirmy = NazwaFirmy,
            ID = ID,
            Guid = Guid,
            SystemZewn = SystemZewn,
            Nazwa = Nazwa,
            Token = Token,
            Pobieranie = Pobieranie,
            Wysylanie = Wysylanie,
            Stamp = Stamp,
            OperatorzyGuids = OperatorzyGuids,
            RefreshTokenValidUntil = RefreshTokenValidUntil,
            RefreshTokenValue = RefreshTokenValue,
            RefreshTokenRequestRefNumber = RefreshTokenRequestRefNumber,
            RefreshTokenRequestTempTokenValue = RefreshTokenRequestTempTokenValue,
            WersjaAPI = WersjaAPI,
            Rodzaj = Rodzaj,
            Certyfikat = Certyfikat,
            CertyfikatKey = CertyfikatKey,
            CertyfikatHasloValue = CertyfikatHasloValue,
            Przeznaczenie = Przeznaczenie
        };
    }

    public void CopyFrom(TokenCalaEnovaRow source)
    {
        if (source == null)
        {
            return;
        }

        SystemZewn = source.SystemZewn;
        Nazwa = source.Nazwa;
        Token = source.Token;
        Pobieranie = source.Pobieranie;
        Wysylanie = source.Wysylanie;
        OperatorzyGuids = source.OperatorzyGuids;
        RefreshTokenValidUntil = source.RefreshTokenValidUntil;
        RefreshTokenRequestRefNumber = source.RefreshTokenRequestRefNumber;
        WersjaAPI = source.WersjaAPI;
        Rodzaj = source.Rodzaj;
        Przeznaczenie = source.Przeznaczenie;
    }
}
