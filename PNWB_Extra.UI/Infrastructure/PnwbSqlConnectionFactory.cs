using System;
using Microsoft.Data.SqlClient;
using Soneta.Business.App;

namespace PNWB_Extra.UI;

internal static class PnwbSqlConnectionFactory
{
    public static SqlConnection Create(SqlDatabase sqlDatabase, string applicationName)
    {
        if (sqlDatabase == null)
        {
            throw new ArgumentNullException(nameof(sqlDatabase));
        }

        return Create(sqlDatabase, sqlDatabase.DatabaseName, applicationName);
    }

    public static SqlConnection Create(SqlDatabase sqlDatabase, string initialCatalog, string applicationName)
    {
        if (sqlDatabase == null)
        {
            throw new ArgumentNullException(nameof(sqlDatabase));
        }

        SqlConnectionStringBuilder builder = BuildBase(sqlDatabase);
        if (!string.IsNullOrWhiteSpace(initialCatalog))
        {
            builder.InitialCatalog = initialCatalog;
        }

        if (!string.IsNullOrWhiteSpace(applicationName))
        {
            builder.ApplicationName = applicationName;
        }

        // Zachowujemy dotychczasowe zachowanie dodatku PNWB Extra.
        builder.TrustServerCertificate = true;
        return new SqlConnection(builder.ConnectionString);
    }

    private static SqlConnectionStringBuilder BuildBase(SqlDatabase sqlDatabase)
    {
        if (sqlDatabase is MsSqlDatabase msSqlDatabase
            && msSqlDatabase.UseConnectionString
            && !string.IsNullOrWhiteSpace(msSqlDatabase.ConnectionString))
        {
            return new SqlConnectionStringBuilder(msSqlDatabase.ConnectionString);
        }

        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
        {
            DataSource = sqlDatabase.Server
        };

        if (sqlDatabase.Trusted)
        {
            builder.IntegratedSecurity = true;
            return builder;
        }

        builder.IntegratedSecurity = false;
        builder.UserID = sqlDatabase.User;
        builder.Password = GetSqlPassword(sqlDatabase);
        return builder;
    }

    private static string GetSqlPassword(SqlDatabase sqlDatabase)
    {
        try
        {
            return sqlDatabase.GetPassword();
        }
        catch
        {
            return sqlDatabase.Password;
        }
    }
}
