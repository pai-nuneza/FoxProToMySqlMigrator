using System.Text;
using MySql.Data.MySqlClient;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal class BulkInsertService
    {
        public async Task ExecuteBulkInsertAsync(
            MySqlConnection connection,
            MySqlTransaction transaction,
            string tableName,
            string columnNames,
            List<DbfColumnInfo> schema,
            List<object?[]> rows,
            MigrationMode migrationMode,
            CancellationToken cancellationToken)
        {
            if (rows.Count == 0) return;

            try
            {
                var sql = new StringBuilder();
                
                if (migrationMode == MigrationMode.PatchLoad)
                {
                    sql.Append($"INSERT IGNORE INTO `{tableName}` ({columnNames}) VALUES ");
                }
                else
                {
                    sql.Append($"INSERT INTO `{tableName}` ({columnNames}) VALUES ");
                }

                using var cmd = new MySqlCommand("", connection, transaction);
                cmd.CommandTimeout = 600; // Increased to 10 minutes for very large batches
                
                for (int rowIdx = 0; rowIdx < rows.Count; rowIdx++)
                {
                    if (rowIdx > 0)
                    {
                        sql.Append(',');
                    }
                    
                    sql.Append('(');
                    
                    for (int colIdx = 0; colIdx < rows[rowIdx].Length; colIdx++)
                    {
                        if (colIdx > 0)
                        {
                            sql.Append(',');
                        }
                        
                        var paramName = $"@p{rowIdx}_{colIdx}";
                        sql.Append(paramName);
                        cmd.Parameters.AddWithValue(paramName, rows[rowIdx][colIdx] ?? DBNull.Value);
                    }
                    
                    sql.Append(')');
                }

                cmd.CommandText = sql.ToString();
                
                // Add timeout protection with better error messaging
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
                
                try
                {
                    await cmd.ExecuteNonQueryAsync(linkedCts.Token);
                }
                catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
                {
                    throw new TimeoutException($"Bulk insert operation timed out after 10 minutes. This may indicate a database performance issue or network problem.");
                }
            }
            catch (MySqlException ex)
            {
                // Provide more specific error messages for common MySQL errors
                var errorMessage = ex.Number switch
                {
                    1062 => "Duplicate entry found (key constraint violation)",
                    1406 => $"Data too long for a column in table '{tableName}'",
                    1054 => $"Unknown column in table '{tableName}'",
                    2013 => "Lost connection to MySQL server during query",
                    2006 => "MySQL server has gone away (connection lost)",
                    _ => $"MySQL error #{ex.Number}: {ex.Message}"
                };
                
                throw new Exception($"Database error during bulk insert to '{tableName}': {errorMessage}", ex);
            }
            catch (Exception ex) when (ex is not TimeoutException)
            {
                throw new Exception($"Unexpected error during bulk insert to '{tableName}': {ex.Message}", ex);
            }
        }
    }
}
