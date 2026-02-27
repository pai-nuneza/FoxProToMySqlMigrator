using System.Text;
using MySql.Data.MySqlClient;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal class BulkInsertService
    {
        public async Task<List<(int Index, object? PrimaryId)>> ExecuteBulkInsertAsync(
            MySqlConnection connection,
            MySqlTransaction transaction,
            string tableName,
            string columnNames,
            List<DbfColumnInfo> schema,
            List<object?[]> rows,
            MigrationMode migrationMode,
            CancellationToken cancellationToken)
        {
            if (rows.Count == 0) return new List<(int, object?)>();

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
                
                var skippedIndices = new List<(int Index, object? PrimaryId)>();

                try
                {
                    var affected = await cmd.ExecuteNonQueryAsync(linkedCts.Token);

                    // If using PATCH load mode with INSERT IGNORE some rows may be ignored (e.g. duplicates)
                    if (migrationMode == MigrationMode.PatchLoad && affected < rows.Count)
                    {
                        // Determine which rows were skipped by checking for an existing matching row in the target table.
                        for (int r = 0; r < rows.Count; r++)
                        {
                            var whereClause = new StringBuilder();
                            using var existsCmd = new MySqlCommand();
                            existsCmd.Connection = connection;
                            existsCmd.Transaction = transaction;

                            var predicates = new List<string>();
                            for (int c = 0; c < schema.Count; c++)
                            {
                                var colName = schema[c].Name;
                                var paramName = $"@e{r}_{c}";
                                predicates.Add($"((`{colName}` IS NULL AND {paramName} IS NULL) OR (`{colName}` = {paramName}))");
                                existsCmd.Parameters.AddWithValue(paramName, rows[r][c] ?? DBNull.Value);
                            }
                            whereClause.Append(string.Join(" AND ", predicates));

                            // Try to fetch the primary_id if present, otherwise fall back to a simple existence check
                            try
                            {
                                existsCmd.CommandText = $"SELECT `primary_id` FROM `{tableName}` WHERE {whereClause}";
                                var scalar = await existsCmd.ExecuteScalarAsync(cancellationToken);
                                if (scalar != null)
                                {
                                    skippedIndices.Add((r, scalar));
                                    continue;
                                }
                            }
                            catch (MySqlException mex) when (mex.Number == 1054)
                            {
                                // unknown column `primary_id` - fall back to SELECT 1
                                try
                                {
                                    existsCmd.CommandText = $"SELECT 1 FROM `{tableName}` WHERE {whereClause}";
                                    var scalar2 = await existsCmd.ExecuteScalarAsync(cancellationToken);
                                    if (scalar2 != null)
                                    {
                                        skippedIndices.Add((r, null));
                                        continue;
                                    }
                                }
                                catch
                                {
                                    skippedIndices.Add((r, null));
                                }
                            }
                            catch
                            {
                                // Any other error - conservatively mark skipped with unknown primary id
                                skippedIndices.Add((r, null));
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
                {
                    throw new TimeoutException($"Bulk insert operation timed out after 10 minutes. This may indicate a database performance issue or network problem.");
                }

                return skippedIndices;
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
