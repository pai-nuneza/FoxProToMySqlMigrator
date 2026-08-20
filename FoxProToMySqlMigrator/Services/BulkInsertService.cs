using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal class BulkInsertResult
    {
        public List<(int Index, object? PrimaryId)> SkippedRows { get; } = new();
        public List<BulkInsertRepairEvent> RepairedRows { get; } = new();
        public List<BulkInsertFailedRow> FailedRows { get; } = new();
    }

    internal class BulkInsertRepairEvent
    {
        public int? Index { get; set; }
        public string Message { get; set; } = "";
        public object?[]? InsertedRow { get; set; }
    }

    internal class BulkInsertFailedRow
    {
        public int Index { get; set; }
        public string Message { get; set; } = "";
        public object?[] Row { get; set; } = Array.Empty<object?>();
    }

    internal class BulkInsertService
    {
        public async Task<BulkInsertResult> ExecuteBulkInsertAsync(
            MySqlConnection connection,
            MySqlTransaction transaction,
            string tableName,
            string columnNames,
            List<DbfColumnInfo> schema,
            List<object?[]> rows,
            MigrationMode migrationMode,
            CancellationToken cancellationToken)
        {
            if (rows == null || rows.Count == 0) return new BulkInsertResult();

            try
            {
                var sql = new StringBuilder();

                sql.Append($"INSERT INTO `{tableName}` ({columnNames}) VALUES ");

                using var cmd = new MySqlCommand("", connection, transaction);
                cmd.CommandTimeout = 600; // Increased to 10 minutes for very large batches

                for (int rowIdx = 0; rowIdx < rows.Count; rowIdx++)
                {
                    if (rowIdx > 0)
                        sql.Append(',');

                    sql.Append('(');

                    for (int colIdx = 0; colIdx < rows[rowIdx].Length; colIdx++)
                    {
                        if (colIdx > 0)
                            sql.Append(',');

                        var paramName = $"@p{rowIdx}_{colIdx}";
                        sql.Append(paramName);
                        AddTypedParameter(cmd, paramName, rows[rowIdx][colIdx] ?? DBNull.Value);
                    }

                    sql.Append(')');
                }

                cmd.CommandText = sql.ToString();

                // Add timeout protection with better error messaging
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

                var result = new BulkInsertResult();

                try
                {
                    await cmd.ExecuteNonQueryAsync(linkedCts.Token);
                    result.RepairedRows.AddRange(await GetWarningsAsync(connection, transaction, cancellationToken));
                }
                catch (MySqlException ex)
                {
                    // If a large batch failed, try to split and retry smaller batches to identify problematic rows
                    if (IsConnectionError(ex))
                    {
                        throw;
                    }

                    if (migrationMode == MigrationMode.PatchLoad && ex.Number == 1062 && rows.Count == 1)
                    {
                        return new BulkInsertResult
                        {
                            SkippedRows = { (0, null) }
                        };
                    }

                    if (rows.Count > 1)
                    {
                        int mid = rows.Count / 2;
                        var firstHalf = rows.Take(mid).ToList();
                        var secondHalf = rows.Skip(mid).ToList();

                        var left = await ExecuteBulkInsertAsync(connection, transaction, tableName, columnNames, schema, firstHalf, migrationMode, cancellationToken);
                        var splitResult = new BulkInsertResult();
                        splitResult.SkippedRows.AddRange(left.SkippedRows);
                        splitResult.RepairedRows.AddRange(left.RepairedRows);
                        splitResult.FailedRows.AddRange(left.FailedRows);

                        var right = await ExecuteBulkInsertAsync(connection, transaction, tableName, columnNames, schema, secondHalf, migrationMode, cancellationToken);
                        // adjust indices for the second half
                        splitResult.SkippedRows.AddRange(right.SkippedRows.Select(r => (r.Index + mid, r.PrimaryId)));
                        splitResult.RepairedRows.AddRange(right.RepairedRows.Select(r => new BulkInsertRepairEvent
                        {
                            Index = AdjustWarningIndex(r.Index, mid),
                            Message = r.Message,
                            InsertedRow = r.InsertedRow
                        }));
                        splitResult.FailedRows.AddRange(right.FailedRows.Select(r => new BulkInsertFailedRow
                        {
                            Index = r.Index + mid,
                            Message = r.Message,
                            Row = r.Row
                        }));

                        return splitResult;
                    }
                    else
                    {
                        return await TryInsertCorruptedRowAsync(
                            connection, transaction, tableName, columnNames, schema, rows[0], migrationMode, ex, cancellationToken);
                    }
                }
                catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
                {
                    throw new TimeoutException($"Bulk insert operation timed out after 10 minutes. This may indicate a database performance issue or network problem.");
                }

                return result;
            }
            catch (MySqlException ex)
            {
                // Provide more specific error messages for common MySQL errors
                var errorMessage = ex.Number switch
                {
                    1062 => migrationMode == MigrationMode.PatchLoad
                        ? "Duplicate entry found while isolating a patch-load row"
                        : "Duplicate entry found (key constraint violation)",
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

        private async Task<BulkInsertResult> TryInsertCorruptedRowAsync(
            MySqlConnection connection,
            MySqlTransaction transaction,
            string tableName,
            string columnNames,
            List<DbfColumnInfo> schema,
            object?[] row,
            MigrationMode migrationMode,
            MySqlException originalException,
            CancellationToken cancellationToken)
        {
            var result = new BulkInsertResult();
            if (migrationMode == MigrationMode.PatchLoad && originalException.Number == 1062)
            {
                result.SkippedRows.Add((0, null));
                return await Task.FromResult(result);
            }

            result.FailedRows.Add(new BulkInsertFailedRow
            {
                Index = 0,
                Message = $"Row could not be inserted safely. MySQL error #{originalException.Number}: {originalException.Message}. Source values were preserved in the failed-row log.",
                Row = row
            });
            return await Task.FromResult(result);
        }

        private void AddTypedParameter(MySqlCommand command, string parameterName, object? value)
        {
            if (value == null || value == DBNull.Value)
            {
                command.Parameters.AddWithValue(parameterName, DBNull.Value);
                return;
            }

            if (value is string stringValue)
            {
                var parameter = command.Parameters.Add(parameterName, MySqlDbType.LongText);
                parameter.Value = stringValue;
                return;
            }

            if (value is byte[] bytes)
            {
                var parameter = command.Parameters.Add(parameterName, MySqlDbType.LongBlob);
                parameter.Value = bytes;
                return;
            }

            command.Parameters.AddWithValue(parameterName, value);
        }

        private bool IsConnectionError(Exception exception)
        {
            if (exception is MySqlException mySqlException)
            {
                return mySqlException.Number == 0
                    || mySqlException.Number == 1042
                    || mySqlException.Number == 1047
                    || mySqlException.Number == 2002
                    || mySqlException.Number == 2003
                    || mySqlException.Number == 2006
                    || mySqlException.Number == 2013;
            }

            return exception.InnerException != null && IsConnectionError(exception.InnerException);
        }

        private async Task<List<BulkInsertRepairEvent>> GetWarningsAsync(
            MySqlConnection connection,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var warnings = new List<BulkInsertRepairEvent>();

            using var warningCmd = new MySqlCommand("SHOW WARNINGS", connection, transaction);
            using var reader = await warningCmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var level = reader.GetString(0);
                var code = reader.GetInt32(1);
                var message = reader.GetString(2);
                if (code == 1062)
                {
                    continue;
                }

                warnings.Add(new BulkInsertRepairEvent
                {
                    Index = TryGetZeroBasedRowIndex(message),
                    Message = $"{level} {code}: {message}"
                });
            }

            return warnings;
        }

        private int? TryGetZeroBasedRowIndex(string warningMessage)
        {
            var match = Regex.Match(warningMessage, @"at row (?<row>\d+)", RegexOptions.IgnoreCase);
            if (!match.Success || !int.TryParse(match.Groups["row"].Value, out var oneBasedRow))
            {
                return null;
            }

            return Math.Max(0, oneBasedRow - 1);
        }

        private int? AdjustWarningIndex(int? index, int offset)
        {
            return index.HasValue ? index.Value + offset : null;
        }

        private string FormatRowForLog(object?[] row, List<DbfColumnInfo> schema)
        {
            try
            {
                var sb = new StringBuilder();
                for (int i = 0; i < Math.Min(schema.Count, row.Length); i++)
                {
                    var name = schema[i].Name;
                    var val = row[i] == null || row[i] == DBNull.Value ? "NULL" : row[i]?.ToString() ?? "";
                    sb.AppendFormat("{0}={1}; ", name, val);
                }
                return sb.ToString();
            }
            catch
            {
                return "<unavailable>";
            }
        }
    }
}
