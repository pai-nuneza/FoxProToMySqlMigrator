using System.IO;
using System.Text;
using MySql.Data.MySqlClient;
using FoxProToMySqlMigrator.Models;
using FoxProToMySqlMigrator.Services;
using FoxProToMySqlMigrator.Helpers;

namespace FoxProToMySqlMigrator
{
    public class FoxProMigrationService
    {
        private const long MaxEstimatedBatchBytes = 4L * 1024 * 1024;

        public event Action<string>? LogMessage;
        public event Action<TableMigrationResult>? TableCompleted;
        
        private MigrationLogger? _logger;
        private string _errorRecordsFolder = "";
        private string _skippedRecordsFolder = "";
        private string _repairedRecordsFolder = "";
        private string _migrationTimestamp = "";
        private CheckpointService? _checkpointService;
        
        // Cache reflection lookups per table
        private System.Reflection.PropertyInfo? _cachedDbfRecordProperty;
        private System.Reflection.PropertyInfo? _cachedIsDeletedProperty;

        private sealed class DateColumnMigrationStats
        {
            public required string ColumnName { get; init; }
            public required string OriginalName { get; init; }
            public long SourceValueCount { get; set; }
            public DateTime? SourceMin { get; set; }
            public DateTime? SourceMax { get; set; }
            public long? TargetRowCount { get; set; }
            public long? TargetValueCount { get; set; }
            public DateTime? TargetMin { get; set; }
            public DateTime? TargetMax { get; set; }
        }

        private sealed class MigrationCounters
        {
            public long TotalRecords { get; set; }
            public long Read { get; set; }
            public long Inserted { get; set; }
            public long Updated { get; set; }
            public long Deleted { get; set; }
            public long Duplicates { get; set; }
            public long Skipped { get; set; }
            public long Failed { get; set; }
            public long ConversionWarnings { get; set; }
            public long ResumeSkipped { get; set; }
            public List<DateColumnMigrationStats> DateStats { get; set; } = new();

            public long Accounted => Read + ResumeSkipped;
            public long RowCount => Inserted + Updated;
            public long ErrorCount => Failed + ConversionWarnings;
        }

        public async Task<MigrationCheckpoint?> LoadCheckpointAsync(string foxProFolder, string targetDatabase)
        {
            var checkpointFile = Path.Combine(AppSettings.LogsFolder, $"checkpoint_{targetDatabase}.json");
            
            var checkpointService = new CheckpointService(checkpointFile);
            return await checkpointService.LoadCheckpointAsync(foxProFolder, targetDatabase);
        }

        public async Task MigrateAsync(
            string foxProFolder, 
            string mySqlConnectionString, 
            string targetDatabase, 
            bool safeMode,
            MigrationMode migrationMode,
            int batchSize,
            MigrationCheckpoint? resumeFromCheckpoint = null,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // Initialize migration session
                _migrationTimestamp = resumeFromCheckpoint?.StartTime.ToString("yyyyMMdd_HHmmss") 
                    ?? DateTime.Now.ToString("yyyyMMdd_HHmmss");
                
                var (mainLogsFolder, checkpointFilePath) = SetupLogFolders(targetDatabase);
                _logger = new MigrationLogger(Path.Combine(mainLogsFolder, "migration_errors.txt"));
                _logger.LogMessage += (msg) => LogMessage?.Invoke(msg);
                _checkpointService = new CheckpointService(checkpointFilePath);
                
                LogMigrationStart(foxProFolder, migrationMode, batchSize, mainLogsFolder, resumeFromCheckpoint);

                if (!Directory.Exists(foxProFolder))
                {
                    _logger.Log($"ERROR: Folder does not exist: {foxProFolder}");
                    throw new DirectoryNotFoundException($"FoxPro folder not found: {foxProFolder}");
                }

                var dbfFiles = Directory.GetFiles(foxProFolder, "*.dbf");
                _logger.Log($"Found {dbfFiles.Length} DBF file(s)");

                if (dbfFiles.Length == 0)
                {
                    _logger.Log("No DBF files found in the selected folder");
                    throw new FileNotFoundException("No DBF files found in the selected folder");
                }

                // Initialize or restore checkpoint
                var checkpoint = resumeFromCheckpoint ?? new MigrationCheckpoint
                {
                    FoxProFolder = foxProFolder,
                    TargetDatabase = targetDatabase,
                    StartTime = DateTime.Now,
                    LastUpdateTime = DateTime.Now,
                    TotalTables = dbfFiles.Length,
                    CompletedTables = new List<string>(),
                    IsCompleted = false
                };

                await _checkpointService.SaveCheckpointAsync(checkpoint);
                cancellationToken.ThrowIfCancellationRequested();

                using var mySqlConn = new MySqlConnection(mySqlConnectionString);
                
                try
                {
                    await mySqlConn.OpenAsync(cancellationToken);
                    _logger.Log($"Connected to MySQL server");
                }
                catch (MySqlException ex)
                {
                    _logger.Log($"Failed to connect to MySQL server: {ex.Message}");
                    throw new Exception($"Cannot connect to MySQL server. Please check your connection string.\n\nDetails: {ex.Message}", ex);
                }

                var tableService = new MySqlTableService();
                await tableService.EnsureDatabaseExistsAsync(mySqlConn, targetDatabase, cancellationToken);
                _logger.Log($"Database '{targetDatabase}' ready");
                
                mySqlConn.ChangeDatabase(targetDatabase);
                _logger.Log($"Using database: {targetDatabase}");
                await ConfigureStrictMySqlSessionAsync(mySqlConn, cancellationToken);

                await ProcessTables(dbfFiles, mySqlConn, checkpoint, safeMode, migrationMode, batchSize, cancellationToken);

                // Mark as completed
                checkpoint.IsCompleted = true;
                await _checkpointService.SaveCheckpointAsync(checkpoint);
                _checkpointService.DeleteCheckpoint();

                _logger.Log("Migration completed successfully!");
                _logger.Log($"===========================================");
                _logger.Log($"📁 All logs saved to:");
                _logger.Log($"   {mainLogsFolder}");
                _logger.Log($"===========================================");
            }
            catch (OperationCanceledException)
            {
                _logger?.Log("Migration cancelled by user.");
                _logger?.Log($"💾 Progress saved! You can resume this migration later.");
                throw;
            }
            catch (Exception ex)
            {
                _logger?.Log($"CRITICAL ERROR: {ex.Message}");
                _logger?.Log($"Stack trace: {ex.StackTrace}");
                // Include full exception details (including inner exceptions and MySqlException info)
                _logger?.LogError("CRITICAL", "Migration", ex.Message, ex.ToString());
                _logger?.Log($"💾 Progress saved! You can resume this migration later.");
                throw; // Re-throw so UI can handle it
            }
        }

        private (string mainLogsFolder, string checkpointFilePath) SetupLogFolders(string targetDatabase)
        {
            var mainLogsFolder = Path.Combine(AppSettings.LogsFolder, _migrationTimestamp);
            Directory.CreateDirectory(mainLogsFolder);
            
            var checkpointFolder = AppSettings.LogsFolder;
            Directory.CreateDirectory(checkpointFolder);
            var checkpointFilePath = Path.Combine(checkpointFolder, $"checkpoint_{targetDatabase}.json");
            
            _errorRecordsFolder = Path.Combine(mainLogsFolder, "ErrorRecords");
            Directory.CreateDirectory(_errorRecordsFolder);
            
            _skippedRecordsFolder = Path.Combine(mainLogsFolder, "SkippedRecords");
            Directory.CreateDirectory(_skippedRecordsFolder);

            _repairedRecordsFolder = Path.Combine(mainLogsFolder, "RepairedRecords");
            Directory.CreateDirectory(_repairedRecordsFolder);

            return (mainLogsFolder, checkpointFilePath);
        }

        private async Task ConfigureStrictMySqlSessionAsync(
            MySqlConnection connection,
            CancellationToken cancellationToken)
        {
            try
            {
                using var modeCmd = new MySqlCommand(
                    "SET SESSION sql_mode = CONCAT_WS(',', @@SESSION.sql_mode, 'STRICT_ALL_TABLES', 'ERROR_FOR_DIVISION_BY_ZERO', 'NO_ZERO_DATE', 'NO_ZERO_IN_DATE')",
                    connection);
                await modeCmd.ExecuteNonQueryAsync(cancellationToken);

                using var packetCmd = new MySqlCommand("SELECT @@SESSION.sql_mode, @@GLOBAL.max_allowed_packet", connection);
                using var reader = await packetCmd.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    var sqlMode = reader.IsDBNull(0) ? "" : reader.GetString(0);
                    var maxAllowedPacket = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
                    _logger!.Log($"MySQL strict session enabled. sql_mode={sqlMode}");
                    _logger.Log($"MySQL max_allowed_packet={maxAllowedPacket:N0} bytes. Very large memo fields bigger than this can fail even with LONGTEXT.");
                }
            }
            catch (Exception ex)
            {
                _logger!.Log($"⚠️ Could not force strict MySQL session mode: {ex.Message}. MySQL may downgrade some data errors to warnings depending on server settings.");
            }
        }

        private void LogMigrationStart(
            string foxProFolder, 
            MigrationMode migrationMode, 
            int batchSize, 
            string mainLogsFolder,
            MigrationCheckpoint? resumeFromCheckpoint)
        {
            _logger!.Log($"Starting migration from {foxProFolder}");
            _logger.Log($"Migration Mode: {migrationMode}");
            _logger.Log($"Batch Size: {batchSize} records per commit");
            _logger.Log("Deleted FoxPro records: included (marked in MySQL `is_deleted` column)");
            
            if (resumeFromCheckpoint != null)
            {
                _logger.Log($"📦 RESUMING migration from checkpoint");
                _logger.Log($"   Already completed: {resumeFromCheckpoint.CompletedTables.Count}/{resumeFromCheckpoint.TotalTables} tables");
                _logger.Log($"   Started: {resumeFromCheckpoint.StartTime:yyyy-MM-dd HH:mm:ss}");
            }
            
            _logger.Log($"Error records will be saved to: {_errorRecordsFolder}");
            _logger.Log($"Skipped records will be saved to: {_skippedRecordsFolder}");
            _logger.Log($"Repaired records will be saved to: {_repairedRecordsFolder}");
            _logger.Log($"All migration logs saved to: {mainLogsFolder}");
        }

        private async Task ProcessTables(
            string[] dbfFiles,
            MySqlConnection mySqlConn,
            MigrationCheckpoint checkpoint,
            bool safeMode,
            MigrationMode migrationMode,
            int batchSize,
            CancellationToken cancellationToken)
        {
            var totalTables = dbfFiles.Length;
            var currentTable = 0;

            foreach (var dbfFile in dbfFiles)
            {
                var tableName = Path.GetFileNameWithoutExtension(dbfFile);
                
                // Skip already completed tables
                if (checkpoint.CompletedTables.Contains(tableName))
                {
                    currentTable++;
                    _logger!.Log($"[{currentTable}/{totalTables}] ⏭️  Skipping already completed table: {tableName}");
                    continue;
                }

                cancellationToken.ThrowIfCancellationRequested();

                currentTable++;
                _logger!.Log($"[{currentTable}/{totalTables}] Processing table: {tableName}");
                
                // Reset reflection cache for each table
                _cachedDbfRecordProperty = null;
                _cachedIsDeletedProperty = null;
                
                var wasResumingThisTable = string.Equals(checkpoint.CurrentTable, tableName, StringComparison.OrdinalIgnoreCase);
                checkpoint.CurrentTable = tableName;
                checkpoint.CurrentTableLastCommittedRecordNumber = wasResumingThisTable
                    ? checkpoint.CurrentTableLastCommittedRecordNumber
                    : 0;
                checkpoint.LastUpdateTime = DateTime.Now;
                await _checkpointService!.SaveCheckpointAsync(checkpoint);

                await MigrateTableAsync(dbfFile, mySqlConn, checkpoint, safeMode, migrationMode, batchSize, cancellationToken);
                
                // Update checkpoint after successful table migration
                checkpoint.CompletedTables.Add(tableName);
                checkpoint.CurrentTable = null;
                checkpoint.CurrentTableLastCommittedRecordNumber = 0;
                checkpoint.LastUpdateTime = DateTime.Now;
                await _checkpointService!.SaveCheckpointAsync(checkpoint);
            }
        }

        private async Task MigrateTableAsync(
            string dbfFilePath, 
            MySqlConnection mySqlConn, 
            MigrationCheckpoint checkpoint,
            bool safeMode,
            MigrationMode migrationMode,
            int batchSize,
            CancellationToken cancellationToken = default)
        {
            var tableName = Path.GetFileNameWithoutExtension(dbfFilePath).ToLower();

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var schemaReader = new DbfSchemaReader();
                var (dbfReader, memoStream) = schemaReader.OpenDbfFile(dbfFilePath, out bool hasMemoFile, out string memoFileType);
                
                try
                {
                    _logger!.Log($"  Opened DBF file: {Path.GetFileNameWithoutExtension(dbfFilePath)}.dbf" + 
                        (hasMemoFile ? $" (with {memoFileType} memo file)" : ""));
                    _logger.Log($"  MySQL table name: '{tableName}'");
                    await LogTableNameCaseWarningsAsync(mySqlConn, tableName, cancellationToken);
                    var dbfTotalCount = GetDbfRecordCount(dbfReader);
                    var resumeAfterRecordNumber = string.Equals(checkpoint.CurrentTable, tableName, StringComparison.OrdinalIgnoreCase)
                        ? checkpoint.CurrentTableLastCommittedRecordNumber
                        : 0;

                    var schema = schemaReader.GetTableSchema(dbfReader, dbfFilePath);
                    using var memoResolver = DbfMemoResolver.TryCreate(dbfFilePath, schema, Encoding.GetEncoding(1252));
                    
                    LogSchemaInfo(schema, schemaReader, safeMode);
                    
                    var tableService = new MySqlTableService();
                    await tableService.CreateTableAsync(
                        mySqlConn,
                        tableName,
                        schema,
                        safeMode,
                        migrationMode,
                        preserveExistingRows: resumeAfterRecordNumber > 0,
                        cancellationToken);
                    
                    var logMode = migrationMode == MigrationMode.FullReload ? "Full Reload" : "Patch Load";
                    _logger.Log($"  Created table with AUTO_INCREMENT primary_id and {schema.Count} data columns ({logMode})");

                    var counters = await CopyDataBulkAsync(
                        dbfReader,
                        mySqlConn,
                        tableName,
                        schema,
                        safeMode,
                        migrationMode,
                        batchSize,
                        checkpoint,
                        dbfFilePath,
                        memoResolver,
                        dbfTotalCount,
                        resumeAfterRecordNumber,
                        cancellationToken);

                    counters.TotalRecords = dbfTotalCount ?? counters.Accounted;
                    var targetRowCount = await GetTargetRowCountAsync(mySqlConn, tableName, cancellationToken);
                    var countSummary = GetRecordCountSummary(dbfTotalCount, counters, targetRowCount);
                    await LogDateColumnStatsAsync(mySqlConn, tableName, counters.DateStats, cancellationToken);
                    
                    _logger.Log($"  ✓ Completed: {counters.RowCount:N0} rows migrated" + 
                        (counters.ResumeSkipped > 0 ? $", {counters.ResumeSkipped:N0} already committed before resume" : "") +
                        (counters.Duplicates > 0 ? $", {counters.Duplicates:N0} duplicates accounted" : "") +
                        (counters.Skipped > 0 ? $", {counters.Skipped:N0} skipped" : "") +
                        (counters.Deleted > 0 ? $", {counters.Deleted:N0} marked as deleted" : "") +
                        (counters.ErrorCount > 0 ? $", {counters.ErrorCount:N0} warnings/errors (see log file)" : ""));

                    LogCounterSummary(counters, countSummary, targetRowCount);

                    // Fire TableCompleted event
                    TableCompleted?.Invoke(new TableMigrationResult
                    {
                        TableName = tableName,
                        RowCount = counters.RowCount,
                        ErrorCount = counters.ErrorCount,
                        TotalRecords = counters.TotalRecords,
                        ReadCount = counters.Accounted,
                        InsertedCount = counters.Inserted,
                        UpdatedCount = counters.Updated,
                        DeletedCount = counters.Deleted,
                        DuplicateCount = counters.Duplicates,
                        SkippedCount = counters.Skipped,
                        FailedCount = counters.Failed,
                        DbfTotalCount = dbfTotalCount,
                        AccountedCount = countSummary.AccountedCount,
                        MissingCount = countSummary.MissingCount,
                        CountStatus = countSummary.Status
                    });

                    if (countSummary.Status == "Mismatch")
                    {
                        LogDbfPhysicalRecordDiagnostics(dbfFilePath, countSummary.AccountedCount, dbfTotalCount);
                        throw new InvalidOperationException(
                            $"Unexplained DBF record count mismatch for `{tableName}`. DBF header={dbfTotalCount:N0}, read/accounted={countSummary.AccountedCount:N0}, missing={countSummary.MissingCount:N0}. Migration was not marked complete.");
                    }
                }
                finally
                {
                    dbfReader?.Dispose();
                    memoStream?.Dispose();
                }
            }
            catch (OperationCanceledException)
            {
                _logger!.Log($"  Migration of {tableName} cancelled.");
                throw;
            }
            catch (Exception ex)
            {
                _logger!.Log($"  ✗ ERROR migrating {tableName}: {ex.Message}");
                if (ex.InnerException != null)
                {
                    _logger.Log($"    Inner exception: {ex.InnerException.Message}");
                }
                // Log full exception details (includes inner exceptions such as MySqlException)
                _logger.LogError(tableName, "Table Migration", ex.Message, ex.ToString());

                // Fire TableCompleted event with error
                TableCompleted?.Invoke(new TableMigrationResult
                {
                    TableName = tableName,
                    RowCount = 0,
                    ErrorCount = 1,
                    TotalRecords = 0,
                    ReadCount = 0,
                    InsertedCount = 0,
                    UpdatedCount = 0,
                    DeletedCount = 0,
                    DuplicateCount = 0,
                    SkippedCount = 0,
                    FailedCount = 1
                });
                throw;
            }
        }

        private void LogSchemaInfo(List<DbfColumnInfo> schema, DbfSchemaReader schemaReader, bool safeMode)
        {
            var largeTextFieldCount = schemaReader.CountLargeTextFields(schema);
            var typeMapper = new MySqlTypeMapper();
            
            _logger!.Log($"  Columns found: {schema.Count} columns" + 
                (largeTextFieldCount > 0 ? $" ({largeTextFieldCount} large text field(s) → TEXT)" : ""));
            
            foreach (var col in schema)
            {
                // Show the DBF native type
                string dbfType = GetDbfTypeDescription(col.DbfFieldType, col.Length, col.DecimalCount);
                
                // Show what MySQL type it will be mapped to (use actual safeMode setting)
                string mySqlType = typeMapper.MapToMySqlType(col, safeMode);
                
                _logger.Log($"    - '{col.OriginalName}' -> '{col.Name}' (DBF: {dbfType} → MySQL: {mySqlType})");
            }
            _logger.Log($"    - DBF Deletion Flag -> 'is_deleted' (Boolean → BOOLEAN)");
        }
        
        private string GetDbfTypeDescription(char dbfType, int length, int decimalCount)
        {
            return dbfType switch
            {
                'C' => $"Character({length})",
                'N' => decimalCount > 0 ? $"Numeric({length},{decimalCount})" : $"Numeric({length})",
                'F' => "Float",
                'D' => "Date",
                'T' => "DateTime",
                'L' => "Logical",
                'M' => "Memo",
                'G' => "General",
                'I' => "Integer",
                '+' => "AutoIncrement",
                'Y' => "Currency",
                'B' => "Double",
                'O' => "Double",
                'V' => $"Varchar({length})",
                'Q' => $"Varbinary({length})",
                'W' => "Blob",
                _ => $"Unknown({dbfType})"
            };
        }

        private long? GetDbfRecordCount(DbfDataReader.DbfDataReader dbfReader)
        {
            try
            {
                var dbfTableProperty = dbfReader.GetType().GetProperty("DbfTable",
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.NonPublic |
                    System.Reflection.BindingFlags.Public);

                var dbfTable = dbfTableProperty?.GetValue(dbfReader);
                if (dbfTable == null)
                {
                    return null;
                }

                var directCount = TryGetLongProperty(dbfTable, "RecordCount")
                    ?? TryGetLongProperty(dbfTable, "RecordsCount")
                    ?? TryGetLongProperty(dbfTable, "NumberOfRecords")
                    ?? TryGetLongProperty(dbfTable, "Records");

                if (directCount.HasValue)
                {
                    return directCount.Value;
                }

                var headerProperty = dbfTable.GetType().GetProperty("Header",
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.NonPublic |
                    System.Reflection.BindingFlags.Public);
                var header = headerProperty?.GetValue(dbfTable);

                return header == null
                    ? null
                    : TryGetLongProperty(header, "RecordCount")
                        ?? TryGetLongProperty(header, "RecordsCount")
                        ?? TryGetLongProperty(header, "NumberOfRecords");
            }
            catch
            {
                return null;
            }
        }

        private long? TryGetLongProperty(object target, string propertyName)
        {
            var property = target.GetType().GetProperty(propertyName,
                System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic |
                System.Reflection.BindingFlags.Public);

            var value = property?.GetValue(target);
            if (value == null)
            {
                return null;
            }

            try
            {
                return Convert.ToInt64(value);
            }
            catch
            {
                return null;
            }
        }

        private async Task LogTableNameCaseWarningsAsync(
            MySqlConnection connection,
            string tableName,
            CancellationToken cancellationToken)
        {
            try
            {
                using var cmd = new MySqlCommand(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND LOWER(TABLE_NAME) = LOWER(@tableName)",
                    connection);
                cmd.Parameters.AddWithValue("@tableName", tableName);

                var matchingTables = new List<string>();
                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    matchingTables.Add(reader.GetString(0));
                }

                var differentCaseTables = matchingTables
                    .Where(t => !string.Equals(t, tableName, StringComparison.Ordinal))
                    .ToList();

                if (differentCaseTables.Count > 0)
                {
                    _logger!.Log($"  ⚠️ Table name case warning: migrator writes to `{tableName}`, but database also has {string.Join(", ", differentCaseTables.Select(t => $"`{t}`"))}. Query the exact table `{tableName}` to verify new data.");
                }
            }
            catch (Exception ex)
            {
                _logger!.Log($"  ⚠️ Could not check table name casing for `{tableName}`: {ex.Message}");
            }
        }

        private (string Status, long AccountedCount, long MissingCount) GetRecordCountSummary(
            long? dbfTotalCount,
            MigrationCounters counters,
            long? targetRowCount)
        {
            var accountedCount = counters.Accounted;

            if (!dbfTotalCount.HasValue)
            {
                return ("Unknown", accountedCount, 0);
            }

            if (accountedCount == dbfTotalCount.Value)
            {
                return ("Match", accountedCount, 0);
            }

            return ("Mismatch", accountedCount, Math.Max(0, dbfTotalCount.Value - accountedCount));
        }

        private async Task<long?> GetTargetRowCountAsync(
            MySqlConnection connection,
            string tableName,
            CancellationToken cancellationToken)
        {
            try
            {
                using var cmd = new MySqlCommand($"SELECT COUNT(*) FROM `{tableName}`", connection);
                var value = await cmd.ExecuteScalarAsync(cancellationToken);
                return value == null || value == DBNull.Value ? null : Convert.ToInt64(value);
            }
            catch (Exception ex)
            {
                _logger!.Log($"  ⚠️ Could not verify target row count for `{tableName}`: {ex.Message}");
                return null;
            }
        }

        private void LogCounterSummary(
            MigrationCounters counters,
            (string Status, long AccountedCount, long MissingCount) countSummary,
            long? targetRowCount)
        {
            _logger!.Log($"  📊 Counters: TotalRecords={counters.TotalRecords:N0}, Read={counters.Accounted:N0}, Inserted={counters.Inserted:N0}, Updated={counters.Updated:N0}, Deleted={counters.Deleted:N0}, Duplicates={counters.Duplicates:N0}, Skipped={counters.Skipped:N0}, Failed={counters.Failed:N0}");
            _logger.Log($"  📊 Verification: source accounted={countSummary.AccountedCount:N0}/{counters.TotalRecords:N0}, target rows={(targetRowCount?.ToString("N0") ?? "unknown")}, status={countSummary.Status}" +
                (countSummary.MissingCount > 0 ? $", unexplained missing={countSummary.MissingCount:N0}" : ""));
        }

        private void LogDbfPhysicalRecordDiagnostics(
            string dbfFilePath,
            long lastAccountedRecordNumber,
            long? dbfHeaderRecordCount)
        {
            try
            {
                using var stream = new FileStream(dbfFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                if (stream.Length < 32)
                {
                    _logger!.Log($"  ⚠️ DBF physical check: file is too small to contain a valid DBF header ({stream.Length:N0} bytes).");
                    return;
                }

                var header = new byte[32];
                _ = stream.Read(header, 0, header.Length);

                var headerRecordCount = BitConverter.ToUInt32(header, 4);
                var headerLength = BitConverter.ToUInt16(header, 8);
                var recordLength = BitConverter.ToUInt16(header, 10);
                var firstMissingRecordNumber = lastAccountedRecordNumber + 1;

                if (recordLength == 0 || headerLength == 0)
                {
                    _logger!.Log($"  ⚠️ DBF physical check: invalid header sizes. headerLength={headerLength}, recordLength={recordLength}.");
                    return;
                }

                var bytesAfterHeader = Math.Max(0, stream.Length - headerLength);
                var physicalRecordCapacity = bytesAfterHeader / recordLength;
                var expectedRecordCount = dbfHeaderRecordCount ?? headerRecordCount;
                var expectedFirstMissingOffset = headerLength + ((firstMissingRecordNumber - 1) * (long)recordLength);

                _logger!.Log($"  🔎 DBF physical check: fileBytes={stream.Length:N0}, headerRecords={headerRecordCount:N0}, reflectedHeaderRecords={(dbfHeaderRecordCount?.ToString("N0") ?? "unknown")}, headerLength={headerLength:N0}, recordLength={recordLength:N0}, physicalCapacity={physicalRecordCapacity:N0} records.");
                _logger.Log($"  🔎 First missing logical record #{firstMissingRecordNumber:N0} should start at byte offset {expectedFirstMissingOffset:N0}.");

                if (expectedFirstMissingOffset >= stream.Length)
                {
                    _logger.Log($"  ⚠️ DBF physical check: expected first missing record is beyond end of file. Header may overstate the true record count.");
                    return;
                }

                stream.Position = expectedFirstMissingOffset;
                var marker = stream.ReadByte();
                _logger.Log($"  🔎 First missing record marker: 0x{marker:X2} ({DescribeDbfRecordMarker(marker)}).");

                var scanLimit = Math.Min(expectedRecordCount, physicalRecordCapacity);
                if (firstMissingRecordNumber > scanLimit)
                {
                    _logger.Log($"  ⚠️ DBF physical check: no physical records remain after #{lastAccountedRecordNumber:N0}; header likely reports more records than the file contains.");
                    return;
                }

                var activeCount = 0L;
                var deletedCount = 0L;
                var eofMarkerCount = 0L;
                var otherMarkerCount = 0L;
                long? firstEofMarkerRecord = null;
                long? firstUnexpectedMarkerRecord = null;

                stream.Position = expectedFirstMissingOffset;
                for (var recordNumber = firstMissingRecordNumber; recordNumber <= scanLimit; recordNumber++)
                {
                    var currentMarker = stream.ReadByte();
                    if (currentMarker < 0)
                    {
                        break;
                    }

                    switch (currentMarker)
                    {
                        case 0x20:
                            activeCount++;
                            break;
                        case 0x2A:
                            deletedCount++;
                            break;
                        case 0x1A:
                            eofMarkerCount++;
                            firstEofMarkerRecord ??= recordNumber;
                            break;
                        default:
                            otherMarkerCount++;
                            firstUnexpectedMarkerRecord ??= recordNumber;
                            break;
                    }

                    if (recordNumber < scanLimit)
                    {
                        stream.Position += recordLength - 1;
                    }
                }

                _logger.Log($"  🔎 Physical records after reader stopped: active={activeCount:N0}, deleted={deletedCount:N0}, eofMarkers={eofMarkerCount:N0}, otherMarkers={otherMarkerCount:N0}, scanned={scanLimit - firstMissingRecordNumber + 1:N0}.");

                if (firstEofMarkerRecord.HasValue)
                {
                    _logger.Log($"  ⚠️ Found 0x1A EOF marker at physical record #{firstEofMarkerRecord.Value:N0}. Some DBF readers stop at this marker even when the header says more records follow.");
                }

                if (firstUnexpectedMarkerRecord.HasValue)
                {
                    _logger.Log($"  ⚠️ Found unexpected deletion/status marker at physical record #{firstUnexpectedMarkerRecord.Value:N0}. This can indicate corruption or a DBF variant the reader does not understand.");
                }
            }
            catch (Exception ex)
            {
                _logger!.Log($"  ⚠️ DBF physical check failed: {ex.Message}");
                _logger.LogError(Path.GetFileNameWithoutExtension(dbfFilePath), "DBF Physical Check", ex.Message, ex.ToString());
            }
        }

        private string DescribeDbfRecordMarker(int marker)
        {
            return marker switch
            {
                0x20 => "active record",
                0x2A => "deleted record",
                0x1A => "EOF marker",
                -1 => "end of stream",
                _ => "unexpected marker"
            };
        }

        private async Task<MigrationCounters> CopyDataBulkAsync(
            DbfDataReader.DbfDataReader dbfReader, 
            MySqlConnection mySqlConn, 
            string tableName, 
            List<DbfColumnInfo> schema, 
            bool safeMode,
            MigrationMode migrationMode,
            int batchSize,
            MigrationCheckpoint checkpoint,
            string dbfFilePath,
            DbfMemoResolver? memoResolver,
            long? dbfTotalCount,
            long resumeAfterRecordNumber,
            CancellationToken cancellationToken = default)
        {
            var counters = new MigrationCounters
            {
                ResumeSkipped = resumeAfterRecordNumber,
                DateStats = CreateDateColumnStats(schema)
            };
            long recordNumber = 0;
            var batchNumber = 0;
            
            InitializeReflectionCache(dbfReader);
            
            var columnNames = BuildColumnNames(schema);
            var bulkInsertService = new BulkInsertService();
            
            using var recordTracking = new RecordTrackingService(_errorRecordsFolder, _skippedRecordsFolder, _repairedRecordsFolder, tableName);
            
            MySqlTransaction? transaction = null;
            var batchRows = new List<object?[]>();
            var batchRecordNumbers = new List<long>();
            long estimatedBatchBytes = 0;
            
            try
            {
                while (dbfReader.Read())
                {
                    if (recordNumber % 100 == 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    recordNumber++;

                    if (recordNumber <= resumeAfterRecordNumber)
                    {
                        continue;
                    }

                    counters.Read++;
                    
                    try
                    {
                        bool isDeleted = GetIsDeletedFlag(dbfReader);

                        if (isDeleted)
                        {
                            counters.Deleted++;
                        }

                        var (rowData, repairMessages) = ExtractRowData(dbfReader, schema, isDeleted, memoResolver, recordNumber);
                        TrackDateColumnStats(counters.DateStats, schema, rowData);
                        if (repairMessages.Count > 0)
                        {
                            counters.ConversionWarnings += repairMessages.Count;
                            recordTracking.LogErrorRowData(recordNumber, schema, rowData, string.Join(" | ", repairMessages));
                            recordTracking.LogRepairedRowData(recordNumber, schema, rowData, string.Join(" | ", repairMessages));
                        }

                        estimatedBatchBytes += EstimateRowPayloadBytes(rowData);
                        batchRows.Add(rowData);
                        batchRecordNumbers.Add(recordNumber);

                        if (batchRows.Count >= batchSize || estimatedBatchBytes >= MaxEstimatedBatchBytes)
                        {
                    try
                    {
                        var (newTransaction, skippedInBatch, failedInBatch) = await ProcessBatch(
                            mySqlConn, transaction, tableName, columnNames, schema,
                            batchRows, batchRecordNumbers, migrationMode, ++batchNumber, counters.RowCount,
                            bulkInsertService, recordTracking, cancellationToken);
                        transaction = newTransaction;
                        counters.Duplicates += skippedInBatch;
                        counters.Failed += failedInBatch;
                        counters.Inserted += batchRows.Count - skippedInBatch - failedInBatch;
                        await SaveTableProgressCheckpointAsync(checkpoint, tableName, batchRecordNumbers[^1]);
                        batchRows.Clear();
                        batchRecordNumbers.Clear();
                        estimatedBatchBytes = 0;
                    }
                            catch (Exception batchEx)
                            {
                                _logger!.Log($"  ❌ CRITICAL: Batch processing failed at batch #{batchNumber}");
                                _logger.Log($"  Error: {batchEx.Message}");
                                
                                if (transaction != null)
                                {
                                    try
                                    {
                                        await transaction.RollbackAsync();
                                        await transaction.DisposeAsync();
                                    }
                                    catch { }
                                    transaction = null;
                                }
                                
                                batchRows.Clear();
                                batchRecordNumbers.Clear();
                                estimatedBatchBytes = 0;
                                throw new Exception($"Batch processing failed at batch #{batchNumber}. This usually indicates a database connection issue or data corruption.", batchEx);
                            }
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        counters.Failed++;
                        HandleRecordError(recordNumber, dbfReader, schema, ex, tableName, recordTracking);
                        
                        if (transaction != null)
                        {
                            try
                            {
                                await transaction.RollbackAsync();
                                await transaction.DisposeAsync();
                            }
                            catch { }
                            transaction = null;
                        }
                        
                        batchRows.Clear();
                        batchRecordNumbers.Clear();
                        estimatedBatchBytes = 0;
                    }
                }

                if (dbfTotalCount.HasValue && recordNumber < dbfTotalCount.Value)
                {
                    var physicalStartRecordNumber = Math.Max(recordNumber, resumeAfterRecordNumber) + 1;
                    _logger!.Log($"  ⚠️ DbfDataReader stopped at record #{recordNumber:N0}, but DBF header says {dbfTotalCount.Value:N0}. Forcing physical DBF read from record #{physicalStartRecordNumber:N0}.");

                    var physicalReader = new DbfPhysicalRecordReader(Encoding.GetEncoding(1252));
                    foreach (var physicalRecord in physicalReader.ReadRecords(dbfFilePath, schema, physicalStartRecordNumber, dbfTotalCount))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        recordNumber = physicalRecord.RecordNumber;
                        counters.Read++;

                        if (physicalRecord.Marker == 0x1A)
                        {
                            counters.Skipped++;
                            recordTracking.LogSkippedRowData(
                                physicalRecord.RecordNumber,
                                schema,
                                physicalRecord.Values,
                                "Physical DBF fallback found EOF marker at this record slot; counted as skipped/accounted.");
                            continue;
                        }

                        if (physicalRecord.Marker != 0x20 && physicalRecord.Marker != 0x2A && physicalRecord.Marker != 0x00)
                        {
                            counters.ConversionWarnings++;
                            recordTracking.LogErrorRowData(
                                physicalRecord.RecordNumber,
                                schema,
                                physicalRecord.Values,
                                $"Physical DBF fallback found unexpected record marker 0x{physicalRecord.Marker:X2}; attempted to preserve row.");
                        }

                        if (physicalRecord.IsDeleted)
                        {
                            counters.Deleted++;
                        }

                        var (rowData, repairMessages) = ExtractPhysicalRowData(schema, physicalRecord, memoResolver);
                        TrackDateColumnStats(counters.DateStats, schema, rowData);
                        if (repairMessages.Count > 0)
                        {
                            counters.ConversionWarnings += repairMessages.Count;
                            recordTracking.LogErrorRowData(physicalRecord.RecordNumber, schema, rowData, string.Join(" | ", repairMessages));
                            recordTracking.LogRepairedRowData(physicalRecord.RecordNumber, schema, rowData, string.Join(" | ", repairMessages));
                        }

                        estimatedBatchBytes += EstimateRowPayloadBytes(rowData);
                        batchRows.Add(rowData);
                        batchRecordNumbers.Add(physicalRecord.RecordNumber);

                        if (batchRows.Count >= batchSize || estimatedBatchBytes >= MaxEstimatedBatchBytes)
                        {
                            try
                            {
                                var (newTransaction, skippedInBatch, failedInBatch) = await ProcessBatch(
                                    mySqlConn, transaction, tableName, columnNames, schema,
                                    batchRows, batchRecordNumbers, migrationMode, ++batchNumber, counters.RowCount,
                                    bulkInsertService, recordTracking, cancellationToken);
                                transaction = newTransaction;
                                counters.Duplicates += skippedInBatch;
                                counters.Failed += failedInBatch;
                                counters.Inserted += batchRows.Count - skippedInBatch - failedInBatch;
                                await SaveTableProgressCheckpointAsync(checkpoint, tableName, batchRecordNumbers[^1]);
                                batchRows.Clear();
                                batchRecordNumbers.Clear();
                                estimatedBatchBytes = 0;
                            }
                            catch (Exception batchEx)
                            {
                                _logger!.Log($"  ❌ CRITICAL: Forced physical-read batch failed at batch #{batchNumber}");
                                _logger.Log($"  Error: {batchEx.Message}");

                                if (transaction != null)
                                {
                                    try
                                    {
                                        await transaction.RollbackAsync();
                                        await transaction.DisposeAsync();
                                    }
                                    catch { }
                                    transaction = null;
                                }

                                batchRows.Clear();
                                batchRecordNumbers.Clear();
                                estimatedBatchBytes = 0;
                                throw new Exception($"Forced physical DBF read batch failed at batch #{batchNumber}.", batchEx);
                            }
                        }
                    }
                }
                
                if (batchRows.Count > 0)
                {
                    try
                    {
                        var (skippedInFinalBatch, failedInFinalBatch) = await ProcessFinalBatch(
                            mySqlConn, transaction, tableName, columnNames, schema, 
                            batchRows, batchRecordNumbers, migrationMode, ++batchNumber, counters.RowCount, 
                            bulkInsertService, recordTracking, cancellationToken);
                        counters.Duplicates += skippedInFinalBatch;
                        counters.Failed += failedInFinalBatch;
                        counters.Inserted += batchRows.Count - skippedInFinalBatch - failedInFinalBatch;
                        await SaveTableProgressCheckpointAsync(checkpoint, tableName, batchRecordNumbers[^1]);
                    }
                    catch (Exception finalEx)
                    {
                        _logger!.Log($"  ❌ CRITICAL: Final batch processing failed");
                        _logger.Log($"  Error: {finalEx.Message}");
                        throw new Exception($"Final batch processing failed. {batchRows.Count} records may have been lost.", finalEx);
                    }
                }

                LogTrackingFiles(recordTracking);
            }
            catch (OperationCanceledException)
            {
                if (transaction != null)
                {
                    try
                    {
                        await transaction.RollbackAsync();
                        await transaction.DisposeAsync();
                    }
                    catch { }
                    _logger!.Log($"  ❌ Batch rolled back due to cancellation");
                }
                throw;
            }
            catch (Exception ex)
            {
                if (transaction != null)
                {
                    try
                    {
                        await transaction.RollbackAsync();
                        await transaction.DisposeAsync();
                    }
                    catch { }
                }
                _logger!.Log($"  ❌ Batch rolled back due to error: {ex.Message}");
                _logger.LogError(tableName, "Transaction", ex.Message, ex.StackTrace ?? "");
                throw new Exception($"Migration failed for table '{tableName}' at record #{recordNumber}. Check error logs for details.", ex);
            }

            return counters;
        }

        private long EstimateRowPayloadBytes(object?[] row)
        {
            long bytes = 0;

            foreach (var value in row)
            {
                bytes += value switch
                {
                    null => 4,
                    DBNull => 4,
                    string text => Encoding.UTF8.GetByteCount(text),
                    byte[] binary => binary.Length,
                    DateTime => 8,
                    bool => 1,
                    int => 4,
                    long => 8,
                    decimal => 16,
                    double => 8,
                    float => 4,
                    _ => Encoding.UTF8.GetByteCount(value.ToString() ?? "")
                };
            }

            return bytes;
        }

        private (object?[] rowData, List<string> repairMessages) ExtractPhysicalRowData(
            List<DbfColumnInfo> schema,
            DbfPhysicalRecord physicalRecord,
            DbfMemoResolver? memoResolver)
        {
            var rowData = new object?[schema.Count + 1];
            var repairMessages = new List<string>();

            for (var i = 0; i < schema.Count; i++)
            {
                var value = physicalRecord.Values.Length > i ? physicalRecord.Values[i] : DBNull.Value;
                rowData[i] = NormalizeValueForColumn(schema[i], value, repairMessages, memoResolver, physicalRecord.RecordNumber);
            }

            rowData[schema.Count] = physicalRecord.IsDeleted;
            return (rowData, repairMessages);
        }

        private async Task SaveTableProgressCheckpointAsync(
            MigrationCheckpoint checkpoint,
            string tableName,
            long lastCommittedRecordNumber)
        {
            checkpoint.CurrentTable = tableName;
            checkpoint.CurrentTableLastCommittedRecordNumber = lastCommittedRecordNumber;
            checkpoint.LastUpdateTime = DateTime.Now;
            await _checkpointService!.SaveCheckpointAsync(checkpoint);
        }

        private List<DateColumnMigrationStats> CreateDateColumnStats(List<DbfColumnInfo> schema)
        {
            return schema
                .Where(IsDateColumn)
                .Select(c => new DateColumnMigrationStats
                {
                    ColumnName = c.Name,
                    OriginalName = c.OriginalName
                })
                .ToList();
        }

        private bool IsDateColumn(DbfColumnInfo column)
        {
            return column.DbfFieldType == 'D' || column.DbfFieldType == 'T' || column.ColumnType == typeof(DateTime);
        }

        private bool IsNumericColumn(DbfColumnInfo column)
        {
            return column.DbfFieldType == 'N'
                || column.DbfFieldType == 'F'
                || column.DbfFieldType == 'B'
                || column.DbfFieldType == 'Y'
                || column.DbfFieldType == 'I'
                || column.DbfFieldType == '+'
                || column.DbfFieldType == 'O';
        }

        private bool IsInvalidNumericValue(object value)
        {
            if (value is double doubleValue)
            {
                return double.IsNaN(doubleValue) || double.IsInfinity(doubleValue);
            }

            if (value is float floatValue)
            {
                return float.IsNaN(floatValue) || float.IsInfinity(floatValue);
            }

            return false;
        }

        private bool IsValidMySqlDate(DateTime dateValue)
        {
            return dateValue.Year >= 1000 && dateValue.Year <= 9999;
        }

        private void TrackDateColumnStats(List<DateColumnMigrationStats> dateStats, List<DbfColumnInfo> schema, object?[] rowData)
        {
            if (dateStats.Count == 0)
            {
                return;
            }

            foreach (var stat in dateStats)
            {
                var columnIndex = schema.FindIndex(c => c.Name == stat.ColumnName);
                if (columnIndex < 0 || columnIndex >= rowData.Length)
                {
                    continue;
                }

                if (TryGetDateTime(rowData[columnIndex], out var dateValue))
                {
                    stat.SourceValueCount++;
                    stat.SourceMin = stat.SourceMin.HasValue && stat.SourceMin.Value <= dateValue ? stat.SourceMin : dateValue;
                    stat.SourceMax = stat.SourceMax.HasValue && stat.SourceMax.Value >= dateValue ? stat.SourceMax : dateValue;
                }
            }
        }

        private bool TryGetDateTime(object? value, out DateTime dateValue)
        {
            dateValue = default;

            if (value == null || value == DBNull.Value)
            {
                return false;
            }

            if (value is DateTime directDate)
            {
                dateValue = directDate;
                return true;
            }

            return DateTime.TryParse(value.ToString(), out dateValue);
        }

        private async Task LogDateColumnStatsAsync(
            MySqlConnection connection,
            string tableName,
            List<DateColumnMigrationStats> dateStats,
            CancellationToken cancellationToken)
        {
            if (dateStats.Count == 0)
            {
                return;
            }

            foreach (var stat in dateStats)
            {
                try
                {
                    using var cmd = new MySqlCommand(
                        $"SELECT COUNT(*), COUNT(`{stat.ColumnName}`), MIN(`{stat.ColumnName}`), MAX(`{stat.ColumnName}`) FROM `{tableName}`",
                        connection);

                    using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                    if (await reader.ReadAsync(cancellationToken))
                    {
                        stat.TargetRowCount = reader.IsDBNull(0) ? null : reader.GetInt64(0);
                        stat.TargetValueCount = reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1));
                        stat.TargetMin = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
                        stat.TargetMax = reader.IsDBNull(3) ? null : reader.GetDateTime(3);
                    }
                }
                catch (Exception ex)
                {
                    _logger!.Log($"  ⚠️ Could not verify date range for '{stat.OriginalName}': {ex.Message}");
                    continue;
                }

                _logger!.Log($"  📅 Date check '{stat.OriginalName}': source values={stat.SourceValueCount}, source range={FormatDateRange(stat.SourceMin, stat.SourceMax)}, target values={stat.TargetValueCount?.ToString() ?? "unknown"}, target range={FormatDateRange(stat.TargetMin, stat.TargetMax)}");

                if (stat.SourceMax.HasValue && stat.TargetMax.HasValue && stat.SourceMax.Value.Date > stat.TargetMax.Value.Date)
                {
                    _logger.Log($"  ⚠️ Date mismatch for '{stat.OriginalName}': DBF has data up to {stat.SourceMax:yyyy-MM-dd}, but MySQL only has data up to {stat.TargetMax:yyyy-MM-dd}.");
                }
            }
        }

        private string FormatDateRange(DateTime? min, DateTime? max)
        {
            if (!min.HasValue && !max.HasValue)
            {
                return "none";
            }

            return $"{min:yyyy-MM-dd HH:mm:ss} to {max:yyyy-MM-dd HH:mm:ss}";
        }

        private void InitializeReflectionCache(DbfDataReader.DbfDataReader dbfReader)
        {
            if (_cachedDbfRecordProperty == null)
            {
                _cachedDbfRecordProperty = dbfReader.GetType().GetProperty("DbfRecord");
                if (_cachedDbfRecordProperty != null)
                {
                    var sampleRecord = _cachedDbfRecordProperty.GetValue(dbfReader);
                    if (sampleRecord != null)
                    {
                        _cachedIsDeletedProperty = sampleRecord.GetType().GetProperty("IsDeleted");
                    }
                }
            }
        }

        private string BuildColumnNames(List<DbfColumnInfo> schema)
        {
            var allColumnNames = schema.Select(c => $"`{c.Name}`").ToList();
            allColumnNames.Add("`is_deleted`");
            return string.Join(", ", allColumnNames);
        }

        private bool GetIsDeletedFlag(DbfDataReader.DbfDataReader dbfReader)
        {
            bool isDeleted = false;
            if (_cachedDbfRecordProperty != null && _cachedIsDeletedProperty != null)
            {
                try
                {
                    var dbfRecord = _cachedDbfRecordProperty.GetValue(dbfReader);
                    if (dbfRecord != null)
                    {
                        isDeleted = (bool)(_cachedIsDeletedProperty.GetValue(dbfRecord) ?? false);
                    }
                }
                catch { }
            }
            return isDeleted;
        }

        private (object?[] rowData, List<string> repairMessages) ExtractRowData(
            DbfDataReader.DbfDataReader dbfReader, 
            List<DbfColumnInfo> schema, 
            bool isDeleted,
            DbfMemoResolver? memoResolver,
            long recordNumber)
        {
            var rowData = new object?[schema.Count + 1];
            var repairMessages = new List<string>();
            
            for (int i = 0; i < schema.Count; i++)
            {
                try
                {
                    var value = dbfReader.GetValue(i);
                    
                    rowData[i] = NormalizeValueForColumn(schema[i], value, repairMessages, memoResolver, recordNumber);
                }
                catch (Exception ex)
                {
                    if (schema[i].DbfFieldType == 'M' &&
                        memoResolver?.TryResolveMemo(schema[i], recordNumber, null, out var resolvedMemo, out var memoRepairMessage) == true)
                    {
                        rowData[i] = resolvedMemo;
                        if (!string.IsNullOrWhiteSpace(memoRepairMessage))
                        {
                            repairMessages.Add(memoRepairMessage);
                        }
                        continue;
                    }

                    rowData[i] = GetCorruptedFallbackValue(schema[i]);
                    repairMessages.Add($"{schema[i].OriginalName}: read failed, inserted fallback value ({ex.Message})");
                }
            }
            
            rowData[schema.Count] = isDeleted;
            return (rowData, repairMessages);
        }

        private object? NormalizeValueForColumn(
            DbfColumnInfo column,
            object? value,
            List<string> repairMessages,
            DbfMemoResolver? memoResolver = null,
            long recordNumber = 0)
        {
            if (column.DbfFieldType == 'M' &&
                memoResolver?.TryResolveMemo(column, recordNumber, value, out var resolvedMemo, out var memoRepairMessage) == true)
            {
                if (!string.IsNullOrWhiteSpace(memoRepairMessage))
                {
                    repairMessages.Add(memoRepairMessage);
                }

                return resolvedMemo;
            }

            if (value == null || value == DBNull.Value)
            {
                return DBNull.Value;
            }

            if (IsDateColumn(column))
            {
                return NormalizeDateValue(column, value, repairMessages);
            }

            if (IsNumericColumn(column))
            {
                return NormalizeNumericValue(column, value, repairMessages);
            }

            if (column.DbfFieldType == 'L' || column.ColumnType == typeof(bool))
            {
                return NormalizeBooleanValue(column, value, repairMessages);
            }

            if (value is string strValue)
            {
                var sanitized = SanitizeStringValue(strValue);
                if (column.DbfFieldType == 'M' && IsLikelyUnresolvedMemoPointer(sanitized))
                {
                    repairMessages.Add($"{column.OriginalName}: unresolved memo pointer detected, inserted NULL");
                    return DBNull.Value;
                }

                return sanitized;
            }

            return value;
        }

        private object NormalizeDateValue(DbfColumnInfo column, object value, List<string> repairMessages)
        {
            if (value is DateTime dateValue)
            {
                if (IsValidMySqlDate(dateValue))
                {
                    return dateValue;
                }

                repairMessages.Add($"{column.OriginalName}: invalid MySQL date '{dateValue:yyyy-MM-dd HH:mm:ss}', inserted NULL");
                return DBNull.Value;
            }

            var rawValue = value.ToString()?.Trim();
            if (string.IsNullOrWhiteSpace(rawValue))
            {
                return DBNull.Value;
            }

            if (DateTime.TryParse(rawValue, out var parsedDate) && IsValidMySqlDate(parsedDate))
            {
                repairMessages.Add($"{column.OriginalName}: date was stored/read as text '{rawValue}', parsed successfully");
                return parsedDate;
            }

            repairMessages.Add($"{column.OriginalName}: invalid date value '{rawValue}', inserted NULL");
            return DBNull.Value;
        }

        private object NormalizeNumericValue(DbfColumnInfo column, object value, List<string> repairMessages)
        {
            if (IsInvalidNumericValue(value))
            {
                repairMessages.Add($"{column.OriginalName}: invalid numeric value '{value}', inserted NULL");
                return DBNull.Value;
            }

            if (value is string rawString)
            {
                var rawValue = rawString.Trim();
                if (string.IsNullOrWhiteSpace(rawValue))
                {
                    return DBNull.Value;
                }

                if (decimal.TryParse(rawValue, out var parsedDecimal))
                {
                    repairMessages.Add($"{column.OriginalName}: numeric value was stored/read as text '{rawValue}', parsed successfully");
                    return parsedDecimal;
                }

                repairMessages.Add($"{column.OriginalName}: invalid numeric text '{rawValue}', inserted NULL");
                return DBNull.Value;
            }

            return value;
        }

        private object NormalizeBooleanValue(DbfColumnInfo column, object value, List<string> repairMessages)
        {
            if (value is bool boolValue)
            {
                return boolValue;
            }

            var rawValue = value.ToString()?.Trim();
            if (string.IsNullOrWhiteSpace(rawValue))
            {
                return DBNull.Value;
            }

            if (rawValue.Equals("T", StringComparison.OrdinalIgnoreCase) ||
                rawValue.Equals("Y", StringComparison.OrdinalIgnoreCase) ||
                rawValue.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                rawValue.Equals("TRUE", StringComparison.OrdinalIgnoreCase))
            {
                repairMessages.Add($"{column.OriginalName}: logical value was stored/read as text '{rawValue}', parsed successfully");
                return true;
            }

            if (rawValue.Equals("F", StringComparison.OrdinalIgnoreCase) ||
                rawValue.Equals("N", StringComparison.OrdinalIgnoreCase) ||
                rawValue.Equals("0", StringComparison.OrdinalIgnoreCase) ||
                rawValue.Equals("FALSE", StringComparison.OrdinalIgnoreCase))
            {
                repairMessages.Add($"{column.OriginalName}: logical value was stored/read as text '{rawValue}', parsed successfully");
                return false;
            }

            repairMessages.Add($"{column.OriginalName}: invalid logical value '{rawValue}', inserted NULL");
            return DBNull.Value;
        }

        private object? GetCorruptedFallbackValue(DbfColumnInfo column)
        {
            if (!IsTextColumn(column))
            {
                return DBNull.Value;
            }

            var marker = "CORRUPTED OLD DATA";
            if (column.Length <= 0 || column.Length >= marker.Length)
            {
                return marker;
            }

            return column.Length >= 7 ? "CORRUPT" : "BAD";
        }

        private bool IsTextColumn(DbfColumnInfo column)
        {
            return column.DbfFieldType == 'C' || column.DbfFieldType == 'M' || column.ColumnType == typeof(string);
        }

        private string SanitizeStringValue(string value)
        {
            return value
                .Replace("\0", "")
                .Replace("\u001a", "")
                .TrimEnd();
        }

        private bool IsLikelyUnresolvedMemoPointer(string value)
        {
            var trimmed = value.Trim();
            return trimmed.Length <= 8 &&
                   trimmed.Any(c => char.IsControl(c) || c == '\u0081' || c == '\u008D' || c == '\u008F' || c == '\u0090' || c == '\u009D');
        }

        private async Task<(MySqlTransaction? transaction, int skippedCount, int failedCount)> ProcessBatch(
            MySqlConnection connection,
            MySqlTransaction? transaction,
            string tableName,
            string columnNames,
            List<DbfColumnInfo> schema,
            List<object?[]> batchRows,
            List<long> batchRecordNumbers,
            MigrationMode migrationMode,
            int batchNumber,
            long totalRowCount,
            BulkInsertService bulkInsertService,
            RecordTrackingService recordTracking,
            CancellationToken cancellationToken)
        {
            if (transaction == null)
            {
                transaction = await connection.BeginTransactionAsync(cancellationToken);
            }

            _logger!.Log($"  → Processing batch #{batchNumber} ({batchRows.Count} records)...");

            BulkInsertResult insertResult;
            try
            {
                insertResult = await bulkInsertService.ExecuteBulkInsertAsync(
                    connection, transaction, tableName, columnNames, schema, batchRows, migrationMode, cancellationToken);
            }
            catch (Exception ex)
            {
                // Log full exception details immediately to make diagnostics easier
                try
                {
                    _logger!.Log($"  ❌ Bulk insert failed at batch #{batchNumber}: {ex.Message}");
                    _logger.Log($"  Exception details: {ex.ToString()}");
                    _logger.LogError(tableName, $"Batch #{batchNumber}", ex.Message, ex.ToString());

                    // Save problematic batch rows to skipped records for inspection
                    try
                    {
                        var reason = $"Batch #{batchNumber} failed: {ex.Message}";
                        for (int i = 0; i < batchRows.Count; i++)
                        {
                            var recordNumber = batchRecordNumbers.Count > i ? batchRecordNumbers[i] : totalRowCount - batchRows.Count + i + 1;
                            recordTracking.LogSkippedRowData(recordNumber, schema, batchRows[i], reason);
                        }
                        _logger.Log($"  Saved {batchRows.Count} batch rows to skipped records for inspection");
                    }
                    catch { }
                }
                catch { }

                // Rollback transaction if present then rethrow
                if (transaction != null)
                {
                    try
                    {
                        await transaction.RollbackAsync();
                        await transaction.DisposeAsync();
                    }
                    catch { }
                    transaction = null;
                }

                throw;
            }

            await transaction.CommitAsync(cancellationToken);
            await transaction.DisposeAsync();

            _logger.Log($"  ✓ Batch #{batchNumber} committed: {batchRows.Count} records saved");
            _logger.Log($"  📊 Total progress: {totalRowCount} rows migrated");

            // Log skipped rows (if any)
            if (insertResult.SkippedRows.Count > 0)
            {
                foreach (var skippedItem in insertResult.SkippedRows)
                {
                    try
                    {
                        var idx = skippedItem.Index;
                        var primaryId = skippedItem.PrimaryId;

                        var recordNumber = batchRecordNumbers.Count > idx ? batchRecordNumbers[idx] : totalRowCount - batchRows.Count + idx + 1;

                        var reason = primaryId != null
                            ? $"Duplicate row skipped during patch load. primary_id={primaryId}"
                            : "Duplicate row skipped during patch load";

                        recordTracking.LogSkippedRowData(recordNumber, schema, batchRows[idx], reason);
                    }
                    catch { }
                }

                _logger.Log($"  ⚠️ {insertResult.SkippedRows.Count} record(s) skipped in batch #{batchNumber} (logged to skipped files)");
            }

            LogRepairedRows(recordTracking, schema, batchRows, batchRecordNumbers, insertResult.RepairedRows, batchNumber, false);
            LogFailedRows(recordTracking, schema, batchRows, batchRecordNumbers, insertResult.FailedRows, batchNumber, totalRowCount, false);

            return (null, insertResult.SkippedRows.Count, insertResult.FailedRows.Count);
        }

        private async Task<(int skippedCount, int failedCount)> ProcessFinalBatch(
            MySqlConnection connection,
            MySqlTransaction? transaction,
            string tableName,
            string columnNames,
            List<DbfColumnInfo> schema,
            List<object?[]> batchRows,
            List<long> batchRecordNumbers,
            MigrationMode migrationMode,
            int batchNumber,
            long totalRowCount,
            BulkInsertService bulkInsertService,
            RecordTrackingService recordTracking,
            CancellationToken cancellationToken)
        {
            if (transaction == null)
            {
                transaction = await connection.BeginTransactionAsync(cancellationToken);
            }

            _logger!.Log($"  → Processing final batch #{batchNumber} ({batchRows.Count} records)...");
            
            var insertResult = await bulkInsertService.ExecuteBulkInsertAsync(
                connection, transaction, tableName, columnNames, schema, batchRows, migrationMode, cancellationToken);

            await transaction.CommitAsync(cancellationToken);
            await transaction.DisposeAsync();

            _logger.Log($"  ✓ Final batch committed: {batchRows.Count} records saved");
            _logger.Log($"  📊 Migration complete: {totalRowCount} total rows migrated");

            if (insertResult.SkippedRows.Count > 0)
            {
                foreach (var skippedItem in insertResult.SkippedRows)
                {
                    try
                    {
                        var idx = skippedItem.Index;
                        var primaryId = skippedItem.PrimaryId;
                        var recordNumber = batchRecordNumbers.Count > idx ? batchRecordNumbers[idx] : totalRowCount - batchRows.Count + idx + 1;

                        var reason = primaryId != null
                            ? $"Duplicate row skipped during patch load. primary_id={primaryId}"
                            : "Duplicate row skipped during patch load";

                        recordTracking.LogSkippedRowData(recordNumber, schema, batchRows[idx], reason);
                    }
                    catch { }
                }

                _logger.Log($"  ⚠️ {insertResult.SkippedRows.Count} record(s) skipped in final batch (logged to skipped files)");
            }

            LogRepairedRows(recordTracking, schema, batchRows, batchRecordNumbers, insertResult.RepairedRows, batchNumber, true);
            LogFailedRows(recordTracking, schema, batchRows, batchRecordNumbers, insertResult.FailedRows, batchNumber, totalRowCount, true);

            return (insertResult.SkippedRows.Count, insertResult.FailedRows.Count);
        }

        private void LogFailedRows(
            RecordTrackingService recordTracking,
            List<DbfColumnInfo> schema,
            List<object?[]> batchRows,
            List<long> batchRecordNumbers,
            List<BulkInsertFailedRow> failedRows,
            int batchNumber,
            long totalRowCount,
            bool isFinalBatch)
        {
            if (failedRows.Count == 0)
            {
                return;
            }

            foreach (var failedRow in failedRows)
            {
                try
                {
                    var recordNumber = batchRecordNumbers.Count > failedRow.Index ? batchRecordNumbers[failedRow.Index] : totalRowCount - batchRows.Count + failedRow.Index + 1;
                    var row = failedRow.Row.Length > 0 ? failedRow.Row : batchRows[failedRow.Index];
                    var identity = GetRowIdentity(schema, row);
                    var message = string.IsNullOrWhiteSpace(identity)
                        ? failedRow.Message
                        : $"{identity}; {failedRow.Message}";

                    recordTracking.LogErrorRowData(recordNumber, schema, row, message);
                    _logger!.Log($"  ⚠️ Record #{recordNumber} failed and was skipped safely. {message}");
                }
                catch { }
            }

            var label = isFinalBatch ? "final batch" : $"batch #{batchNumber}";
            _logger!.Log($"  ⚠️ {failedRows.Count} bad record(s) skipped safely in {label}; migration continued");
        }

        private string GetRowIdentity(List<DbfColumnInfo> schema, object?[] row)
        {
            var identityColumns = new[] { "primary_id", "id", "userid", "user_id", "code" };
            var parts = new List<string>();

            foreach (var identityColumn in identityColumns)
            {
                var index = schema.FindIndex(c =>
                    string.Equals(c.Name, identityColumn, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(c.OriginalName, identityColumn, StringComparison.OrdinalIgnoreCase));

                if (index >= 0 && index < row.Length)
                {
                    var value = row[index] == null || row[index] == DBNull.Value ? "NULL" : row[index]?.ToString();
                    parts.Add($"{schema[index].OriginalName}={value}");
                }
            }

            return parts.Count == 0 ? "" : string.Join(", ", parts);
        }

        private void LogRepairedRows(
            RecordTrackingService recordTracking,
            List<DbfColumnInfo> schema,
            List<object?[]> batchRows,
            List<long> batchRecordNumbers,
            List<BulkInsertRepairEvent> repairedRows,
            int batchNumber,
            bool isFinalBatch)
        {
            if (repairedRows.Count == 0)
            {
                return;
            }

            foreach (var repairedRow in repairedRows)
            {
                try
                {
                    if (repairedRow.Index.HasValue && repairedRow.Index.Value >= 0 && repairedRow.Index.Value < batchRows.Count)
                    {
                        var recordNumber = batchRecordNumbers.Count > repairedRow.Index.Value ? batchRecordNumbers[repairedRow.Index.Value] : repairedRow.Index.Value + 1;
                        var loggedRow = repairedRow.InsertedRow ?? batchRows[repairedRow.Index.Value];
                        recordTracking.LogRepairedRowData(recordNumber, schema, loggedRow, repairedRow.Message);
                    }
                    else
                    {
                        _logger!.Log($"  ⚠️ MySQL warning in batch #{batchNumber}: {repairedRow.Message}");
                    }
                }
                catch { }
            }

            var label = isFinalBatch ? "final batch" : $"batch #{batchNumber}";
            _logger!.Log($"  ⚠️ {repairedRows.Count} warning/repaired record event(s) in {label} (logged when row number was available)");
        }

        private void HandleRecordError(
            long recordNumber,
            DbfDataReader.DbfDataReader dbfReader,
            List<DbfColumnInfo> schema,
            Exception ex,
            string tableName,
            RecordTrackingService recordTracking)
        {
            var errorDetails = new StringBuilder();
            errorDetails.AppendLine($"Record #{recordNumber}:");
            for (int i = 0; i < schema.Count; i++)
            {
                try
                {
                    var value = dbfReader.GetValue(i);
                    errorDetails.AppendLine($"  {schema[i].Name} = {value ?? "NULL"}");
                }
                catch
                {
                    errorDetails.AppendLine($"  {schema[i].Name} = <error reading value>");
                }
            }
            errorDetails.AppendLine($"Error: {ex.Message}");
            
            // Include full exception details alongside per-field dump for diagnosis
            var fullDetails = new StringBuilder();
            fullDetails.AppendLine(errorDetails.ToString());
            fullDetails.AppendLine("Exception:");
            fullDetails.AppendLine(ex.ToString());
            _logger!.LogError(tableName, $"Record #{recordNumber}", ex.Message, fullDetails.ToString());
            recordTracking.LogErrorRecord(recordNumber, dbfReader, schema, ex.Message);
            _logger.Log($"  ⚠️ Error in record #{recordNumber}: {ex.Message} (logged to error files)");
        }

        private void LogTrackingFiles(RecordTrackingService recordTracking)
        {
            var (skippedPath, errorPath, repairedPath) = recordTracking.GetLogPaths();
            
            if (skippedPath != null)
            {
                _logger!.Log($"  📄 Skipped records saved to: {skippedPath}");
            }
            
            if (errorPath != null)
            {
                _logger!.Log($"  📄 Error records saved to: {errorPath}");
            }

            if (repairedPath != null)
            {
                _logger!.Log($"  📄 Repaired records saved to: {repairedPath}");
            }
        }
    }
}
