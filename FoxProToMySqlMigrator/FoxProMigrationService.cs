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
        public event Action<string>? LogMessage;
        public event Action<TableMigrationResult>? TableCompleted;
        
        private MigrationLogger? _logger;
        private string _errorRecordsFolder = "";
        private string _skippedRecordsFolder = "";
        private string _migrationTimestamp = "";
        private CheckpointService? _checkpointService;
        
        // Cache reflection lookups per table
        private System.Reflection.PropertyInfo? _cachedDbfRecordProperty;
        private System.Reflection.PropertyInfo? _cachedIsDeletedProperty;

        public async Task<MigrationCheckpoint?> LoadCheckpointAsync(string foxProFolder, string targetDatabase)
        {
            var desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            var checkpointFile = Path.Combine(desktopPath, "FoxProMySqlMigrator_Logs", $"checkpoint_{targetDatabase}.json");
            
            var checkpointService = new CheckpointService(checkpointFile);
            return await checkpointService.LoadCheckpointAsync(foxProFolder, targetDatabase);
        }

        public async Task MigrateAsync(
            string foxProFolder, 
            string mySqlConnectionString, 
            string targetDatabase, 
            bool safeMode,
            bool skipDeletedRecords,
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
                
                LogMigrationStart(foxProFolder, migrationMode, batchSize, skipDeletedRecords, mainLogsFolder, resumeFromCheckpoint);

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

                await ProcessTables(dbfFiles, mySqlConn, checkpoint, safeMode, skipDeletedRecords, migrationMode, batchSize, cancellationToken);

                // Mark as completed
                checkpoint.IsCompleted = true;
                await _checkpointService.SaveCheckpointAsync(checkpoint);
                _checkpointService.DeleteCheckpoint();

                _logger.Log("Migration completed successfully!");
                _logger.Log($"===========================================");
                _logger.Log($"📁 All logs saved to Desktop:");
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
                _logger?.LogError("CRITICAL", "Migration", ex.Message, ex.StackTrace ?? "");
                _logger?.Log($"💾 Progress saved! You can resume this migration later.");
                throw; // Re-throw so UI can handle it
            }
        }

        private (string mainLogsFolder, string checkpointFilePath) SetupLogFolders(string targetDatabase)
        {
            var desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
            var mainLogsFolder = Path.Combine(desktopPath, "FoxProMySqlMigrator_Logs", _migrationTimestamp);
            Directory.CreateDirectory(mainLogsFolder);
            
            var checkpointFolder = Path.Combine(desktopPath, "FoxProMySqlMigrator_Logs");
            Directory.CreateDirectory(checkpointFolder);
            var checkpointFilePath = Path.Combine(checkpointFolder, $"checkpoint_{targetDatabase}.json");
            
            _errorRecordsFolder = Path.Combine(mainLogsFolder, "ErrorRecords");
            Directory.CreateDirectory(_errorRecordsFolder);
            
            _skippedRecordsFolder = Path.Combine(mainLogsFolder, "SkippedRecords");
            Directory.CreateDirectory(_skippedRecordsFolder);

            return (mainLogsFolder, checkpointFilePath);
        }

        private void LogMigrationStart(
            string foxProFolder, 
            MigrationMode migrationMode, 
            int batchSize, 
            bool skipDeletedRecords,
            string mainLogsFolder,
            MigrationCheckpoint? resumeFromCheckpoint)
        {
            _logger!.Log($"Starting migration from {foxProFolder}");
            _logger.Log($"Migration Mode: {migrationMode}");
            _logger.Log($"Batch Size: {batchSize} records per commit");
            _logger.Log($"Skip Deleted Records: {skipDeletedRecords}");
            
            if (resumeFromCheckpoint != null)
            {
                _logger.Log($"📦 RESUMING migration from checkpoint");
                _logger.Log($"   Already completed: {resumeFromCheckpoint.CompletedTables.Count}/{resumeFromCheckpoint.TotalTables} tables");
                _logger.Log($"   Started: {resumeFromCheckpoint.StartTime:yyyy-MM-dd HH:mm:ss}");
            }
            
            _logger.Log($"Error records will be saved to: {_errorRecordsFolder}");
            _logger.Log($"Skipped records will be saved to: {_skippedRecordsFolder}");
            _logger.Log($"All migration logs saved to: {mainLogsFolder}");
        }

        private async Task ProcessTables(
            string[] dbfFiles,
            MySqlConnection mySqlConn,
            MigrationCheckpoint checkpoint,
            bool safeMode,
            bool skipDeletedRecords,
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
                
                await MigrateTableAsync(dbfFile, mySqlConn, safeMode, skipDeletedRecords, migrationMode, batchSize, cancellationToken);
                
                // Update checkpoint after successful table migration
                checkpoint.CompletedTables.Add(tableName);
                checkpoint.LastUpdateTime = DateTime.Now;
                await _checkpointService!.SaveCheckpointAsync(checkpoint);
            }
        }

        private async Task MigrateTableAsync(
            string dbfFilePath, 
            MySqlConnection mySqlConn, 
            bool safeMode,
            bool skipDeletedRecords,
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

                    var schema = schemaReader.GetTableSchema(dbfReader);
                    
                    LogSchemaInfo(schema, schemaReader);
                    
                    var tableService = new MySqlTableService();
                    await tableService.CreateTableAsync(mySqlConn, tableName, schema, safeMode, migrationMode, cancellationToken);
                    
                    var logMode = migrationMode == MigrationMode.FullReload ? "Full Reload" : "Patch Load";
                    _logger.Log($"  Created table with AUTO_INCREMENT primary_id and {schema.Count} data columns ({logMode})");

                    var (rowCount, errorCount, deletedCount, skippedCount) = await CopyDataBulkAsync(
                        dbfReader, mySqlConn, tableName, schema, safeMode, skipDeletedRecords, migrationMode, batchSize, cancellationToken);
                    
                    _logger.Log($"  ✓ Completed: {rowCount} rows migrated" + 
                        (skippedCount > 0 ? $", {skippedCount} deleted records skipped" : "") +
                        (deletedCount > 0 ? $", {deletedCount} marked as deleted" : "") +
                        (errorCount > 0 ? $", {errorCount} errors (see log file)" : ""));

                    // Fire TableCompleted event
                    TableCompleted?.Invoke(new TableMigrationResult
                    {
                        TableName = tableName,
                        RowCount = rowCount,
                        ErrorCount = errorCount,
                        DeletedCount = deletedCount,
                        SkippedCount = skippedCount
                    });
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
                _logger.LogError(tableName, "Table Migration", ex.Message, ex.StackTrace ?? "");

                // Fire TableCompleted event with error
                TableCompleted?.Invoke(new TableMigrationResult
                {
                    TableName = tableName,
                    RowCount = 0,
                    ErrorCount = 1,
                    DeletedCount = 0,
                    SkippedCount = 0
                });
            }
        }

        private void LogSchemaInfo(List<DbfColumnInfo> schema, DbfSchemaReader schemaReader)
        {
            var largeTextFieldCount = schemaReader.CountLargeTextFields(schema);
            
            _logger!.Log($"  Columns found: {schema.Count} columns" + 
                (largeTextFieldCount > 0 ? $" ({largeTextFieldCount} large text field(s) → TEXT)" : ""));
            
            foreach (var col in schema)
            {
                var isLargeTextField = schemaReader.IsLargeTextField(col);
                string typeInfo = col.ColumnType.Name;
                
                if (col.ColumnType == typeof(string))
                {
                    typeInfo = isLargeTextField ? "String → TEXT" : "String → VARCHAR(500)";
                }
                
                _logger.Log($"    - '{col.OriginalName}' -> '{col.Name}' ({typeInfo})");
            }
            _logger.Log($"    - DBF Deletion Flag -> 'is_deleted' (Boolean)");
        }

        private async Task<(int rowCount, int errorCount, int deletedCount, int skippedCount)> CopyDataBulkAsync(
            DbfDataReader.DbfDataReader dbfReader, 
            MySqlConnection mySqlConn, 
            string tableName, 
            List<DbfColumnInfo> schema, 
            bool safeMode,
            bool skipDeletedRecords,
            MigrationMode migrationMode,
            int batchSize,
            CancellationToken cancellationToken = default)
        {
            var rowCount = 0;
            var errorCount = 0;
            var deletedCount = 0;
            var skippedCount = 0;
            var recordNumber = 0;
            var batchNumber = 0;
            
            InitializeReflectionCache(dbfReader);
            
            var columnNames = BuildColumnNames(schema);
            var bulkInsertService = new BulkInsertService();
            
            using var recordTracking = new RecordTrackingService(_errorRecordsFolder, _skippedRecordsFolder, tableName);
            
            MySqlTransaction? transaction = null;
            var batchRows = new List<object?[]>();
            
            try
            {
                while (dbfReader.Read())
                {
                    if (recordNumber % 100 == 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    recordNumber++;
                    
                    try
                    {
                        bool isDeleted = GetIsDeletedFlag(dbfReader);

                        if (skipDeletedRecords && isDeleted)
                        {
                            skippedCount++;
                            recordTracking.LogSkippedRecord(recordNumber, dbfReader, schema, "Record marked as deleted in DBF file");
                            continue;
                        }

                        if (isDeleted)
                        {
                            deletedCount++;
                        }

                        var rowData = ExtractRowData(dbfReader, schema, isDeleted, safeMode);
                        batchRows.Add(rowData);
                        rowCount++;

                        if (batchRows.Count >= batchSize)
                        {
                            try
                            {
                                transaction = await ProcessBatch(
                                    mySqlConn, transaction, tableName, columnNames, schema, 
                                    batchRows, migrationMode, ++batchNumber, rowCount, 
                                    bulkInsertService, cancellationToken);
                                batchRows.Clear();
                            }
                            catch (Exception batchEx)
                            {
                                // Critical: Batch processing failed
                                _logger!.Log($"  ❌ CRITICAL: Batch processing failed at batch #{batchNumber}");
                                _logger.Log($"  Error: {batchEx.Message}");
                                
                                // Cleanup transaction
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
                                
                                // Re-throw to stop migration of this table
                                throw new Exception($"Batch processing failed at batch #{batchNumber}. This usually indicates a database connection issue or data corruption.", batchEx);
                            }
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        errorCount++;
                        HandleRecordError(recordNumber, dbfReader, schema, ex, tableName, recordTracking);
                        
                        // CRITICAL FIX: Rollback and dispose transaction on record error
                        if (transaction != null)
                        {
                            try
                            {
                                await transaction.RollbackAsync();
                                await transaction.DisposeAsync();
                            }
                            catch
                            {
                                // Ignore rollback errors
                            }
                            transaction = null;
                        }
                        
                        // Clear batch to start fresh after error
                        batchRows.Clear();
                    }
                }
                
                // Commit remaining records
                if (batchRows.Count > 0)
                {
                    try
                    {
                        await ProcessFinalBatch(
                            mySqlConn, transaction, tableName, columnNames, schema, 
                            batchRows, migrationMode, ++batchNumber, rowCount, 
                            bulkInsertService, cancellationToken);
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
                
                // Re-throw with better context
                throw new Exception($"Migration failed for table '{tableName}' at record #{recordNumber}. Check error logs for details.", ex);
            }

            return (rowCount, errorCount, deletedCount, skippedCount);
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
                catch
                {
                    // Silently ignore
                }
            }
            return isDeleted;
        }

        private object?[] ExtractRowData(
            DbfDataReader.DbfDataReader dbfReader, 
            List<DbfColumnInfo> schema, 
            bool isDeleted, 
            bool safeMode)
        {
            var rowData = new object?[schema.Count + 1];
            
            for (int i = 0; i < schema.Count; i++)
            {
                var value = dbfReader.GetValue(i);
                
                if (value == null || value == DBNull.Value)
                {
                    rowData[i] = DBNull.Value;
                }
                else if (value is string strValue && safeMode)
                {
                    rowData[i] = strValue.Replace("\0", "").Trim();
                }
                else
                {
                    rowData[i] = value;
                }
            }
            
            rowData[schema.Count] = isDeleted;
            return rowData;
        }

        private async Task<MySqlTransaction?> ProcessBatch(
            MySqlConnection connection,
            MySqlTransaction? transaction,
            string tableName,
            string columnNames,
            List<DbfColumnInfo> schema,
            List<object?[]> batchRows,
            MigrationMode migrationMode,
            int batchNumber,
            int totalRowCount,
            BulkInsertService bulkInsertService,
            CancellationToken cancellationToken)
        {
            if (transaction == null)
            {
                transaction = await connection.BeginTransactionAsync(cancellationToken);
            }

            _logger!.Log($"  → Processing batch #{batchNumber} ({batchRows.Count} records)...");
            
            await bulkInsertService.ExecuteBulkInsertAsync(
                connection, transaction, tableName, columnNames, schema, batchRows, migrationMode, cancellationToken);
            
            await transaction.CommitAsync(cancellationToken);
            await transaction.DisposeAsync();
            
            _logger.Log($"  ✓ Batch #{batchNumber} committed: {batchRows.Count} records saved");
            _logger.Log($"  📊 Total progress: {totalRowCount} rows migrated");
            
            return null;
        }

        private async Task ProcessFinalBatch(
            MySqlConnection connection,
            MySqlTransaction? transaction,
            string tableName,
            string columnNames,
            List<DbfColumnInfo> schema,
            List<object?[]> batchRows,
            MigrationMode migrationMode,
            int batchNumber,
            int totalRowCount,
            BulkInsertService bulkInsertService,
            CancellationToken cancellationToken)
        {
            if (transaction == null)
            {
                transaction = await connection.BeginTransactionAsync(cancellationToken);
            }

            _logger!.Log($"  → Processing final batch #{batchNumber} ({batchRows.Count} records)...");
            
            await bulkInsertService.ExecuteBulkInsertAsync(
                connection, transaction, tableName, columnNames, schema, batchRows, migrationMode, cancellationToken);
            
            await transaction.CommitAsync(cancellationToken);
            await transaction.DisposeAsync();
            
            _logger.Log($"  ✓ Final batch committed: {batchRows.Count} records saved");
            _logger.Log($"  📊 Migration complete: {totalRowCount} total rows migrated");
        }

        private void HandleRecordError(
            int recordNumber,
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
            
            _logger!.LogError(tableName, $"Record #{recordNumber}", ex.Message, errorDetails.ToString());
            recordTracking.LogErrorRecord(recordNumber, dbfReader, schema, ex.Message);
            _logger.Log($"  ⚠️ Error in record #{recordNumber}: {ex.Message} (logged to error files)");
        }

        private void LogTrackingFiles(RecordTrackingService recordTracking)
        {
            var (skippedPath, errorPath) = recordTracking.GetLogPaths();
            
            if (skippedPath != null)
            {
                _logger!.Log($"  📄 Skipped records saved to: {skippedPath}");
            }
            
            if (errorPath != null)
            {
                _logger!.Log($"  📄 Error records saved to: {errorPath}");
            }
        }
    }
}
