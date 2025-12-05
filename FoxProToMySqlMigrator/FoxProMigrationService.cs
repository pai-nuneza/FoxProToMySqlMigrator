using System.Data;
using System.IO;
using System.Text;
using System.Text.Json;
using DbfDataReader;
using MySql.Data.MySqlClient;

namespace FoxProToMySqlMigrator
{
    public enum MigrationMode
    {
        FullReload,
        PatchLoad
    }

    public class TableMigrationResult
    {
        public required string TableName { get; set; }
        public int RowCount { get; set; }
        public int ErrorCount { get; set; }
        public int DeletedCount { get; set; }
        public int SkippedCount { get; set; }
    }

    public class MigrationCheckpoint
    {
        public string FoxProFolder { get; set; } = "";
        public string TargetDatabase { get; set; } = "";
        public DateTime StartTime { get; set; }
        public DateTime LastUpdateTime { get; set; }
        public List<string> CompletedTables { get; set; } = new();
        public int TotalTables { get; set; }
        public bool IsCompleted { get; set; }
    }

    public class FoxProMigrationService
    {
        public event Action<string>? LogMessage;
        public event Action<TableMigrationResult>? TableCompleted;
        private string _errorLogPath = "";
        private string _errorRecordsFolder = "";
        private string _skippedRecordsFolder = "";
        private string _migrationTimestamp = "";
        private string _checkpointFilePath = "";
        
        // Cache reflection lookups per table
        private System.Reflection.PropertyInfo? _cachedDbfRecordProperty;
        private System.Reflection.PropertyInfo? _cachedIsDeletedProperty;

        public async Task<MigrationCheckpoint?> LoadCheckpointAsync(string foxProFolder, string targetDatabase)
        {
            try
            {
                var desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
                var checkpointFile = Path.Combine(desktopPath, "FoxProMySqlMigrator_Logs", $"checkpoint_{targetDatabase}.json");

                if (File.Exists(checkpointFile))
                {
                    var json = await File.ReadAllTextAsync(checkpointFile);
                    var checkpoint = JsonSerializer.Deserialize<MigrationCheckpoint>(json);
                    
                    // Validate checkpoint matches current migration
                    if (checkpoint != null && 
                        checkpoint.FoxProFolder == foxProFolder && 
                        checkpoint.TargetDatabase == targetDatabase &&
                        !checkpoint.IsCompleted)
                    {
                        return checkpoint;
                    }
                }
            }
            catch
            {
                // If checkpoint can't be loaded, return null
            }

            return null;
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
                Log($"Starting migration from {foxProFolder}");
                Log($"Migration Mode: {migrationMode}");
                Log($"Batch Size: {batchSize} records per commit");
                Log($"Skip Deleted Records: {skipDeletedRecords}");
                
                // Create timestamp for this migration session
                _migrationTimestamp = resumeFromCheckpoint?.StartTime.ToString("yyyyMMdd_HHmmss") 
                    ?? DateTime.Now.ToString("yyyyMMdd_HHmmss");
                
                // Create main logs folder on Desktop
                var desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
                var mainLogsFolder = Path.Combine(desktopPath, "FoxProMySqlMigrator_Logs", _migrationTimestamp);
                Directory.CreateDirectory(mainLogsFolder);
                
                // Setup checkpoint file
                var checkpointFolder = Path.Combine(desktopPath, "FoxProMySqlMigrator_Logs");
                Directory.CreateDirectory(checkpointFolder);
                _checkpointFilePath = Path.Combine(checkpointFolder, $"checkpoint_{targetDatabase}.json");
                
                // Create main error log file
                _errorLogPath = Path.Combine(mainLogsFolder, "migration_errors.txt");
                
                // Create error records folder
                _errorRecordsFolder = Path.Combine(mainLogsFolder, "ErrorRecords");
                Directory.CreateDirectory(_errorRecordsFolder);
                
                // Create skipped records folder
                _skippedRecordsFolder = Path.Combine(mainLogsFolder, "SkippedRecords");
                Directory.CreateDirectory(_skippedRecordsFolder);
                
                if (resumeFromCheckpoint != null)
                {
                    Log($"📦 RESUMING migration from checkpoint");
                    Log($"   Already completed: {resumeFromCheckpoint.CompletedTables.Count}/{resumeFromCheckpoint.TotalTables} tables");
                    Log($"   Started: {resumeFromCheckpoint.StartTime:yyyy-MM-dd HH:mm:ss}");
                }
                
                Log($"Error records will be saved to: {_errorRecordsFolder}");
                Log($"Skipped records will be saved to: {_skippedRecordsFolder}");
                Log($"All migration logs saved to: {mainLogsFolder}");
                
                if (!Directory.Exists(foxProFolder))
                {
                    Log($"ERROR: Folder does not exist: {foxProFolder}");
                    return;
                }

                var dbfFiles = Directory.GetFiles(foxProFolder, "*.dbf");
                Log($"Found {dbfFiles.Length} DBF file(s)");

                if (dbfFiles.Length == 0)
                {
                    Log("No DBF files found in the selected folder");
                    return;
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

                // Save initial checkpoint
                await SaveCheckpointAsync(checkpoint);

                // Check for cancellation
                cancellationToken.ThrowIfCancellationRequested();

                using var mySqlConn = new MySqlConnection(mySqlConnectionString);
                await mySqlConn.OpenAsync(cancellationToken);
                Log($"Connected to MySQL server");

                await EnsureDatabaseExistsAsync(mySqlConn, targetDatabase, cancellationToken);
                
                mySqlConn.ChangeDatabase(targetDatabase);
                Log($"Using database: {targetDatabase}");

                var totalTables = dbfFiles.Length;
                var currentTable = 0;

                foreach (var dbfFile in dbfFiles)
                {
                    var tableName = Path.GetFileNameWithoutExtension(dbfFile);
                    
                    // Skip already completed tables
                    if (checkpoint.CompletedTables.Contains(tableName))
                    {
                        currentTable++;
                        Log($"[{currentTable}/{totalTables}] ⏭️  Skipping already completed table: {tableName}");
                        continue;
                    }

                    // Check for cancellation before each table
                    cancellationToken.ThrowIfCancellationRequested();

                    currentTable++;
                    Log($"[{currentTable}/{totalTables}] Processing table: {tableName}");
                    
                    // Reset reflection cache for each table
                    _cachedDbfRecordProperty = null;
                    _cachedIsDeletedProperty = null;
                    
                    await MigrateTableAsync(dbfFile, mySqlConn, safeMode, skipDeletedRecords, migrationMode, batchSize, cancellationToken);
                    
                    // Update checkpoint after successful table migration
                    checkpoint.CompletedTables.Add(tableName);
                    checkpoint.LastUpdateTime = DateTime.Now;
                    await SaveCheckpointAsync(checkpoint);
                }

                // Mark as completed
                checkpoint.IsCompleted = true;
                await SaveCheckpointAsync(checkpoint);

                // Clean up checkpoint file after successful completion
                if (File.Exists(_checkpointFilePath))
                {
                    File.Delete(_checkpointFilePath);
                }

                Log("Migration completed successfully!");
                Log($"===========================================");
                Log($"📁 All logs saved to Desktop:");
                Log($"   {mainLogsFolder}");
                Log($"===========================================");
            }
            catch (OperationCanceledException)
            {
                Log("Migration cancelled by user.");
                Log($"💾 Progress saved! You can resume this migration later.");
                throw;
            }
            catch (Exception ex)
            {
                Log($"ERROR: {ex.Message}");
                Log($"Stack trace: {ex.StackTrace}");
                LogError("CRITICAL", "Migration", ex.Message, ex.StackTrace ?? "");
                Log($"💾 Progress saved! You can resume this migration later.");
            }
        }

        private async Task SaveCheckpointAsync(MigrationCheckpoint checkpoint)
        {
            try
            {
                var json = JsonSerializer.Serialize(checkpoint, new JsonSerializerOptions 
                { 
                    WriteIndented = true 
                });
                await File.WriteAllTextAsync(_checkpointFilePath, json);
            }
            catch
            {
                // If checkpoint can't be saved, continue migration
            }
        }

        private async Task EnsureDatabaseExistsAsync(MySqlConnection connection, string databaseName, CancellationToken cancellationToken = default)
        {
            var createDbCmd = new MySqlCommand($"CREATE DATABASE IF NOT EXISTS `{databaseName}`", connection);
            await createDbCmd.ExecuteNonQueryAsync(cancellationToken);
            Log($"Database '{databaseName}' ready");
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
            var tableName = Path.GetFileNameWithoutExtension(dbfFilePath);

            try
            {
                // Check for cancellation
                cancellationToken.ThrowIfCancellationRequested();

                // Check for associated memo file (.fpt or .dbt)
                var directory = Path.GetDirectoryName(dbfFilePath) ?? "";
                var fileNameWithoutExt = Path.GetFileNameWithoutExtension(dbfFilePath);
                var fptFile = Path.Combine(directory, fileNameWithoutExt + ".fpt");
                var dbtFile = Path.Combine(directory, fileNameWithoutExt + ".dbt");
                
                var hasMemoFile = File.Exists(fptFile) || File.Exists(dbtFile);
                var memoFileType = File.Exists(fptFile) ? ".fpt" : (File.Exists(dbtFile) ? ".dbt" : "none");
                
                var options = new DbfDataReaderOptions
                {
                    Encoding = Encoding.GetEncoding(1252)
                };

                // Single pass through DBF - read schema and data together
                using var dbfStream = new FileStream(dbfFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                using var dbfReader = new DbfDataReader.DbfDataReader(dbfStream, options);

                Log($"  Opened DBF file: {tableName}.dbf" + 
                    (hasMemoFile ? $" (with {memoFileType} memo file)" : ""));

                var schema = GetTableSchema(dbfReader);
                
                var memoFieldCount = schema.Count(c => c.ColumnType == typeof(string) && 
                    (c.OriginalName.EndsWith("_MEMO", StringComparison.OrdinalIgnoreCase) || 
                     c.Name.Contains("memo") || 
                     c.Name.Contains("note") ||
                     c.Name.Contains("comment")));
                
                // Log all column names found in the DBF file
                Log($"  Columns found: {schema.Count} columns" + 
                    (memoFieldCount > 0 ? $" ({memoFieldCount} potential memo field(s))" : ""));
                
                foreach (var col in schema)
                {
                    var isMemoCandidate = col.ColumnType == typeof(string) && 
                        (col.OriginalName.EndsWith("_MEMO", StringComparison.OrdinalIgnoreCase) || 
                         col.Name.Contains("memo") || 
                         col.Name.Contains("note") ||
                         col.Name.Contains("comment"));
                    
                    Log($"    - '{col.OriginalName}' -> '{col.Name}' ({col.ColumnType.Name})" +
                        (isMemoCandidate ? " [MEMO FIELD]" : ""));
                }
                Log($"    - DBF Deletion Flag -> 'is_deleted' (Boolean)");
                
                await CreateMySqlTableAsync(mySqlConn, tableName, schema, safeMode, migrationMode, cancellationToken);

                // Use optimized bulk insert for large tables
                var (rowCount, errorCount, deletedCount, skippedCount) = await CopyDataBulkAsync(
                    dbfReader, mySqlConn, tableName, schema, safeMode, skipDeletedRecords, migrationMode, batchSize, cancellationToken);
                
                Log($"  ✓ Completed: {rowCount} rows migrated" + 
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
            catch (OperationCanceledException)
            {
                Log($"  Migration of {tableName} cancelled.");
                throw;
            }
            catch (Exception ex)
            {
                Log($"  ✗ ERROR migrating {tableName}: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Log($"    Inner exception: {ex.InnerException.Message}");
                }
                LogError(tableName, "Table Migration", ex.Message, ex.StackTrace ?? "");

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

        private List<DbfColumnInfo> GetTableSchema(DbfDataReader.DbfDataReader reader)
        {
            var columns = new List<DbfColumnInfo>();
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var columnType = reader.GetFieldType(i);
                
                columns.Add(new DbfColumnInfo
                {
                    Name = columnName.ToLower(),
                    OriginalName = columnName,
                    ColumnType = columnType,
                    Index = i
                });
            }

            return columns;
        }

        private async Task CreateMySqlTableAsync(
            MySqlConnection connection, 
            string tableName, 
            List<DbfColumnInfo> schema, 
            bool safeMode,
            MigrationMode migrationMode,
            CancellationToken cancellationToken = default)
        {
            if (migrationMode == MigrationMode.FullReload)
            {
                // Drop and recreate table
                var dropCmd = new MySqlCommand($"DROP TABLE IF EXISTS `{tableName}`", connection);
                await dropCmd.ExecuteNonQueryAsync(cancellationToken);

                var columnDefs = new List<string>();
                
                // Add regular columns
                foreach (var col in schema)
                {
                    var mySqlType = MapToMySqlType(col, safeMode);
                    columnDefs.Add($"`{col.Name}` {mySqlType}");
                }
                
                // Add is_deleted column for DBF deletion flag
                columnDefs.Add("`is_deleted` BOOLEAN DEFAULT FALSE");

                var createTableSql = $"CREATE TABLE `{tableName}` ({string.Join(", ", columnDefs)})";
                var createCmd = new MySqlCommand(createTableSql, connection);
                await createCmd.ExecuteNonQueryAsync(cancellationToken);
                Log($"  Created table with {columnDefs.Count} columns (Full Reload)");
            }
            else
            {
                // Patch Load - create table only if it doesn't exist
                var columnDefs = new List<string>();
                
                foreach (var col in schema)
                {
                    var mySqlType = MapToMySqlType(col, safeMode);
                    columnDefs.Add($"`{col.Name}` {mySqlType}");
                }
                
                columnDefs.Add("`is_deleted` BOOLEAN DEFAULT FALSE");

                var createTableSql = $"CREATE TABLE IF NOT EXISTS `{tableName}` ({string.Join(", ", columnDefs)})";
                var createCmd = new MySqlCommand(createTableSql, connection);
                await createCmd.ExecuteNonQueryAsync(cancellationToken);
                Log($"  Verified table exists with {columnDefs.Count} columns (Patch Load)");
            }
        }

        private string MapToMySqlType(DbfColumnInfo column, bool safeMode)
        {
            if (column.ColumnType == typeof(string))
            {
                // If safe mode is ON, use smart detection
                if (safeMode)
                {
                    // Check if it's a memo field candidate - use TEXT
                    if (column.OriginalName.EndsWith("_MEMO", StringComparison.OrdinalIgnoreCase) ||
                        column.Name.Contains("memo") ||
                        column.Name.Contains("note") ||
                        column.Name.Contains("comment") ||
                        column.Name.Contains("description") ||
                        column.Name.Contains("remarks"))
                    {
                        return "TEXT";
                    }
                    // Regular string fields - use VARCHAR(500) with TEXT fallback
                    // MySQL will auto-upgrade if data exceeds 500 chars
                    return "VARCHAR(500)";
                }
                else
                {
                    // Safe mode OFF - strict VARCHAR(255)
                    return "VARCHAR(255)";
                }
            }
            else if (column.ColumnType == typeof(int) || column.ColumnType == typeof(long))
            {
                return "INT";
            }
            else if (column.ColumnType == typeof(decimal))
            {
                return "DECIMAL(18,4)";
            }
            else if (column.ColumnType == typeof(double) || column.ColumnType == typeof(float))
            {
                return "DOUBLE";
            }
            else if (column.ColumnType == typeof(DateTime))
            {
                return "DATETIME";
            }
            else if (column.ColumnType == typeof(bool))
            {
                return "BOOLEAN";
            }
            else if (column.ColumnType == typeof(byte[]))
            {
                return "BLOB";
            }
            else
            {
                return "TEXT";
            }
        }

        // OPTIMIZED: Bulk insert method for 80k+ records
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
            
            // Initialize reflection cache once per table
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
            
            // Build column names
            var allColumnNames = schema.Select(c => $"`{c.Name}`").ToList();
            allColumnNames.Add("`is_deleted`");
            var columnNames = string.Join(", ", allColumnNames);
            
            StreamWriter? skippedRecordsCsv = null;
            StreamWriter? errorRecordsCsv = null;
            string? skippedRecordsCsvPath = null;
            string? errorRecordsCsvPath = null;

            MySqlTransaction? transaction = null;
            var batchRows = new List<object?[]>();
            
            try
            {
                while (dbfReader.Read())
                {
                    // Check for cancellation periodically
                    if (recordNumber % 100 == 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    recordNumber++;
                    
                    try
                    {
                        // OPTIMIZED: Use cached reflection
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

                        // Skip deleted records if option is enabled
                        if (skipDeletedRecords && isDeleted)
                        {
                            skippedCount++;
                            
                            // Log skipped record to CSV
                            if (skippedRecordsCsv == null)
                            {
                                skippedRecordsCsvPath = Path.Combine(_skippedRecordsFolder, $"{tableName}_skipped.csv");
                                skippedRecordsCsv = new StreamWriter(skippedRecordsCsvPath, false, Encoding.UTF8);
                                
                                // Write header
                                var header = "RecordNumber," + string.Join(",", schema.Select(c => EscapeCsvValue(c.OriginalName))) + ",Reason";
                                skippedRecordsCsv.WriteLine(header);
                            }
                            
                            // Write record data
                            var recordData = new List<string> { recordNumber.ToString() };
                            for (int i = 0; i < schema.Count; i++)
                            {
                                try
                                {
                                    var value = dbfReader.GetValue(i);
                                    recordData.Add(EscapeCsvValue(value?.ToString() ?? "NULL"));
                                }
                                catch
                                {
                                    recordData.Add("ERROR_READING_VALUE");
                                }
                            }
                            recordData.Add("Record marked as deleted in DBF file");
                            skippedRecordsCsv.WriteLine(string.Join(",", recordData));
                            
                            continue;
                        }

                        if (isDeleted)
                        {
                            deletedCount++;
                        }

                        // OPTIMIZED: Collect rows for bulk insert
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
                                // OPTIMIZED: Simplified string cleaning
                                rowData[i] = strValue.Replace("\0", "").Trim();
                            }
                            else
                            {
                                rowData[i] = value;
                            }
                        }
                        rowData[schema.Count] = isDeleted;
                        
                        batchRows.Add(rowData);
                        rowCount++;

                        // OPTIMIZED: Execute bulk insert when batch is full
                        if (batchRows.Count >= batchSize)
                        {
                            if (transaction == null)
                            {
                                batchNumber++;
                                transaction = await mySqlConn.BeginTransactionAsync(cancellationToken);
                            }

                            Log($"  → Processing batch #{batchNumber} ({batchRows.Count} records)...");
                            
                            await ExecuteBulkInsertAsync(mySqlConn, transaction, tableName, columnNames, schema, batchRows, migrationMode, cancellationToken);
                            
                            await transaction.CommitAsync(cancellationToken);
                            await transaction.DisposeAsync();
                            transaction = null;
                            
                            Log($"  ✓ Batch #{batchNumber} committed: {batchRows.Count} records saved");
                            Log($"  📊 Total progress: {rowCount} rows migrated");
                            
                            batchRows.Clear();
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        errorCount++;
                        
                        // Build error details for main error log
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
                        
                        LogError(tableName, $"Record #{recordNumber}", ex.Message, errorDetails.ToString());
                        
                        // Log error record to CSV
                        if (errorRecordsCsv == null)
                        {
                            errorRecordsCsvPath = Path.Combine(_errorRecordsFolder, $"{tableName}_errors.csv");
                            errorRecordsCsv = new StreamWriter(errorRecordsCsvPath, false, Encoding.UTF8);
                            
                            // Write header
                            var header = "RecordNumber," + string.Join(",", schema.Select(c => EscapeCsvValue(c.OriginalName))) + ",ErrorMessage";
                            errorRecordsCsv.WriteLine(header);
                        }
                        
                        // Write record data
                        var recordData = new List<string> { recordNumber.ToString() };
                        for (int i = 0; i < schema.Count; i++)
                        {
                            try
                            {
                                var value = dbfReader.GetValue(i);
                                recordData.Add(EscapeCsvValue(value?.ToString() ?? "NULL"));
                            }
                            catch
                            {
                                recordData.Add("ERROR_READING_VALUE");
                            }
                        }
                        recordData.Add(EscapeCsvValue(ex.Message));
                        errorRecordsCsv.WriteLine(string.Join(",", recordData));
                        
                        Log($"  ⚠️ Error in record #{recordNumber}: {ex.Message} (logged to error files)");
                    }
                }
                
                // Commit remaining records
                if (batchRows.Count > 0)
                {
                    if (transaction == null)
                    {
                        batchNumber++;
                        transaction = await mySqlConn.BeginTransactionAsync(cancellationToken);
                    }

                    Log($"  → Processing final batch #{batchNumber} ({batchRows.Count} records)...");
                    
                    await ExecuteBulkInsertAsync(mySqlConn, transaction, tableName, columnNames, schema, batchRows, migrationMode, cancellationToken);
                    
                    await transaction.CommitAsync(cancellationToken);
                    await transaction.DisposeAsync();
                    
                    Log($"  ✓ Final batch committed: {batchRows.Count} records saved");
                    Log($"  📊 Migration complete: {rowCount} total rows migrated");
                }
            }
            catch (OperationCanceledException)
            {
                if (transaction != null)
                {
                    await transaction.RollbackAsync();
                    await transaction.DisposeAsync();
                    Log($"  ❌ Batch rolled back due to cancellation");
                }
                throw;
            }
            catch (Exception ex)
            {
                if (transaction != null)
                {
                    await transaction.RollbackAsync();
                    await transaction.DisposeAsync();
                }
                Log($"  ❌ Batch rolled back due to error: {ex.Message}");
                LogError(tableName, "Transaction", ex.Message, ex.StackTrace ?? "");
                throw;
            }
            finally
            {
                // Close CSV files
                if (skippedRecordsCsv != null)
                {
                    skippedRecordsCsv.Close();
                    skippedRecordsCsv.Dispose();
                    Log($"  📄 Skipped records saved to: {skippedRecordsCsvPath}");
                }
                
                if (errorRecordsCsv != null)
                {
                    errorRecordsCsv.Close();
                    errorRecordsCsv.Dispose();
                    Log($"  📄 Error records saved to: {errorRecordsCsvPath}");
                }
            }

            return (rowCount, errorCount, deletedCount, skippedCount);
        }

        // OPTIMIZED: Multi-value INSERT for bulk operations
        private async Task ExecuteBulkInsertAsync(
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
            cmd.CommandTimeout = 300; // 5 minutes for large batches
            
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
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private string EscapeCsvValue(string? value)
        {
            if (string.IsNullOrEmpty(value))
                return "";
            
            // Escape quotes and wrap in quotes if contains comma, quote, or newline
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n") || value.Contains("\r"))
            {
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            
            return value;
        }

        private void Log(string message)
        {
            LogMessage?.Invoke($"[{DateTime.Now:HH:mm:ss}] {message}");
        }

        private void LogError(string tableName, string context, string error, string details)
        {
            try
            {
                var logEntry = new StringBuilder();
                logEntry.AppendLine($"========================================");
                logEntry.AppendLine($"Timestamp: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                logEntry.AppendLine($"Table: {tableName}");
                logEntry.AppendLine($"Context: {context}");
                logEntry.AppendLine($"Error: {error}");
                logEntry.AppendLine($"Details:");
                logEntry.AppendLine(details);
                logEntry.AppendLine();

                File.AppendAllText(_errorLogPath, logEntry.ToString());
            }
            catch
            {
                // If we can't write to error log, just continue
            }
        }
    }

    internal class DbfColumnInfo
    {
        public required string Name { get; set; }
        public string OriginalName { get; set; } = string.Empty;
        public required Type ColumnType { get; set; }
        public int Index { get; set; }
    }
}
