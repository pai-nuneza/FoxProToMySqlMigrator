using MySql.Data.MySqlClient;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal class MySqlTableService
    {
        private readonly MySqlTypeMapper _typeMapper;

        public MySqlTableService()
        {
            _typeMapper = new MySqlTypeMapper();
        }

        public async Task EnsureDatabaseExistsAsync(
            MySqlConnection connection, 
            string databaseName, 
            CancellationToken cancellationToken = default)
        {
            var createDbCmd = new MySqlCommand($"CREATE DATABASE IF NOT EXISTS `{databaseName}`", connection);
            await createDbCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        public async Task<bool> TableExistsAsync(
            MySqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            using var cmd = new MySqlCommand(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND LOWER(TABLE_NAME) = LOWER(@tableName)",
                connection);
            cmd.Parameters.AddWithValue("@tableName", tableName);

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt64(result ?? 0) > 0;
        }

        public async Task CreateTableAsync(
            MySqlConnection connection, 
            string tableName, 
            List<DbfColumnInfo> schema, 
            bool safeMode,
            MigrationMode migrationMode,
            bool preserveExistingRows = false,
            CancellationToken cancellationToken = default)
        {
            // Create table if it does not exist (both FullReload and PatchLoad)
            var columnDefs = BuildColumnDefinitions(schema, safeMode);
            var createTableSql = $"CREATE TABLE IF NOT EXISTS `{tableName}` ({string.Join(", ", columnDefs)})";
            var createCmd = new MySqlCommand(createTableSql, connection);
            await createCmd.ExecuteNonQueryAsync(cancellationToken);

            await EnsureMigrationRemarksColumnAsync(connection, tableName, cancellationToken);
            await WidenTextColumnsAsync(connection, tableName, schema, cancellationToken);

            // For FullReload mode, do NOT DROP the table. Instead TRUNCATE it to remove existing rows
            // while keeping the schema intact and avoiding accidental loss of schema-level objects.
            if (migrationMode == MigrationMode.FullReload && !preserveExistingRows)
            {
                var truncateCmd = new MySqlCommand($"TRUNCATE TABLE `{tableName}`", connection);
                await truncateCmd.ExecuteNonQueryAsync(cancellationToken);
                await SynchronizeColumnTypesAsync(connection, tableName, schema, safeMode, cancellationToken);
            }
        }

        public async Task DropTableIfExistsAsync(
            MySqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            var dropCmd = new MySqlCommand($"DROP TABLE IF EXISTS `{tableName}`", connection);
            await dropCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task WidenTextColumnsAsync(
            MySqlConnection connection,
            string tableName,
            List<DbfColumnInfo> schema,
            CancellationToken cancellationToken)
        {
            foreach (var column in schema.Where(IsTextColumn))
            {
                var alterCmd = new MySqlCommand($"ALTER TABLE `{tableName}` MODIFY COLUMN `{column.Name}` LONGTEXT", connection);
                await alterCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var column in schema.Where(IsBinaryColumn))
            {
                var alterCmd = new MySqlCommand($"ALTER TABLE `{tableName}` MODIFY COLUMN `{column.Name}` LONGBLOB", connection);
                await alterCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        private bool IsTextColumn(DbfColumnInfo column)
        {
            return column.DbfFieldType == 'C'
                || column.DbfFieldType == 'M'
                || column.DbfFieldType == 'V'
                || column.ColumnType == typeof(string);
        }

        private bool IsBinaryColumn(DbfColumnInfo column)
        {
            return column.DbfFieldType == 'G'
                || column.DbfFieldType == 'Q'
                || column.DbfFieldType == 'W'
                || column.ColumnType == typeof(byte[]);
        }

        private async Task SynchronizeColumnTypesAsync(
            MySqlConnection connection,
            string tableName,
            List<DbfColumnInfo> schema,
            bool safeMode,
            CancellationToken cancellationToken)
        {
            foreach (var column in schema)
            {
                var mySqlType = _typeMapper.MapToMySqlType(column, safeMode);
                var alterCmd = new MySqlCommand($"ALTER TABLE `{tableName}` MODIFY COLUMN `{column.Name}` {mySqlType}", connection);
                await alterCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        private async Task CreateTableInternalAsync(
            MySqlConnection connection,
            string tableName,
            List<DbfColumnInfo> schema,
            bool safeMode,
            CancellationToken cancellationToken)
        {
            var columnDefs = BuildColumnDefinitions(schema, safeMode);

            var createTableSql = $"CREATE TABLE `{tableName}` ({string.Join(", ", columnDefs)})";
            var createCmd = new MySqlCommand(createTableSql, connection);
            await createCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        private List<string> BuildColumnDefinitions(List<DbfColumnInfo> schema, bool safeMode)
        {
            var columnDefs = new List<string>();
            
            // Add auto-increment primary key as first column
            columnDefs.Add("`primary_id` INT AUTO_INCREMENT PRIMARY KEY");
            
            // Add regular columns
            foreach (var col in schema)
            {
                var mySqlType = _typeMapper.MapToMySqlType(col, safeMode);
                columnDefs.Add($"`{col.Name}` {mySqlType}");
            }
            
            // Add is_deleted column for DBF deletion flag
            columnDefs.Add("`is_deleted` BOOLEAN DEFAULT FALSE");

            columnDefs.Add("`migration_remarks` LONGTEXT NULL");
            
            // Add index on is_deleted for faster queries
            columnDefs.Add("INDEX `idx_is_deleted` (`is_deleted`)");

            return columnDefs;
        }

        private async Task EnsureMigrationRemarksColumnAsync(
            MySqlConnection connection,
            string tableName,
            CancellationToken cancellationToken)
        {
            try
            {
                var alterCmd = new MySqlCommand($"ALTER TABLE `{tableName}` ADD COLUMN `migration_remarks` LONGTEXT NULL", connection);
                await alterCmd.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (MySqlException ex) when (ex.Number == 1060)
            {
                // Column already exists.
            }
        }
    }
}
