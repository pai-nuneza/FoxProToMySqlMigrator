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
            
            // Add index on is_deleted for faster queries
            columnDefs.Add("INDEX `idx_is_deleted` (`is_deleted`)");

            return columnDefs;
        }
    }
}
