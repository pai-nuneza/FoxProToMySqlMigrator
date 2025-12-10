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
            CancellationToken cancellationToken = default)
        {
            if (migrationMode == MigrationMode.FullReload)
            {
                // Drop and recreate table
                var dropCmd = new MySqlCommand($"DROP TABLE IF EXISTS `{tableName}`", connection);
                await dropCmd.ExecuteNonQueryAsync(cancellationToken);

                await CreateTableInternalAsync(connection, tableName, schema, safeMode, cancellationToken);
            }
            else
            {
                // Patch Load - create table only if it doesn't exist
                var columnDefs = BuildColumnDefinitions(schema, safeMode);

                var createTableSql = $"CREATE TABLE IF NOT EXISTS `{tableName}` ({string.Join(", ", columnDefs)})";
                var createCmd = new MySqlCommand(createTableSql, connection);
                await createCmd.ExecuteNonQueryAsync(cancellationToken);
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
