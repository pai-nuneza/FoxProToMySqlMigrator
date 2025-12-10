namespace FoxProToMySqlMigrator.Models
{
    public class TableMigrationResult
    {
        public required string TableName { get; set; }
        public int RowCount { get; set; }
        public int ErrorCount { get; set; }
        public int DeletedCount { get; set; }
        public int SkippedCount { get; set; }
    }
}
