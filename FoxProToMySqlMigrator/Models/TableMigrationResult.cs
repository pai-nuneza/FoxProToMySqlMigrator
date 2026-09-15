namespace FoxProToMySqlMigrator.Models
{
    public class TableMigrationResult
    {
        public required string TableName { get; set; }
        public long RowCount { get; set; }
        public long ErrorCount { get; set; }
        public long WarningCount { get; set; }
        public long TotalRecords { get; set; }
        public long ReadCount { get; set; }
        public long InsertedCount { get; set; }
        public long UpdatedCount { get; set; }
        public long DeletedCount { get; set; }
        public long DuplicateCount { get; set; }
        public long SkippedCount { get; set; }
        public long FailedCount { get; set; }
        public long? DbfTotalCount { get; set; }
        public long AccountedCount { get; set; }
        public long MissingCount { get; set; }
        public string CountStatus { get; set; } = "Unknown";
    }
}
