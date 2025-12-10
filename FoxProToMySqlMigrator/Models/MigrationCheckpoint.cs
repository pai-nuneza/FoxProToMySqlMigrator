namespace FoxProToMySqlMigrator.Models
{
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
}
