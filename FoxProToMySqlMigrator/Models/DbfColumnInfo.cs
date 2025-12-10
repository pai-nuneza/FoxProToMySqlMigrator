namespace FoxProToMySqlMigrator.Models
{
    internal class DbfColumnInfo
    {
        public required string Name { get; set; }
        public string OriginalName { get; set; } = string.Empty;
        public required Type ColumnType { get; set; }
        public int Index { get; set; }
    }
}
