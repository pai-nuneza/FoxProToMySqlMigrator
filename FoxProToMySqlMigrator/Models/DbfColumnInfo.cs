namespace FoxProToMySqlMigrator.Models
{
    internal class DbfColumnInfo
    {
        public required string Name { get; set; }
        public string OriginalName { get; set; } = string.Empty;
        public required Type ColumnType { get; set; }
        public int Index { get; set; }

        // FoxPro/DBF native field type (C=Character, N=Numeric, D=Date, L=Logical, M=Memo, etc.)
        public char DbfFieldType { get; set; } = 'C';
        public int Length { get; set; } = 0;
        public int DecimalCount { get; set; } = 0;
    }
}
