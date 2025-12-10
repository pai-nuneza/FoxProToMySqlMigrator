using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal class MySqlTypeMapper
    {
        private static readonly string[] LargeTextColumnKeywords = new[]
        {
            "memo", "note", "notes",  "comment", "description", "particular", 
            "remarks", "detail", "content", "text", "message",
            "body", "summary", "narrative", "observation", "review",
            "address"  // Added address to large text fields
        };

        public string MapToMySqlType(DbfColumnInfo column, bool safeMode)
        {
            if (column.ColumnType == typeof(string))
            {
                if (safeMode)
                {
                    // Check if this is likely a large text field
                    if (IsLargeTextField(column))
                    {
                        // TEXT for large fields (up to 64KB) - good balance for reporting
                        // Using TEXT instead of MEDIUMTEXT for better performance
                        return "TEXT";
                    }
                    
                    // Regular string fields - use VARCHAR(500) for optimal reporting performance
                    // VARCHAR is indexed-friendly and perfect for WHERE, JOIN, ORDER BY clauses
                    return "VARCHAR(500)";
                }
                else
                {
                    // Safe mode OFF - strict VARCHAR(255)
                    // Maximum performance for reports with strict data limits
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

        private bool IsLargeTextField(DbfColumnInfo column)
        {
            var lowerName = column.Name.ToLower();
            var lowerOriginalName = column.OriginalName.ToLower();

            // Check if column name ends with _MEMO
            if (lowerOriginalName.EndsWith("_memo"))
            {
                return true;
            }

            // Check if column name contains any of the large text keywords
            foreach (var keyword in LargeTextColumnKeywords)
            {
                if (lowerName.Contains(keyword) || lowerOriginalName.Contains(keyword))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
