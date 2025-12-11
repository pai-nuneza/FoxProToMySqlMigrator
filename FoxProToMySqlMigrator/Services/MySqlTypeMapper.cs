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
            // Use native DBF field type for accurate mapping
            // DBF Field Types:
            // C = Character (string)
            // N = Numeric (can be integer or decimal)
            // F = Float
            // D = Date
            // T = DateTime
            // L = Logical (boolean)
            // M = Memo (large text)
            // G = General (blob/binary)
            // I = Integer
            // Y = Currency
            // B = Double
            
            switch (column.DbfFieldType)
            {
                case 'D': // Date field
                    return "DATE";
                
                case 'T': // DateTime field
                    return "DATETIME";
                
                case 'L': // Logical/Boolean field
                    return "BOOLEAN";
                
                case 'I': // Integer field
                    return "INT";
                
                case 'N': // Numeric field - could be integer or decimal
                    if (column.DecimalCount > 0)
                    {
                        // Has decimal places - use DECIMAL
                        return $"DECIMAL({column.Length},{column.DecimalCount})";
                    }
                    else if (column.Length <= 10)
                    {
                        // No decimal, length <= 10 - likely an integer
                        return "INT";
                    }
                    else
                    {
                        // No decimal, length > 10 - use BIGINT for safety
                        return "BIGINT";
                    }
                
                case 'F': // Float field
                case 'B': // Double field
                    return "DOUBLE";
                
                case 'Y': // Currency field
                    return "DECIMAL(19,4)"; // Standard for currency
                
                case 'M': // Memo field - always large text
                    return "TEXT";
                
                case 'G': // General/Binary field
                    return "BLOB";
                
                case 'C': // Character field
                default:  // Default to character handling
                    // Check if this is a memo-like field
                    if (IsLargeTextField(column))
                    {
                        return "TEXT";
                    }
                    
                    // Use the actual DBF field length from FoxPro
                    int fieldLength = column.Length > 0 ? column.Length : 255;
                    
                    if (safeMode)
                    {
                        // Safe mode: Add buffer to prevent truncation (20% extra or min +50)
                        int safeLength = Math.Max(fieldLength + 50, (int)(fieldLength * 1.2));
                        
                        // Cap at 65535 (TEXT range), use TEXT if exceeds VARCHAR max
                        if (safeLength > 65535)
                        {
                            return "TEXT";
                        }
                        
                        return $"VARCHAR({safeLength})";
                    }
                    else
                    {
                        // Safe mode OFF - use exact FoxPro field length
                        // Cap at 65535 (VARCHAR max in MySQL)
                        if (fieldLength > 65535)
                        {
                            return "TEXT";
                        }
                        
                        return $"VARCHAR({fieldLength})";
                    }
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
