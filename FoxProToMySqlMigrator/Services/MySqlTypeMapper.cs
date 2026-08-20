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
            "address",  // Added address to large text fields
            "name"      // Treat name-like fields as large text to avoid truncation
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
                case '+': // Auto-increment integer field
                    return safeMode ? "DECIMAL(20,0)" : "INT";
                
                case 'N': // Numeric field - could be integer or decimal
                    int numericLength = Math.Clamp(column.Length > 0 ? column.Length : 19, 1, 65);
                    int numericScale = Math.Clamp(column.DecimalCount, 0, numericLength);

                    if (safeMode)
                    {
                        return $"DECIMAL({numericLength},{numericScale})";
                    }

                    if (column.DecimalCount > 0)
                    {
                        // Has decimal places - use a generous DECIMAL to avoid overflow/truncation
                        // Use a safe precision (total digits) while keeping the original scale (decimal places)
                        int precision = Math.Max(numericLength, 19);
                        return $"DECIMAL({precision},{column.DecimalCount})";
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
                case 'O': // Double field
                    return "DOUBLE";
                
                case 'Y': // Currency field
                    return "DECIMAL(19,4)"; // Standard for currency
                
                case 'M': // Memo field - always large text
                    return "LONGTEXT";
                
                case 'G': // General/Binary field
                case 'Q': // Varbinary
                case 'W': // Blob
                    return "LONGBLOB";
                
                case 'C': // Character field
                case 'V': // Varchar/varbinary-like text in Visual FoxPro
                default:  // Default to character handling
                    // Data integrity wins over compact schema. LONGTEXT is MySQL's largest text type.
                    return "LONGTEXT";
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
