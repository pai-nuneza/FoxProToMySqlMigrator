using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal class MySqlTypeMapper
    {
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
                    return MapCharacterType(column);

                default:
                    return column.ColumnType == typeof(string)
                        ? MapCharacterType(column)
                        : "LONGTEXT";
            }
        }

        private string MapCharacterType(DbfColumnInfo column)
        {
            if (column.Length <= 0)
            {
                return "VARCHAR(255)";
            }

            if (column.Length <= 255)
            {
                return $"VARCHAR({Math.Max(1, column.Length)})";
            }

            return column.Length <= 65_535 ? "TEXT" : "LONGTEXT";
        }
    }
}
