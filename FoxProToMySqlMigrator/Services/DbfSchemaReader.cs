using System.IO;
using System.Text;
using DbfDataReader;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal class DbfSchemaReader
    {
        private static readonly string[] LargeTextColumnKeywords = new[]
        {
            "memo", "note", "comment", "description", "particular", 
            "remarks", "detail", "content", "text", "message",
            "body", "summary", "narrative", "observation", "review",
            "address"  // Added address to large text fields
        };

        public List<DbfColumnInfo> GetTableSchema(DbfDataReader.DbfDataReader reader)
        {
            var columns = new List<DbfColumnInfo>();
            
            // Access the DbfTable property using reflection to get field metadata
            var dbfTableProperty = reader.GetType().GetProperty("DbfTable", 
                System.Reflection.BindingFlags.Instance | 
                System.Reflection.BindingFlags.NonPublic | 
                System.Reflection.BindingFlags.Public);
            
            object? dbfTable = dbfTableProperty?.GetValue(reader);
            System.Reflection.PropertyInfo? columnsProperty = null;
            object? columnsCollection = null;
            
            if (dbfTable != null)
            {
                columnsProperty = dbfTable.GetType().GetProperty("Columns", 
                    System.Reflection.BindingFlags.Instance | 
                    System.Reflection.BindingFlags.NonPublic | 
                    System.Reflection.BindingFlags.Public);
                columnsCollection = columnsProperty?.GetValue(dbfTable);
            }
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var columnType = reader.GetFieldType(i);
                
                // Try to get the native DBF field type
                char dbfFieldType = 'C'; // Default to Character
                int length = 0;
                int decimalCount = 0;
                
                if (columnsCollection != null && columnsCollection is System.Collections.IList list && i < list.Count)
                {
                    var column = list[i];
                    if (column != null)
                    {
                        // Get ColumnType property (this is the DBF native type like 'C', 'N', 'D', 'L', 'M')
                        var columnTypeProperty = column.GetType().GetProperty("ColumnType");
                        if (columnTypeProperty != null)
                        {
                            var dbfTypeValue = columnTypeProperty.GetValue(column);
                            if (dbfTypeValue != null)
                            {
                                dbfFieldType = Convert.ToChar(dbfTypeValue);
                            }
                        }
                        
                        // Get Length property
                        var lengthProperty = column.GetType().GetProperty("Length");
                        if (lengthProperty != null)
                        {
                            var lengthValue = lengthProperty.GetValue(column);
                            if (lengthValue != null)
                            {
                                length = Convert.ToInt32(lengthValue);
                            }
                        }
                        
                        // Get DecimalCount property
                        var decimalProperty = column.GetType().GetProperty("DecimalCount");
                        if (decimalProperty != null)
                        {
                            var decimalValue = decimalProperty.GetValue(column);
                            if (decimalValue != null)
                            {
                                decimalCount = Convert.ToInt32(decimalValue);
                            }
                        }
                    }
                }
                
                columns.Add(new DbfColumnInfo
                {
                    Name = columnName.ToLower(),
                    OriginalName = columnName,
                    ColumnType = columnType,
                    Index = i,
                    DbfFieldType = dbfFieldType,
                    Length = length,
                    DecimalCount = decimalCount
                });
            }

            return columns;
        }

        public (DbfDataReader.DbfDataReader reader, FileStream? memoStream) OpenDbfFile(
            string dbfFilePath, 
            out bool hasMemoFile,
            out string memoFileType)
        {
            var directory = Path.GetDirectoryName(dbfFilePath) ?? "";
            var fileNameWithoutExt = Path.GetFileNameWithoutExtension(dbfFilePath);
            var fptFile = Path.Combine(directory, fileNameWithoutExt + ".fpt");
            var dbtFile = Path.Combine(directory, fileNameWithoutExt + ".dbt");
            
            string? memoFilePath = null;
            if (File.Exists(fptFile))
            {
                memoFilePath = fptFile;
            }
            else if (File.Exists(dbtFile))
            {
                memoFilePath = dbtFile;
            }
            
            hasMemoFile = memoFilePath != null;
            memoFileType = memoFilePath != null ? Path.GetExtension(memoFilePath) : "none";
            
            var options = new DbfDataReaderOptions
            {
                Encoding = Encoding.GetEncoding(1252)
            };

            var dbfStream = new FileStream(dbfFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            FileStream? memoStream = null;
            DbfDataReader.DbfDataReader? dbfReader = null;
            
            try
            {
                if (hasMemoFile && memoFilePath != null)
                {
                    memoStream = new FileStream(memoFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                    dbfReader = new DbfDataReader.DbfDataReader(dbfStream, memoStream, options);
                }
                else
                {
                    dbfReader = new DbfDataReader.DbfDataReader(dbfStream, options);
                }

                return (dbfReader, memoStream);
            }
            catch
            {
                memoStream?.Dispose();
                dbfStream.Dispose();
                throw;
            }
        }

        public int CountLargeTextFields(List<DbfColumnInfo> schema)
        {
            return schema.Count(c => IsLargeTextField(c));
        }

        public bool IsLargeTextField(DbfColumnInfo column)
        {
            if (column.ColumnType != typeof(string))
            {
                return false;
            }

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
