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
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var columnType = reader.GetFieldType(i);
                
                columns.Add(new DbfColumnInfo
                {
                    Name = columnName.ToLower(),
                    OriginalName = columnName,
                    ColumnType = columnType,
                    Index = i
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
