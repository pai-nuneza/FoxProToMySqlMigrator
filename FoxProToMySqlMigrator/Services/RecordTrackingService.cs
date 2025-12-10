using System.IO;
using System.Text;
using FoxProToMySqlMigrator.Models;
using FoxProToMySqlMigrator.Helpers;

namespace FoxProToMySqlMigrator.Services
{
    internal class RecordTrackingService : IDisposable
    {
        private readonly string _errorRecordsFolder;
        private readonly string _skippedRecordsFolder;
        private readonly string _tableName;
        
        private StreamWriter? _skippedRecordsCsv;
        private StreamWriter? _errorRecordsCsv;
        private string? _skippedRecordsCsvPath;
        private string? _errorRecordsCsvPath;

        public RecordTrackingService(string errorRecordsFolder, string skippedRecordsFolder, string tableName)
        {
            _errorRecordsFolder = errorRecordsFolder;
            _skippedRecordsFolder = skippedRecordsFolder;
            _tableName = tableName;
        }

        public void LogSkippedRecord(
            int recordNumber,
            DbfDataReader.DbfDataReader dbfReader,
            List<DbfColumnInfo> schema,
            string reason)
        {
            if (_skippedRecordsCsv == null)
            {
                InitializeSkippedRecordsCsv(schema);
            }

            var columnData = new List<(string columnName, string value)>();
            for (int i = 0; i < schema.Count; i++)
            {
                try
                {
                    var value = dbfReader.GetValue(i);
                    columnData.Add((schema[i].OriginalName, value?.ToString() ?? "NULL"));
                }
                catch
                {
                    columnData.Add((schema[i].OriginalName, "ERROR_READING_VALUE"));
                }
            }

            CsvHelper.WriteSkippedRecord(_skippedRecordsCsv!, recordNumber, columnData, reason);
        }

        public void LogErrorRecord(
            int recordNumber,
            DbfDataReader.DbfDataReader dbfReader,
            List<DbfColumnInfo> schema,
            string errorMessage)
        {
            if (_errorRecordsCsv == null)
            {
                InitializeErrorRecordsCsv(schema);
            }

            var columnData = new List<(string columnName, string value)>();
            for (int i = 0; i < schema.Count; i++)
            {
                try
                {
                    var value = dbfReader.GetValue(i);
                    columnData.Add((schema[i].OriginalName, value?.ToString() ?? "NULL"));
                }
                catch
                {
                    columnData.Add((schema[i].OriginalName, "ERROR_READING_VALUE"));
                }
            }

            CsvHelper.WriteErrorRecord(_errorRecordsCsv!, recordNumber, columnData, errorMessage);
        }

        private void InitializeSkippedRecordsCsv(List<DbfColumnInfo> schema)
        {
            _skippedRecordsCsvPath = Path.Combine(_skippedRecordsFolder, $"{_tableName}_skipped.csv");
            _skippedRecordsCsv = new StreamWriter(_skippedRecordsCsvPath, false, Encoding.UTF8);
            
            var header = CsvHelper.CreateCsvHeader(
                schema.Select(c => c.OriginalName).ToList(), 
                "Reason");
            _skippedRecordsCsv.WriteLine(header);
        }

        private void InitializeErrorRecordsCsv(List<DbfColumnInfo> schema)
        {
            _errorRecordsCsvPath = Path.Combine(_errorRecordsFolder, $"{_tableName}_errors.csv");
            _errorRecordsCsv = new StreamWriter(_errorRecordsCsvPath, false, Encoding.UTF8);
            
            var header = CsvHelper.CreateCsvHeader(
                schema.Select(c => c.OriginalName).ToList(), 
                "ErrorMessage");
            _errorRecordsCsv.WriteLine(header);
        }

        public (string? skippedPath, string? errorPath) GetLogPaths()
        {
            return (_skippedRecordsCsvPath, _errorRecordsCsvPath);
        }

        public void Dispose()
        {
            _skippedRecordsCsv?.Close();
            _skippedRecordsCsv?.Dispose();
            _errorRecordsCsv?.Close();
            _errorRecordsCsv?.Dispose();
        }
    }
}
