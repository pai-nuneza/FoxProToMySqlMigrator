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
        private readonly string _repairedRecordsFolder;
        private readonly string _tableName;
        
        private StreamWriter? _skippedRecordsCsv;
        private StreamWriter? _errorRecordsCsv;
        private StreamWriter? _repairedRecordsCsv;
        private string? _skippedRecordsCsvPath;
        private string? _errorRecordsCsvPath;
        private string? _repairedRecordsCsvPath;

        public RecordTrackingService(string errorRecordsFolder, string skippedRecordsFolder, string repairedRecordsFolder, string tableName)
        {
            _errorRecordsFolder = errorRecordsFolder;
            _skippedRecordsFolder = skippedRecordsFolder;
            _repairedRecordsFolder = repairedRecordsFolder;
            _tableName = tableName;
        }

        public void LogSkippedRowData(long recordNumber, List<DbfColumnInfo> schema, object?[] rowData, string reason)
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
                    var val = rowData.Length > i ? rowData[i] : null;
                    columnData.Add((schema[i].OriginalName, val?.ToString() ?? "NULL"));
                }
                catch
                {
                    columnData.Add((schema[i].OriginalName, "ERROR_READING_VALUE"));
                }
            }

            CsvHelper.WriteSkippedRecord(_skippedRecordsCsv!, recordNumber, columnData, reason);
        }

        public void LogSkippedRecord(
            long recordNumber,
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
            long recordNumber,
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

        public void LogErrorRowData(long recordNumber, List<DbfColumnInfo> schema, object?[] rowData, string errorMessage)
        {
            if (_errorRecordsCsv == null)
            {
                InitializeErrorRecordsCsv(schema);
            }

            CsvHelper.WriteErrorRecord(_errorRecordsCsv!, recordNumber, BuildColumnData(schema, rowData), errorMessage);
        }

        public void LogRepairedRowData(long recordNumber, List<DbfColumnInfo> schema, object?[] rowData, string reason)
        {
            if (_repairedRecordsCsv == null)
            {
                InitializeRepairedRecordsCsv(schema);
            }

            CsvHelper.WriteSkippedRecord(_repairedRecordsCsv!, recordNumber, BuildColumnData(schema, rowData), reason);
        }

        private List<(string columnName, string value)> BuildColumnData(List<DbfColumnInfo> schema, object?[] rowData)
        {
            var columnData = new List<(string columnName, string value)>();
            for (int i = 0; i < schema.Count; i++)
            {
                try
                {
                    var val = rowData.Length > i ? rowData[i] : null;
                    columnData.Add((schema[i].OriginalName, val == null || val == DBNull.Value ? "NULL" : val.ToString() ?? ""));
                }
                catch
                {
                    columnData.Add((schema[i].OriginalName, "ERROR_READING_VALUE"));
                }
            }

            return columnData;
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

        private void InitializeRepairedRecordsCsv(List<DbfColumnInfo> schema)
        {
            _repairedRecordsCsvPath = Path.Combine(_repairedRecordsFolder, $"{_tableName}_repaired.csv");
            _repairedRecordsCsv = new StreamWriter(_repairedRecordsCsvPath, false, Encoding.UTF8);
            
            var header = CsvHelper.CreateCsvHeader(
                schema.Select(c => c.OriginalName).ToList(), 
                "RepairReason");
            _repairedRecordsCsv.WriteLine(header);
        }

        public (string? skippedPath, string? errorPath, string? repairedPath) GetLogPaths()
        {
            return (_skippedRecordsCsvPath, _errorRecordsCsvPath, _repairedRecordsCsvPath);
        }

        public void Dispose()
        {
            _skippedRecordsCsv?.Close();
            _skippedRecordsCsv?.Dispose();
            _errorRecordsCsv?.Close();
            _errorRecordsCsv?.Dispose();
            _repairedRecordsCsv?.Close();
            _repairedRecordsCsv?.Dispose();
        }
    }
}
