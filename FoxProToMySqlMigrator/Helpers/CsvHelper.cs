using System.IO;
using System.Text;

namespace FoxProToMySqlMigrator.Helpers
{
    internal static class CsvHelper
    {
        public static string EscapeCsvValue(string? value)
        {
            if (string.IsNullOrEmpty(value))
                return "";
            
            // Escape quotes and wrap in quotes if contains comma, quote, or newline
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n") || value.Contains("\r"))
            {
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            
            return value;
        }

        public static void WriteErrorRecord(
            StreamWriter writer,
            int recordNumber,
            List<(string columnName, string value)> columnData,
            string errorMessage)
        {
            var recordData = new List<string> { recordNumber.ToString() };
            recordData.AddRange(columnData.Select(c => EscapeCsvValue(c.value)));
            recordData.Add(EscapeCsvValue(errorMessage));
            writer.WriteLine(string.Join(",", recordData));
        }

        public static void WriteSkippedRecord(
            StreamWriter writer,
            int recordNumber,
            List<(string columnName, string value)> columnData,
            string reason)
        {
            var recordData = new List<string> { recordNumber.ToString() };
            recordData.AddRange(columnData.Select(c => EscapeCsvValue(c.value)));
            recordData.Add(reason);
            writer.WriteLine(string.Join(",", recordData));
        }

        public static string CreateCsvHeader(List<string> columnNames, string additionalColumn)
        {
            return "RecordNumber," + string.Join(",", columnNames.Select(EscapeCsvValue)) + "," + additionalColumn;
        }
    }
}
