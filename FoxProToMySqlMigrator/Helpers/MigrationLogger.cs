using System.IO;
using System.Text;

namespace FoxProToMySqlMigrator.Helpers
{
    internal class MigrationLogger
    {
        private readonly string _errorLogPath;
        public event Action<string>? LogMessage;

        public MigrationLogger(string errorLogPath)
        {
            _errorLogPath = errorLogPath;
        }

        public void Log(string message)
        {
            LogMessage?.Invoke($"[{DateTime.Now:HH:mm:ss}] {message}");
        }

        public void LogError(string tableName, string context, string error, string details)
        {
            try
            {
                var logEntry = new StringBuilder();
                logEntry.AppendLine($"========================================");
                logEntry.AppendLine($"Timestamp: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                logEntry.AppendLine($"Table: {tableName}");
                logEntry.AppendLine($"Context: {context}");
                logEntry.AppendLine($"Error: {error}");
                logEntry.AppendLine($"Details:");
                logEntry.AppendLine(details);
                logEntry.AppendLine();

                File.AppendAllText(_errorLogPath, logEntry.ToString());
            }
            catch
            {
                // If we can't write to error log, just continue
            }
        }
    }
}
