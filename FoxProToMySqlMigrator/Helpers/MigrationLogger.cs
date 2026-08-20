using System.IO;
using System.Text;

namespace FoxProToMySqlMigrator.Helpers
{
    internal class MigrationLogger
    {
        private readonly string _errorLogPath;
        private readonly string _activityLogPath;
        public event Action<string>? LogMessage;

        public MigrationLogger(string errorLogPath, string? activityLogPath = null)
        {
            _errorLogPath = errorLogPath;
            _activityLogPath = activityLogPath ?? Path.Combine(
                Path.GetDirectoryName(errorLogPath) ?? "",
                "migration_log.txt");
        }

        public void Log(string message)
        {
            var logMessage = $"[{DateTime.Now:HH:mm:ss}] {message}";
            LogMessage?.Invoke(logMessage);

            try
            {
                File.AppendAllText(_activityLogPath, logMessage + Environment.NewLine);
            }
            catch
            {
                // If we can't write to activity log, still keep the UI log alive.
            }
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
