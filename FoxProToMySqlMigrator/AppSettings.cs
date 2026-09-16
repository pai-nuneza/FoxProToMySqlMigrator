using System.IO;
using System.Text.Json;

namespace FoxProToMySqlMigrator
{
    public static class AppSettings
    {
        // MySQL server connection (without database)
        public const string DefaultServerConnection = "Server=ppsta-database.c6wrkcsmkp2i.ap-southeast-1.rds.amazonaws.com;User Id=admin;Password=<1eR3v1L;";
        
        // Target database name
        public const string DefaultDatabaseName = "ppsta_dev";

        // Default FoxPro folder
        public const string DefaultFoxProFolder = @"C:\Users\painu\Documents\ToMigrate\Inprogress";
        
        // Full connection string (server + database)
        public static string DefaultConnectionString => $"{DefaultServerConnection}Database={DefaultDatabaseName};";
        
        // Safe Mode ON by default - prioritizes preserving rows and avoids string truncation
        public const bool DefaultSafeMode = true;

        public static string AppDataFolder => Path.Combine(AppContext.BaseDirectory, "Data");

        public static string LegacyAppDataFolder => Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "FoxProToMySqlMigrator");

        public static string LogsFolder => Path.Combine(AppDataFolder, "Logs");

        public static string ConfigFilePath => Path.Combine(AppDataFolder, "config.json");
    }

    public sealed class UserAppConfig
    {
        public string MySqlServer { get; set; } = AppSettings.DefaultServerConnection;
        public string TargetDatabase { get; set; } = AppSettings.DefaultDatabaseName;
        public string FoxProFolder { get; set; } = AppSettings.DefaultFoxProFolder;
        public int BatchSize { get; set; } = 1000;
        public bool TableFilterEnabled { get; set; } = true;
    }

    public static class UserAppConfigStore
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented = true
        };

        public static UserAppConfig Load()
        {
            try
            {
                CopyLegacyConfigIfNeeded();

                if (!File.Exists(AppSettings.ConfigFilePath))
                {
                    return new UserAppConfig();
                }

                var json = File.ReadAllText(AppSettings.ConfigFilePath);
                return JsonSerializer.Deserialize<UserAppConfig>(json) ?? new UserAppConfig();
            }
            catch
            {
                return new UserAppConfig();
            }
        }

        public static void Save(UserAppConfig config)
        {
            Directory.CreateDirectory(AppSettings.AppDataFolder);
            Directory.CreateDirectory(AppSettings.LogsFolder);

            var json = JsonSerializer.Serialize(config, JsonOptions);
            File.WriteAllText(AppSettings.ConfigFilePath, json);
        }

        private static void CopyLegacyConfigIfNeeded()
        {
            if (File.Exists(AppSettings.ConfigFilePath))
            {
                return;
            }

            var legacyConfigPath = Path.Combine(AppSettings.LegacyAppDataFolder, "config.json");
            if (!File.Exists(legacyConfigPath))
            {
                return;
            }

            Directory.CreateDirectory(AppSettings.AppDataFolder);
            File.Copy(legacyConfigPath, AppSettings.ConfigFilePath, overwrite: false);
        }
    }
}
