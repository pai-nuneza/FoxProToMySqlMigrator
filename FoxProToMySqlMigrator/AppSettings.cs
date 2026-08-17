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
        
        // Safe Mode OFF by default - uses exact FoxPro field lengths for accurate migration
        public const bool DefaultSafeMode = false;
        // By default do NOT skip deleted records (include them in migration)
        public const bool DefaultSkipDeletedRecords = false;
    }
}
