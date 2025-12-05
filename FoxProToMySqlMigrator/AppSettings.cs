namespace FoxProToMySqlMigrator
{
    public static class AppSettings
    {
        // MySQL server connection (without database)
        public const string DefaultServerConnection = "Server=ppsta-database.c6wrkcsmkp2i.ap-southeast-1.rds.amazonaws.com;User Id=admin;Password=<1eR3v1L;";
        
        // Target database name
        public const string DefaultDatabaseName = "test_db";
        
        // Full connection string (server + database)
        public static string DefaultConnectionString => $"{DefaultServerConnection}Database={DefaultDatabaseName};";
        
        public const bool DefaultSafeMode = true;
        public const bool DefaultSkipDeletedRecords = true;
    }
}
