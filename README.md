# FoxPro to MySQL Migrator

A WPF application that migrates FoxPro (.dbf and .fpt) database files to MySQL with automatic datatype conversion and progress logging.

## Features

- **Multiple DBF File Support**: Reads all .dbf files in a selected folder
- **Automatic Datatype Mapping**: Converts FoxPro datatypes to appropriate MySQL types
- **Lowercase Column Names**: Automatically converts all column names to lowercase for MySQL
- **Detailed Column Logging**: Shows all columns found in each DBF file during migration
- **Safe Mode**: Option to use TEXT for strings to avoid truncation issues
- **Progress Logging**: Real-time logging of migration progress
- **Memo Field Support**: Handles .fpt memo files associated with .dbf files
- **Auto Database Creation**: Creates the target database if it doesn't exist
- **Configurable Defaults**: Set default connection string in code, editable at runtime
- **No Provider Dependencies**: Uses modern .NET library (DbfDataReader) - no OLE DB provider installation required!
- **?? Data Discrepancy Tracking**: Separate CSV logs for skipped and error records to track which records were not inserted
- **?? Resume/Checkpoint System**: Automatically saves progress and allows resuming interrupted migrations from the last completed table

## Data Discrepancy Tracking

The migrator creates organized folders to help you track data discrepancies:

### Output Structure

After migration, you'll find the logs on your **Desktop**:

```
Desktop/
??? FoxProMySqlMigrator_Logs/
    ??? YYYYMMDD_HHMMSS/
        ??? ErrorRecords/
        ?   ??? customers_errors.csv
        ?   ??? orders_errors.csv
        ?   ??? ...
        ??? SkippedRecords/
        ?   ??? customers_skipped.csv
        ?   ??? orders_skipped.csv
        ?   ??? ...
        ??? migration_errors.txt
```

### Error Records (ErrorRecords/)

Contains CSV files for each table with records that **failed to insert** due to errors:
- Data type mismatches
- Constraint violations
- String truncation errors
- Invalid data formats

**CSV Format:**
```csv
RecordNumber,CUSTOMER_ID,NAME,EMAIL,ErrorMessage
125,12345,"John Doe","invalid-email","Data too long for column 'email'"
```

### Skipped Records (SkippedRecords/)

Contains CSV files for each table with records that were **intentionally skipped**:
- Records marked as deleted in the DBF file (when "Skip Deleted Records" is enabled)

**CSV Format:**
```csv
RecordNumber,CUSTOMER_ID,NAME,EMAIL,Reason
89,99999,"Old Customer","old@email.com","Record marked as deleted in DBF file"
```

### Migration Errors Log

A text file with detailed error information for troubleshooting.

### How to Use These Logs

1. **Track Missing Data**: Compare record counts between source DBF and MySQL to identify discrepancies
2. **Fix Data Issues**: Review error records to identify data quality problems in your source DBF files
3. **Audit Skipped Records**: Verify that skipped deleted records are intentional
4. **Re-import Corrected Data**: After fixing data issues, you can use the CSV files to re-import specific records
5. **Easy Access**: All logs are conveniently saved on your Desktop with timestamp for each migration session

## Requirements

- .NET 10
- MySQL Server
- Required NuGet packages (automatically restored):
  - MySql.Data (9.2.0)
  - DbfDataReader (0.8.2)

**No Visual FoxPro OLE DB Provider required!** This application uses a modern, cross-platform DBF reader library.

## Configuration

### Setting Default Connection String

Edit the `AppSettings.cs` file to set your default connection parameters:

```csharp
public static class AppSettings
{
    public const string DefaultConnectionString = "Server=localhost;Database=targetdb;User Id=root;Password=yourpass;";
    public const string DefaultDatabaseName = "targetdb";
    public const bool DefaultSafeMode = true;
}
```

These values will be pre-filled when the application starts, but can be edited in the UI before running the migration.

## Usage

1. **Select FoxPro Folder**: Click "Browse..." to select the folder containing your .dbf files
2. **Configure MySQL Connection**: The connection string is pre-filled from `AppSettings.cs` but can be edited
3. **Set Target Database**: The database name is pre-filled but can be changed
4. **Check for Checkpoint**: If a previous migration was interrupted, you'll see a notification banner offering to resume
5. **Safe Mode**: 
   - Checked (default): Uses TEXT datatype for all string fields (prevents truncation)
   - Unchecked: Uses VARCHAR(255) for string fields
6. **Skip Deleted Records**: 
   - Checked (default): Records marked as deleted in DBF are skipped and logged to SkippedRecords folder
   - Unchecked: Deleted records are migrated with is_deleted=TRUE flag
7. **Click Migrate**: Start the migration process (or click "Resume Migration" if resuming)
8. **Review Logs**: After migration, check the ErrorRecords and SkippedRecords folders for data discrepancies

## Resume/Checkpoint Feature

### How It Works

The migration tool **automatically saves progress** after each table is completed. If the migration is stopped or interrupted:

1. **Automatic Detection**: When you open the tool again and enter the same FoxPro folder and database name, a yellow notification banner appears
2. **Resume Option**: Click "Resume Migration" to continue from the last completed table
3. **Start Fresh Option**: Click "Migrate" to start over (the old checkpoint will be overwritten)

### Checkpoint Storage

- **Location**: `Desktop/FoxProMySqlMigrator_Logs/checkpoint_[database].json`
- **Contains**: 
  - List of completed tables
  - Start time and last update time
  - Source folder and target database
- **Auto-cleanup**: Checkpoint file is automatically deleted when migration completes successfully

### Use Cases

? **Long migrations**: Migrate large datasets over multiple sessions  
? **Interruptions**: Resume after system shutdown, network issues, or manual stops  
? **Testing**: Test a few tables, review results, then continue  
? **Scheduled breaks**: Stop for lunch, resume later without losing progress  

### Example Scenario

```
Day 1 (3:00 PM):
- Start migration of 100 tables
- Complete 60 tables
- Click "Stop" to leave for the day
- Progress automatically saved ?

Day 2 (9:00 AM):
- Open migrator
- Enter same folder and database
- See notification: "Found incomplete migration: 60 of 100 tables completed"
- Click "Resume Migration"
- Continue from table 61 ?
```

### Important Notes

- ?? Checkpoints are **per database** - you can't resume a migration for a different database
- ?? If you change the source folder, the checkpoint won't match and you'll start fresh
- ?? In **Full Reload** mode, resuming will continue dropping and recreating tables (not re-migrate already completed tables)
- ? Checkpoint includes the original timestamp, so all logs stay in the same folder

## Column Name Conversion

**All column names are automatically converted to lowercase** when creating MySQL tables.

Examples:
- `IS_DELETED` ? `is_deleted`
- `CustomerName` ? `customername`
- `ORDER_ID` ? `order_id`

This ensures consistency and follows common MySQL naming conventions. The log will show both the original DBF column names and the converted MySQL names.

## Datatype Mapping

| .NET Type (from DBF) | MySQL Type (Safe Mode OFF) | MySQL Type (Safe Mode ON) |
|----------------------|---------------------------|---------------------------|
| string               | VARCHAR(255)              | TEXT                      |
| int / long           | INT                       | INT                       |
| decimal              | DECIMAL(18,4)             | DECIMAL(18,4)             |
| double / float       | DOUBLE                    | DOUBLE                    |
| DateTime             | DATETIME                  | DATETIME                  |
| bool                 | BOOLEAN                   | BOOLEAN                   |
| byte[]               | BLOB                      | BLOB                      |

## Migration Process

1. Application scans the selected folder for .dbf files
2. For each .dbf file:
   - Reads the schema (column names and datatypes)
   - **Logs all columns found** with original and converted names
   - Creates corresponding MySQL table with lowercase column names (drops if exists)
   - Copies all rows from FoxPro to MySQL
   - Logs progress and row count
3. Reports completion or any errors

## Checking DBF File Contents

The application now provides detailed logging of DBF file structure:

```
[10:30:15] Processing table: customers
[10:30:15]   Opened DBF file: customers.dbf
[10:30:15]   Columns found in DBF:
[10:30:15]     - Original: 'CUSTOMER_ID' -> MySQL: 'customer_id' (Type: Int32)
[10:30:15]     - Original: 'NAME' -> MySQL: 'name' (Type: String)
[10:30:15]     - Original: 'IS_DELETED' -> MySQL: 'is_deleted' (Type: Boolean)
[10:30:15]     - Original: 'CREATED_DATE' -> MySQL: 'created_date' (Type: DateTime)
```

This helps you verify:
- ? All columns are being detected
- ? Column datatypes are correct
- ? The `is_deleted` column (or any other column) exists in the source DBF

**If a column is missing**, it means it's not present in the original DBF file.

## Troubleshooting

### Data Discrepancies

If you notice missing records in MySQL:
1. **Check the SkippedRecords folder**: See which records were intentionally skipped (deleted records)
2. **Check the ErrorRecords folder**: See which records failed to insert and why
3. **Compare counts**: Total records = Migrated + Skipped + Errors
4. **Review error messages**: The CSV files contain the exact error for each failed record
5. **Fix source data**: Correct issues in DBF files and re-run migration if needed

### Missing Columns (e.g., "is_deleted")
If you expect a column but don't see it in the log:
1. Check the log output to see all columns actually in the DBF file
2. Verify the column exists in your source FoxPro database
3. The DBF file may have been exported without all columns

### Connection Errors
- Verify MySQL server is running
- Check connection string credentials
- Ensure the MySQL user has CREATE DATABASE and CREATE TABLE permissions

### Character Encoding Issues
- The application uses Windows-1252 encoding by default for FoxPro files
- Enable Safe Mode to use TEXT fields and strip null characters
- Safe Mode also trims whitespace from strings

### Performance
- Large tables may take time to migrate (one INSERT per row)
- Progress is shown in real-time in the log window
- For very large tables, consider using MySQL bulk loading features

### DBF File Format Issues
- Ensure .dbf files are valid FoxPro/dBase format
- The DbfDataReader library supports most DBF variants (dBase III, IV, FoxPro, etc.)
- Memo files (.fpt, .dbt) are automatically handled if present

## Example Connection Strings

**Local MySQL:**
```
Server=localhost;Database=mydb;User Id=root;Password=mypassword;
```

**Remote MySQL:**
```
Server=192.168.1.100;Port=3306;Database=mydb;User Id=admin;Password=secret;
```

**MySQL with SSL:**
```
Server=myserver.com;Database=mydb;User Id=user;Password=pass;SslMode=Required;
```

**AWS RDS MySQL:**
```
Server=mydb.xxxxx.ap-southeast-1.rds.amazonaws.com;Database=mydb;User Id=admin;Password=mypass;
```

## Notes

- **All column names are converted to lowercase**
- Existing tables in MySQL will be dropped and recreated
- All .dbf files in the selected folder will be processed
- Table names in MySQL will match the .dbf file names (without extension)
- The application handles null values and DBNull properly
- Memo fields (.fpt files) are automatically read if present
- Default connection settings can be configured in `AppSettings.cs`
- All settings can be modified in the UI before migration
- **Check the log to see all columns** found in your DBF files
- **Review ErrorRecords and SkippedRecords folders** to track data discrepancies
- **CSV files can be opened in Excel** for easy review and analysis
- **Stop debugging before migration**: If you see "code changes have not been applied" message, stop the debugger and rebuild

## Technical Details

This application uses:
- **DbfDataReader**: A modern, cross-platform .NET library for reading DBF files
- **MySql.Data**: Official MySQL connector for .NET
- **WPF**: For the user interface
- **Async/Await**: For non-blocking database operations

## License

This project is provided as-is for database migration purposes.
