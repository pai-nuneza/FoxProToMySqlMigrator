# Memo Field Support in FoxPro to MySQL Migrator

## How Memo Fields Work

### DBF File Structure
- **Regular fields** (Character, Numeric, Date, etc.) are stored directly in the .dbf file
- **Memo fields** (large text/binary data) are stored in a separate memo file:
  - FoxPro uses `.fpt` files
  - dBase uses `.dbt` files

### Automatic Detection
The DbfDataReader library **automatically** handles memo files:
1. When you open a .dbf file, it looks for a matching .fpt or .dbt file
2. If found, memo field data is read from that file
3. Memo fields appear as regular string columns in the schema

### Migration Behavior

**What the migrator does:**
- ? Automatically detects .fpt/.dbt files in the same directory
- ? Logs whether a memo file was found
- ? Identifies potential memo fields by naming convention
- ? Maps memo fields to MySQL TEXT type (unlimited length)
- ? Reads and transfers all memo field content

**Example log output:**
```
[1/5] Processing table: customers
  Opened DBF file: customers.dbf (with .fpt memo file)
  Columns found: 8 columns (2 potential memo field(s))
    - 'CUST_ID' -> 'cust_id' (Int32)
    - 'NAME' -> 'name' (String)
    - 'NOTES' -> 'notes' (String) [MEMO FIELD]
    - 'COMMENTS' -> 'comments' (String) [MEMO FIELD]
    - DBF Deletion Flag -> 'is_deleted' (Boolean)
  Created table with 9 columns
  Progress: 1000 rows inserted...
  ? Completed: 5000 rows migrated
```

### MySQL Table Structure

For a table with memo fields:
```sql
CREATE TABLE customers (
    cust_id INT,
    name TEXT,              -- Regular string (safe mode)
    notes TEXT,             -- Memo field from .fpt
    comments TEXT,          -- Memo field from .fpt
    is_deleted BOOLEAN
);
```

### Troubleshooting

**If memo data is missing:**
1. **Check if .fpt file exists** - Must be in same folder as .dbf
2. **Check file names match** - customers.dbf requires customers.fpt (same name)
3. **Check for file corruption** - Try opening in FoxPro first
4. **Check encoding** - Uses Windows-1252 by default
5. **Review error log** - Any read errors will be logged

**Memo field naming conventions detected:**
- Fields ending with `_MEMO`
- Fields containing `memo`, `note`, `comment` in the name
- Any large text fields from FoxPro

### Safe Mode Impact

**Safe Mode ON (default):**
- ALL string fields ? MySQL TEXT
- Unlimited length for all text data
- ? Recommended for memo fields

**Safe Mode OFF:**
- Regular strings ? VARCHAR(255)
- Memo fields ? TEXT (automatically detected as needing unlimited length)
- ?? May truncate if regular fields exceed 255 chars

### Verification

After migration, verify memo field data:
```sql
-- Check if memo data was imported
SELECT cust_id, 
       LENGTH(notes) as notes_length,
       SUBSTRING(notes, 1, 100) as notes_preview
FROM customers
WHERE notes IS NOT NULL
LIMIT 10;
```

### File Requirements

For successful memo field migration:
```
FoxProFolder/
??? customers.dbf       ? Main data file
??? customers.fpt       ? Memo file (REQUIRED for memo fields)
??? orders.dbf
??? orders.fpt
??? products.dbf        ? No memo file (no memo fields)
```

### Technical Details

The DbfDataReader library:
- Supports FoxPro memo files (.fpt) - Visual FoxPro, FoxPro 2.x
- Supports dBase memo files (.dbt) - dBase III, IV, V
- Automatically handles memo file format differences
- Handles corrupted memo blocks gracefully
- Returns null for missing/corrupt memo data

### Error Handling

If memo field data cannot be read:
- The field value will be NULL or empty
- Error is logged to migration_errors_[timestamp].txt
- Migration continues with other records
- Check error log for specific memo block issues
