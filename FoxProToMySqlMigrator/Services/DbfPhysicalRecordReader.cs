using System.IO;
using System.Text;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal sealed class DbfPhysicalRecord
    {
        public long RecordNumber { get; init; }
        public bool IsDeleted { get; init; }
        public object?[] Values { get; init; } = Array.Empty<object?>();
        public DbfRawFieldValue?[] RawFields { get; init; } = Array.Empty<DbfRawFieldValue?>();
        public int Marker { get; init; }
    }

    internal sealed class DbfPhysicalRecordReader
    {
        private readonly Encoding _encoding;

        public DbfPhysicalRecordReader(Encoding encoding)
        {
            _encoding = encoding;
        }

        public IEnumerable<DbfPhysicalRecord> ReadRecords(
            string dbfFilePath,
            List<DbfColumnInfo> schema,
            long startRecordNumber,
            long? maxRecordNumber = null)
        {
            using var stream = new FileStream(dbfFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            if (stream.Length < 32)
            {
                yield break;
            }

            var header = new byte[32];
            _ = stream.Read(header, 0, header.Length);

            var headerRecordCount = BitConverter.ToUInt32(header, 4);
            var headerLength = BitConverter.ToUInt16(header, 8);
            var recordLength = BitConverter.ToUInt16(header, 10);
            if (headerLength == 0 || recordLength == 0)
            {
                yield break;
            }

            var physicalCapacity = Math.Max(0, stream.Length - headerLength) / recordLength;
            var lastRecordNumber = Math.Min(maxRecordNumber ?? headerRecordCount, physicalCapacity);
            if (startRecordNumber > lastRecordNumber)
            {
                yield break;
            }

            var recordBuffer = new byte[recordLength];
            for (var recordNumber = startRecordNumber; recordNumber <= lastRecordNumber; recordNumber++)
            {
                var offset = headerLength + ((recordNumber - 1) * (long)recordLength);
                if (offset < 0 || offset + recordLength > stream.Length)
                {
                    yield break;
                }

                stream.Position = offset;
                var read = stream.Read(recordBuffer, 0, recordBuffer.Length);
                if (read < recordLength)
                {
                    yield break;
                }

                var marker = recordBuffer[0];
                yield return new DbfPhysicalRecord
                {
                    RecordNumber = recordNumber,
                    Marker = marker,
                    IsDeleted = marker == 0x2A,
                    Values = ParseValues(recordBuffer, schema, recordNumber, offset, out var rawFields),
                    RawFields = rawFields
                };
            }
        }

        private object?[] ParseValues(
            byte[] recordBuffer,
            List<DbfColumnInfo> schema,
            long recordNumber,
            long recordOffset,
            out DbfRawFieldValue?[] rawFields)
        {
            var values = new object?[schema.Count];
            rawFields = new DbfRawFieldValue?[schema.Count];
            var offset = 1;

            for (var i = 0; i < schema.Count; i++)
            {
                var column = schema[i];
                var length = Math.Max(0, column.Length);
                if (length == 0 || offset + length > recordBuffer.Length)
                {
                    values[i] = DBNull.Value;
                    continue;
                }

                var rawBytes = new byte[length];
                Buffer.BlockCopy(recordBuffer, offset, rawBytes, 0, length);
                rawFields[i] = new DbfRawFieldValue
                {
                    RecordNumber = recordNumber,
                    ColumnName = column.OriginalName,
                    FieldType = column.DbfFieldType,
                    Length = length,
                    Offset = recordOffset + offset,
                    Bytes = rawBytes,
                    Text = _encoding.GetString(rawBytes).Replace("\0", "").Trim()
                };
                offset += length;

                values[i] = DecodeRawField(column, rawBytes);
            }

            return values;
        }

        private object? DecodeRawField(DbfColumnInfo column, byte[] rawBytes)
        {
            if (rawBytes.All(b => b == 0x00 || b == 0x20))
            {
                return DBNull.Value;
            }

            if (column.DbfFieldType == 'T')
            {
                return TryParseVisualFoxProDateTime(rawBytes, out var dateTimeValue)
                    ? dateTimeValue
                    : GetRawHexValue(rawBytes);
            }

            if (column.DbfFieldType == 'I' || column.DbfFieldType == '+')
            {
                return TryDecodeInt32(rawBytes, out var intValue)
                    ? intValue
                    : GetRawHexValue(rawBytes);
            }

            if (column.DbfFieldType == 'Y')
            {
                return TryDecodeCurrency(rawBytes, out var currencyValue)
                    ? currencyValue
                    : GetRawHexValue(rawBytes);
            }

            if (column.DbfFieldType == 'B' || column.DbfFieldType == 'O')
            {
                return TryDecodeDouble(rawBytes, out var doubleValue)
                    ? doubleValue
                    : GetRawHexValue(rawBytes);
            }

            if (column.DbfFieldType == 'Q' || column.DbfFieldType == 'W' || column.DbfFieldType == 'G')
            {
                return rawBytes;
            }

            var rawText = _encoding.GetString(rawBytes)
                .Replace("\0", "")
                .Trim();

            if (string.IsNullOrWhiteSpace(rawText))
            {
                return DBNull.Value;
            }

            return column.DbfFieldType switch
            {
                'D' => TryParseDbfDate(rawText, out var dateValue) ? dateValue : rawText,
                'L' => TryParseDbfLogical(rawText, out var logicalValue) ? logicalValue : rawText,
                'N' or 'F' => decimal.TryParse(rawText, out var decimalValue) ? decimalValue : rawText,
                'M' => rawText,
                'V' => rawText.TrimEnd(),
                _ => rawText.TrimEnd()
            };
        }

        private bool TryParseDbfDate(string rawText, out DateTime dateValue)
        {
            dateValue = default;
            return rawText.Length == 8
                && int.TryParse(rawText[..4], out var year)
                && int.TryParse(rawText[4..6], out var month)
                && int.TryParse(rawText[6..8], out var day)
                && TryCreateDate(year, month, day, out dateValue);
        }

        private bool TryCreateDate(int year, int month, int day, out DateTime dateValue)
        {
            try
            {
                if (year < 1000 || year > 9999)
                {
                    dateValue = default;
                    return false;
                }

                dateValue = new DateTime(year, month, day);
                return true;
            }
            catch
            {
                dateValue = default;
                return false;
            }
        }

        private bool TryParseDbfLogical(string rawText, out bool value)
        {
            value = false;
            if (rawText.Equals("T", StringComparison.OrdinalIgnoreCase) ||
                rawText.Equals("Y", StringComparison.OrdinalIgnoreCase) ||
                rawText.Equals("1", StringComparison.OrdinalIgnoreCase))
            {
                value = true;
                return true;
            }

            if (rawText.Equals("F", StringComparison.OrdinalIgnoreCase) ||
                rawText.Equals("N", StringComparison.OrdinalIgnoreCase) ||
                rawText.Equals("0", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
        }

        private bool TryParseVisualFoxProDateTime(byte[] rawBytes, out DateTime value)
        {
            value = default;
            if (rawBytes.Length < 8)
            {
                return false;
            }

            var julianDay = BitConverter.ToInt32(rawBytes, 0);
            var milliseconds = BitConverter.ToInt32(rawBytes, 4);
            if (julianDay <= 0 || milliseconds < 0 || milliseconds >= 86_400_000)
            {
                return false;
            }

            if (!TryConvertJulianDayNumber(julianDay, out var date))
            {
                return false;
            }

            try
            {
                value = date.AddMilliseconds(milliseconds);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private bool TryConvertJulianDayNumber(int julianDayNumber, out DateTime date)
        {
            date = default;
            try
            {
                var l = julianDayNumber + 68569;
                var n = (4 * l) / 146097;
                l -= (146097 * n + 3) / 4;
                var i = (4000 * (l + 1)) / 1461001;
                l = l - (1461 * i) / 4 + 31;
                var j = (80 * l) / 2447;
                var day = l - (2447 * j) / 80;
                l = j / 11;
                var month = j + 2 - (12 * l);
                var year = 100 * (n - 49) + i + l;

                date = new DateTime(year, month, day);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private bool TryDecodeInt32(byte[] rawBytes, out int value)
        {
            if (rawBytes.Length >= 4)
            {
                value = BitConverter.ToInt32(rawBytes, 0);
                return true;
            }

            value = default;
            return false;
        }

        private bool TryDecodeCurrency(byte[] rawBytes, out decimal value)
        {
            value = default;
            if (rawBytes.Length < 8)
            {
                return false;
            }

            value = BitConverter.ToInt64(rawBytes, 0) / 10000m;
            return true;
        }

        private bool TryDecodeDouble(byte[] rawBytes, out double value)
        {
            value = default;
            if (rawBytes.Length < 8)
            {
                return false;
            }

            value = BitConverter.ToDouble(rawBytes, 0);
            return !double.IsNaN(value) && !double.IsInfinity(value);
        }

        private string GetRawHexValue(byte[] rawBytes)
        {
            return "0x" + Convert.ToHexString(rawBytes);
        }
    }
}
