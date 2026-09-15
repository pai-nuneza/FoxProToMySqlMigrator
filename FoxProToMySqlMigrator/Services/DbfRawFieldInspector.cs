using System.IO;
using System.Text;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal sealed class DbfRawFieldValue
    {
        public long RecordNumber { get; init; }
        public string ColumnName { get; init; } = "";
        public char FieldType { get; init; }
        public int Length { get; init; }
        public long Offset { get; init; }
        public byte[] Bytes { get; init; } = Array.Empty<byte>();
        public string Text { get; init; } = "";
        public string Hex => "0x" + Convert.ToHexString(Bytes);
    }

    internal sealed class DbfTableRawInfo
    {
        public byte Version { get; init; }
        public DateTime? LastUpdateDate { get; init; }
        public uint HeaderRecordCount { get; init; }
        public ushort HeaderLength { get; init; }
        public ushort RecordLength { get; init; }
        public long FileLength { get; init; }
        public long PhysicalRecordCapacity { get; init; }
    }

    internal sealed class DbfRawFieldInspector : IDisposable
    {
        private readonly FileStream _stream;
        private readonly Encoding _encoding;
        private readonly int[] _fieldOffsets;

        public DbfTableRawInfo TableInfo { get; }

        private DbfRawFieldInspector(
            FileStream stream,
            Encoding encoding,
            DbfTableRawInfo tableInfo,
            int[] fieldOffsets)
        {
            _stream = stream;
            _encoding = encoding;
            TableInfo = tableInfo;
            _fieldOffsets = fieldOffsets;
        }

        public static DbfRawFieldInspector? TryCreate(
            string dbfFilePath,
            List<DbfColumnInfo> schema,
            Encoding encoding)
        {
            try
            {
                var stream = new FileStream(dbfFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                if (stream.Length < 32)
                {
                    stream.Dispose();
                    return null;
                }

                var header = new byte[32];
                _ = stream.Read(header, 0, header.Length);

                var headerLength = BitConverter.ToUInt16(header, 8);
                var recordLength = BitConverter.ToUInt16(header, 10);
                if (headerLength == 0 || recordLength == 0)
                {
                    stream.Dispose();
                    return null;
                }

                var tableInfo = new DbfTableRawInfo
                {
                    Version = header[0],
                    LastUpdateDate = TryReadLastUpdateDate(header, out var lastUpdateDate) ? lastUpdateDate : null,
                    HeaderRecordCount = BitConverter.ToUInt32(header, 4),
                    HeaderLength = headerLength,
                    RecordLength = recordLength,
                    FileLength = stream.Length,
                    PhysicalRecordCapacity = Math.Max(0, stream.Length - headerLength) / recordLength
                };

                return new DbfRawFieldInspector(stream, encoding, tableInfo, BuildFieldOffsets(schema));
            }
            catch
            {
                return null;
            }
        }

        public bool TryReadField(
            long recordNumber,
            DbfColumnInfo column,
            out DbfRawFieldValue rawField)
        {
            rawField = new DbfRawFieldValue();
            if (recordNumber <= 0 ||
                column.Index < 0 ||
                column.Index >= _fieldOffsets.Length ||
                column.Length <= 0)
            {
                return false;
            }

            var offset = TableInfo.HeaderLength
                + ((recordNumber - 1) * (long)TableInfo.RecordLength)
                + 1
                + _fieldOffsets[column.Index];

            if (offset < 0 || offset + column.Length > _stream.Length)
            {
                return false;
            }

            var bytes = new byte[column.Length];
            _stream.Position = offset;
            var read = _stream.Read(bytes, 0, bytes.Length);
            if (read < bytes.Length)
            {
                return false;
            }

            rawField = new DbfRawFieldValue
            {
                RecordNumber = recordNumber,
                ColumnName = column.OriginalName,
                FieldType = column.DbfFieldType,
                Length = column.Length,
                Offset = offset,
                Bytes = bytes,
                Text = _encoding.GetString(bytes).Replace("\0", "").Trim()
            };

            return true;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        private static int[] BuildFieldOffsets(List<DbfColumnInfo> schema)
        {
            var offsets = new int[schema.Count];
            var offset = 0;
            for (var i = 0; i < schema.Count; i++)
            {
                offsets[i] = offset;
                offset += Math.Max(0, schema[i].Length);
            }

            return offsets;
        }

        private static bool TryReadLastUpdateDate(byte[] header, out DateTime lastUpdateDate)
        {
            lastUpdateDate = default;
            try
            {
                var year = 1900 + header[1];
                var month = header[2];
                var day = header[3];
                lastUpdateDate = new DateTime(year, month, day);
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
