using System.Buffers.Binary;
using System.IO;
using System.Text;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal sealed class DbfMemoResolver : IDisposable
    {
        private const int DefaultFptBlockSize = 64;
        private const int DefaultDbtBlockSize = 512;
        private const int MaxMemoBytes = 16 * 1024 * 1024;

        private readonly FileStream _dbfStream;
        private readonly FileStream _memoStream;
        private readonly Encoding _encoding;
        private readonly ushort _headerLength;
        private readonly ushort _recordLength;
        private readonly string _memoExtension;
        private readonly int _memoBlockSize;
        private readonly Dictionary<int, FieldLayout> _fieldLayouts;

        private sealed class FieldLayout
        {
            public required int Offset { get; init; }
            public required int Length { get; init; }
        }

        private DbfMemoResolver(
            FileStream dbfStream,
            FileStream memoStream,
            Encoding encoding,
            ushort headerLength,
            ushort recordLength,
            string memoExtension,
            int memoBlockSize,
            Dictionary<int, FieldLayout> fieldLayouts)
        {
            _dbfStream = dbfStream;
            _memoStream = memoStream;
            _encoding = encoding;
            _headerLength = headerLength;
            _recordLength = recordLength;
            _memoExtension = memoExtension;
            _memoBlockSize = memoBlockSize;
            _fieldLayouts = fieldLayouts;
        }

        public static DbfMemoResolver? TryCreate(
            string dbfFilePath,
            List<DbfColumnInfo> schema,
            Encoding encoding)
        {
            if (!schema.Any(c => c.DbfFieldType == 'M'))
            {
                return null;
            }

            var memoFilePath = GetMemoFilePath(dbfFilePath);
            if (memoFilePath == null)
            {
                return null;
            }

            var dbfStream = new FileStream(dbfFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            FileStream? memoStream = null;

            try
            {
                if (dbfStream.Length < 32)
                {
                    dbfStream.Dispose();
                    return null;
                }

                var header = new byte[32];
                _ = dbfStream.Read(header, 0, header.Length);
                var headerLength = BitConverter.ToUInt16(header, 8);
                var recordLength = BitConverter.ToUInt16(header, 10);
                if (headerLength == 0 || recordLength == 0)
                {
                    dbfStream.Dispose();
                    return null;
                }

                memoStream = new FileStream(memoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                var extension = Path.GetExtension(memoFilePath).ToLowerInvariant();
                var blockSize = extension == ".fpt"
                    ? ReadFptBlockSize(memoStream)
                    : DefaultDbtBlockSize;

                return new DbfMemoResolver(
                    dbfStream,
                    memoStream,
                    encoding,
                    headerLength,
                    recordLength,
                    extension,
                    blockSize,
                    BuildFieldLayouts(schema));
            }
            catch
            {
                memoStream?.Dispose();
                dbfStream.Dispose();
                return null;
            }
        }

        public bool TryResolveMemo(
            DbfColumnInfo column,
            long recordNumber,
            object? currentValue,
            out object? resolvedValue,
            out string? repairMessage)
        {
            resolvedValue = null;
            repairMessage = null;

            try
            {
                if (column.DbfFieldType != 'M')
                {
                    return false;
                }

                if (!TryReadRawMemoPointer(column, recordNumber, out var pointerBytes))
                {
                    return false;
                }

                if (IsBlankPointer(pointerBytes))
                {
                    resolvedValue = DBNull.Value;
                    return true;
                }

                foreach (var pointer in GetPointerCandidates(pointerBytes))
                {
                    if (TryReadMemoBlock(pointer, out var memoText))
                    {
                        resolvedValue = string.IsNullOrWhiteSpace(memoText) ? DBNull.Value : memoText;
                        return true;
                    }
                }

                if (currentValue is string currentText && IsLikelyRawMemoPointerText(currentText))
                {
                    resolvedValue = DBNull.Value;
                    repairMessage = $"{column.OriginalName}: memo pointer could not be resolved, inserted NULL instead of pointer bytes";
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                resolvedValue = DBNull.Value;
                repairMessage = $"{column.OriginalName}: memo resolution failed safely, inserted NULL ({ex.Message})";
                return true;
            }
        }

        private static string? GetMemoFilePath(string dbfFilePath)
        {
            var directory = Path.GetDirectoryName(dbfFilePath) ?? "";
            var fileNameWithoutExt = Path.GetFileNameWithoutExtension(dbfFilePath);
            var fptFile = Path.Combine(directory, fileNameWithoutExt + ".fpt");
            var dbtFile = Path.Combine(directory, fileNameWithoutExt + ".dbt");

            if (File.Exists(fptFile))
            {
                return fptFile;
            }

            return File.Exists(dbtFile) ? dbtFile : null;
        }

        private static int ReadFptBlockSize(FileStream memoStream)
        {
            if (memoStream.Length < 8)
            {
                return DefaultFptBlockSize;
            }

            Span<byte> blockSizeBytes = stackalloc byte[2];
            memoStream.Position = 6;
            if (memoStream.Read(blockSizeBytes) != blockSizeBytes.Length)
            {
                return DefaultFptBlockSize;
            }

            var blockSize = BinaryPrimitives.ReadUInt16BigEndian(blockSizeBytes);
            return blockSize > 0 ? blockSize : DefaultFptBlockSize;
        }

        private static Dictionary<int, FieldLayout> BuildFieldLayouts(List<DbfColumnInfo> schema)
        {
            var layouts = new Dictionary<int, FieldLayout>();
            var offset = 1;

            foreach (var column in schema)
            {
                var length = Math.Max(0, column.Length);
                layouts[column.Index] = new FieldLayout
                {
                    Offset = offset,
                    Length = length
                };
                offset += length;
            }

            return layouts;
        }

        private bool TryReadRawMemoPointer(DbfColumnInfo column, long recordNumber, out byte[] pointerBytes)
        {
            pointerBytes = Array.Empty<byte>();
            if (!_fieldLayouts.TryGetValue(column.Index, out var layout) ||
                layout.Length <= 0 ||
                recordNumber <= 0)
            {
                return false;
            }

            var recordOffset = _headerLength + ((recordNumber - 1) * (long)_recordLength);
            var fieldOffset = recordOffset + layout.Offset;
            if (fieldOffset < 0 || fieldOffset + layout.Length > _dbfStream.Length)
            {
                return false;
            }

            pointerBytes = new byte[layout.Length];
            _dbfStream.Position = fieldOffset;
            return _dbfStream.Read(pointerBytes, 0, pointerBytes.Length) == pointerBytes.Length;
        }

        private static bool IsBlankPointer(byte[] pointerBytes)
        {
            return pointerBytes.All(b => b == 0x00 || b == 0x20);
        }

        private static IEnumerable<int> GetPointerCandidates(byte[] pointerBytes)
        {
            var seen = new HashSet<int>();

            var rawText = Encoding.ASCII.GetString(pointerBytes).Replace("\0", "").Trim();
            if (int.TryParse(rawText, out var asciiPointer) && asciiPointer > 0 && seen.Add(asciiPointer))
            {
                yield return asciiPointer;
            }

            if (pointerBytes.Length >= 4)
            {
                var littleEndianPointer = BitConverter.ToInt32(pointerBytes, 0);
                if (littleEndianPointer > 0 && seen.Add(littleEndianPointer))
                {
                    yield return littleEndianPointer;
                }

                var bigEndianPointer = BinaryPrimitives.ReadInt32BigEndian(pointerBytes.AsSpan(0, 4));
                if (bigEndianPointer > 0 && seen.Add(bigEndianPointer))
                {
                    yield return bigEndianPointer;
                }
            }
        }

        private bool TryReadMemoBlock(int pointer, out string memoText)
        {
            memoText = "";

            return _memoExtension == ".fpt"
                ? TryReadFptMemoBlock(pointer, out memoText)
                : TryReadDbtMemoBlock(pointer, out memoText);
        }

        private bool TryReadFptMemoBlock(int pointer, out string memoText)
        {
            memoText = "";
            var blockOffset = pointer * (long)_memoBlockSize;
            if (blockOffset < 0 || blockOffset + 8 > _memoStream.Length)
            {
                return false;
            }

            Span<byte> blockHeader = stackalloc byte[8];
            _memoStream.Position = blockOffset;
            if (_memoStream.Read(blockHeader) != blockHeader.Length)
            {
                return false;
            }

            var memoLength = BinaryPrimitives.ReadInt32BigEndian(blockHeader[4..8]);
            if (memoLength < 0 ||
                memoLength > MaxMemoBytes ||
                blockOffset + 8 + memoLength > _memoStream.Length)
            {
                return false;
            }

            var memoBytes = new byte[memoLength];
            return _memoStream.Read(memoBytes, 0, memoBytes.Length) == memoBytes.Length
                && TryDecodeMemoBytes(memoBytes, out memoText);
        }

        private bool TryReadDbtMemoBlock(int pointer, out string memoText)
        {
            memoText = "";
            var blockOffset = pointer * (long)_memoBlockSize;
            if (blockOffset < 0 || blockOffset >= _memoStream.Length)
            {
                return false;
            }

            var maxLength = (int)Math.Min(_memoStream.Length - blockOffset, MaxMemoBytes);
            var memoBytes = new byte[maxLength];
            _memoStream.Position = blockOffset;
            var read = _memoStream.Read(memoBytes, 0, memoBytes.Length);
            if (read <= 0)
            {
                return false;
            }

            var end = Array.IndexOf(memoBytes, (byte)0x1A, 0, read);
            var length = end >= 0 ? end : read;
            return TryDecodeMemoBytes(memoBytes.AsSpan(0, length).ToArray(), out memoText);
        }

        private bool TryDecodeMemoBytes(byte[] memoBytes, out string memoText)
        {
            memoText = _encoding.GetString(memoBytes)
                .Replace("\0", "")
                .Replace("\u001a", "")
                .TrimEnd();

            return true;
        }

        private static bool IsLikelyRawMemoPointerText(string value)
        {
            var trimmed = value.Trim();
            return trimmed.Length <= 8 &&
                   trimmed.Any(c => char.IsControl(c) || c == '\u0081' || c == '\u008D' || c == '\u008F' || c == '\u0090' || c == '\u009D');
        }

        public void Dispose()
        {
            _memoStream.Dispose();
            _dbfStream.Dispose();
        }
    }
}
