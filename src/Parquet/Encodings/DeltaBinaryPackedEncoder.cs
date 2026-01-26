using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using Parquet.Data;
using Parquet.Extensions;
using Parquet.Meta;

namespace Parquet.Encodings {
    /// <summary>
    /// DELTA_BINARY_PACKED (https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5)
    /// fastparquet sample: https://github.com/dask/fastparquet/blob/c59e105537a8e7673fa30676dfb16d9fa5fb1cac/fastparquet/cencoding.pyx#L232
    /// golang sample: https://github.com/xitongsys/parquet-go/blob/62cf52a8dad4f8b729e6c38809f091cd134c3749/encoding/encodingread.go#L270
    ///
    /// Supported Types: short, ushort, int, uint, long, ulong
    /// </summary>
    static partial class DeltaBinaryPackedEncoder {

        public static bool IsSupported(System.Type t) =>
            t == typeof(int) || t == typeof(long) ||           // native types
            t == typeof(short) || t == typeof(ushort) ||       // int32 compatible
            t == typeof(uint) || t == typeof(ulong);           // int64 compatible

        /// <summary>
        /// Determines whether the specified data can be encoded using delta binary packed encoding.
        /// For ulong arrays, checks if all values are within the range of long.MaxValue.
        /// </summary>
        /// <param name="data">The input array to check</param>
        /// <param name="offset">Starting offset in the array</param>
        /// <param name="count">Number of elements to check</param>
        /// <returns>True if the data can be delta encoded, false otherwise</returns>
        public static bool CanEncode(Array data, int offset, int count) {
            System.Type elementType = data.GetType().GetElementType() ?? data.GetType();

            if (data.GetType() == typeof(ulong[])) {
                return count == 0 || CanEncodeULongArray((ulong[])data, offset, count);
            }

            return IsSupported(elementType);
        }

        public static bool CanEncode(Array data, int offset, int count, SchemaElement? tse) {
            System.Type? elementType = data.GetType().GetElementType();
            if(elementType == typeof(DateTime)) {
                return tse?.Type == Meta.Type.INT32 || tse?.Type == Meta.Type.INT64;
            }

            return CanEncode(data, offset, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CanEncodeULongArray(ulong[] data, int offset, int count) {
            const ulong maxValue = (ulong)long.MaxValue;
            int end = offset + count;

            for (int i = offset; i < end; i++) {
                if (data[i] > maxValue) {
                    return false;
                }
            }
            return true;
        }




        /// <summary>
        /// Encodes the provided data using a delta encoding scheme and writes it to the given destination stream.
        /// Optionally, collects statistics about the encoded data if the 'stats' parameter is provided.
        /// </summary>
        /// <param name="data">The input array to be encoded.</param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <param name="destination">The stream where the encoded data will be written.</param>
        /// <param name="stats">Optional parameter to collect statistics about the encoded data (can be null).</param>
        /// <exception cref="NotSupportedException"></exception>
        public static void Encode(Array data, int offset, int count, Stream destination, DataColumnStatistics? stats = null) {
            System.Type t = data.GetType();

            // Native types - no conversion needed
            if (t == typeof(int[])) {
                EncodeInt(((int[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((int[])data).AsSpan(offset, count), stats);
            }
            else if (t == typeof(long[])) {
                EncodeLong(((long[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((long[])data).AsSpan(offset, count), stats);
            }
            // Direct encoding for all supported types
            else if (t == typeof(short[])) {
                EncodeShort(((short[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((short[])data).AsSpan(offset, count), stats);
            }
            else if (t == typeof(ushort[])) {
                EncodeUshort(((ushort[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((ushort[])data).AsSpan(offset, count), stats);
            }
            else if (t == typeof(uint[])) {
                EncodeUint(((uint[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((uint[])data).AsSpan(offset, count), stats);
            }
            else if (t == typeof(ulong[])) {
                if (!CanEncodeULongArray((ulong[])data, offset, count)) {
                    throw new NotSupportedException($"ulong values exceed long.MaxValue range and cannot be encoded with {Encoding.DELTA_BINARY_PACKED}. Use plain encoding instead.");
                }
                EncodeUlong(((ulong[])data).AsSpan(offset, count), destination, 1024, 32);
                if (stats != null)
                    ParquetPlainEncoder.FillStats(((ulong[])data).AsSpan(offset, count), stats);
            }
            else {
                throw new NotSupportedException($"type {t} is not supported in {Encoding.DELTA_BINARY_PACKED}");
            }
        }

        public static void Encode(Array data, int offset, int count, Stream destination, SchemaElement? tse, DataColumnStatistics? stats = null) {
            if(data is DateTime[] dateTimes) {
                if(tse == null) {
                    throw new NotSupportedException("schema element is required to encode DateTime with DELTA_BINARY_PACKED");
                }
                EncodeDateTime(dateTimes.AsSpan(offset, count), tse, destination, stats);
                return;
            }

            Encode(data, offset, count, destination, stats);
        }




        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount, out int consumedBytes) {
            if (s.Length == 0 && valueCount == 0) {
                consumedBytes = 0;
                return 0;
            }

            System.Type? elementType = dest.GetType().GetElementType();
            if (elementType == null) {
                throw new NotSupportedException($"element type {elementType} is not supported");
            }

            // Native types - no conversion needed
            if (elementType == typeof(long)) {
                return DecodeLong(s, ((long[])dest).AsSpan(destOffset), out consumedBytes);
            }
            if (elementType == typeof(int)) {
                return DecodeInt(s, ((int[])dest).AsSpan(destOffset), out consumedBytes);
            }

            // Direct decoding for all supported types
            if (elementType == typeof(short)) {
                return DecodeShort(s, ((short[])dest).AsSpan(destOffset), out consumedBytes);
            }
            if (elementType == typeof(ushort)) {
                return DecodeUshort(s, ((ushort[])dest).AsSpan(destOffset), out consumedBytes);
            }
            if (elementType == typeof(uint)) {
                return DecodeUint(s, ((uint[])dest).AsSpan(destOffset), out consumedBytes);
            }
            if (elementType == typeof(ulong)) {
                return DecodeUlong(s, ((ulong[])dest).AsSpan(destOffset), out consumedBytes);
            }

            throw new NotSupportedException($"element type {elementType} is not supported in {Encoding.DELTA_BINARY_PACKED}");
        }

        public static int Decode(Span<byte> s, Array dest, int destOffset, int valueCount, SchemaElement? tse, out int consumedBytes) {
            if (s.Length == 0 && valueCount == 0) {
                consumedBytes = 0;
                return 0;
            }

            System.Type? elementType = dest.GetType().GetElementType();
            if (elementType == null) {
                throw new NotSupportedException($"element type {elementType} is not supported");
            }

            if(elementType == typeof(DateTime)) {
                if(tse == null) {
                    throw new NotSupportedException("schema element is required to decode DateTime with DELTA_BINARY_PACKED");
                }
                return DecodeDateTime(s, (DateTime[])dest, destOffset, valueCount, tse, out consumedBytes);
            }

            return Decode(s, dest, destOffset, valueCount, out consumedBytes);
        }

        private static void EncodeDateTime(ReadOnlySpan<DateTime> data, SchemaElement tse, Stream destination, DataColumnStatistics? stats) {
            if(stats != null) {
                ParquetPlainEncoder.FillStats(data, stats);
            }

            if(tse.Type == Meta.Type.INT32) {
                if(data.Length == 0) {
                    Encode(Array.Empty<int>(), 0, 0, destination, null);
                    return;
                }

                int[] rented = ArrayPool<int>.Shared.Rent(data.Length);
                try {
                    for(int i = 0; i < data.Length; i++) {
                        rented[i] = data[i].ToUnixDays();
                    }
                    Encode(rented, 0, data.Length, destination, null);
                } finally {
                    ArrayPool<int>.Shared.Return(rented);
                }
                return;
            }

            if(tse.Type != Meta.Type.INT64) {
                throw new ParquetException($"cannot delta encode DateTime with physical type {tse.Type}");
            }

            if(data.Length == 0) {
                Encode(Array.Empty<long>(), 0, 0, destination, null);
                return;
            }

            long[] rentedLongs = ArrayPool<long>.Shared.Rent(data.Length);
            try {
                if(tse.LogicalType?.TIMESTAMP is not null) {
                    bool adjustToUtc = tse.LogicalType.TIMESTAMP.IsAdjustedToUTC;
                    for(int i = 0; i < data.Length; i++) {
                        DateTime dt = adjustToUtc ? data[i].ToUtc() : data[i];
                        if(tse.LogicalType.TIMESTAMP.Unit.MILLIS is not null) {
                            rentedLongs[i] = dt.ToUnixMilliseconds();
#if NET7_0_OR_GREATER
                        } else if(tse.LogicalType.TIMESTAMP.Unit.MICROS is not null) {
                            rentedLongs[i] = dt.ToUnixMicroseconds();
                        } else if(tse.LogicalType.TIMESTAMP.Unit.NANOS is not null) {
                            rentedLongs[i] = dt.ToUnixNanoseconds();
#endif
                        } else {
                            throw new ParquetException($"Unexpected TimeUnit: {tse.LogicalType.TIMESTAMP.Unit}");
                        }
                    }
                } else if(tse.ConvertedType == ConvertedType.TIMESTAMP_MILLIS) {
                    for(int i = 0; i < data.Length; i++) {
                        rentedLongs[i] = data[i].ToUtc().ToUnixMilliseconds();
                    }
#if NET7_0_OR_GREATER
                } else if(tse.ConvertedType == ConvertedType.TIMESTAMP_MICROS) {
                    for(int i = 0; i < data.Length; i++) {
                        rentedLongs[i] = data[i].ToUtc().ToUnixMicroseconds();
                    }
#endif
                } else {
                    throw new ArgumentException($"invalid converted type: {tse.ConvertedType}");
                }

                Encode(rentedLongs, 0, data.Length, destination, null);
            } finally {
                ArrayPool<long>.Shared.Return(rentedLongs);
            }
        }

        private static int DecodeDateTime(Span<byte> s, DateTime[] dest, int destOffset, int valueCount, SchemaElement tse, out int consumedBytes) {
            if(tse.Type == Meta.Type.INT32) {
                int[] rented = ArrayPool<int>.Shared.Rent(valueCount);
                try {
                    int read = Decode(s, rented, 0, valueCount, out consumedBytes);
                    for(int i = 0; i < read; i++) {
                        dest[destOffset + i] = rented[i].AsUnixDaysInDateTime();
                    }
                    return read;
                } finally {
                    ArrayPool<int>.Shared.Return(rented);
                }
            }

            if(tse.Type != Meta.Type.INT64) {
                throw new ParquetException($"cannot delta decode DateTime with physical type {tse.Type}");
            }

            long[] rentedLongs = ArrayPool<long>.Shared.Rent(valueCount);
            try {
                int read = Decode(s, rentedLongs, 0, valueCount, out consumedBytes);
                if(tse.LogicalType?.TIMESTAMP is not null) {
                    bool adjustedToUtc = tse.LogicalType.TIMESTAMP.IsAdjustedToUTC;
                    for(int i = 0; i < read; i++) {
                        if(tse.LogicalType.TIMESTAMP.Unit.MILLIS is not null) {
                            DateTime dt = rentedLongs[i].AsUnixMillisecondsInDateTime();
                            DateTimeKind kind = adjustedToUtc ? DateTimeKind.Utc : DateTimeKind.Local;
                            dest[destOffset + i] = DateTime.SpecifyKind(dt, kind);
                        } else if(tse.LogicalType.TIMESTAMP.Unit.MICROS is not null) {
                            long lv = rentedLongs[i];
                            long microseconds = lv % 1000;
                            lv /= 1000;
                            DateTime dt = lv.AsUnixMillisecondsInDateTime().AddTicks(microseconds * 10);
                            DateTimeKind kind = adjustedToUtc ? DateTimeKind.Utc : DateTimeKind.Unspecified;
                            dest[destOffset + i] = DateTime.SpecifyKind(dt, kind);
                        } else if(tse.LogicalType.TIMESTAMP.Unit.NANOS is not null) {
                            long lv = rentedLongs[i];
                            long nanoseconds = lv % 1000000;
                            lv /= 1000000;
                            DateTime dt = lv.AsUnixMillisecondsInDateTime().AddTicks(nanoseconds / 100);
                            DateTimeKind kind = adjustedToUtc ? DateTimeKind.Utc : DateTimeKind.Unspecified;
                            dest[destOffset + i] = DateTime.SpecifyKind(dt, kind);
                        } else {
                            throw new ParquetException($"Unexpected TimeUnit: {tse.LogicalType.TIMESTAMP.Unit}");
                        }
                    }
                } else if(tse.ConvertedType == ConvertedType.TIMESTAMP_MICROS) {
                    for(int i = 0; i < read; i++) {
                        long lv = rentedLongs[i];
                        long microseconds = lv % 1000;
                        lv /= 1000;
                        dest[destOffset + i] = lv.AsUnixMillisecondsInDateTime().AddTicks(microseconds * 10);
                    }
                } else {
                    for(int i = 0; i < read; i++) {
                        dest[destOffset + i] = rentedLongs[i].AsUnixMillisecondsInDateTime();
                    }
                }

                return read;
            } finally {
                ArrayPool<long>.Shared.Return(rentedLongs);
            }
        }


        //this extension method calculates the position of the most significant bit that is set to 1 
        static int CalculateBitWidth(this Span<int> span) {
            int mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 32 - mask.NumberOfLeadingZerosInt();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<long> span) {
            long mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 64 - mask.NumberOfLeadingZerosLong();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<short> span) {
            int mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= (int)span[i];
            }
            return 32 - mask.NumberOfLeadingZerosInt();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<ushort> span) {
            int mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 32 - mask.NumberOfLeadingZerosInt();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<uint> span) {
            long mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 64 - mask.NumberOfLeadingZerosLong();
        }

        //this extension method calculates the position of the most significant bit that is set to 1
        static int CalculateBitWidth(this Span<ulong> span) {
            ulong mask = 0;
            for(int i = 0; i < span.Length; i++) {
                mask |= span[i];
            }
            return 64 - ((long)mask).NumberOfLeadingZerosLong();
        }
    }
}
