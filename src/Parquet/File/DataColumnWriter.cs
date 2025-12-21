using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;
using Parquet.Data;
using Parquet.Encodings;
using Parquet.Extensions;
using Parquet.Meta;
using Parquet.Schema;

namespace Parquet.File;

static class DataColumnWriter {
    private static readonly RecyclableMemoryStreamManager _rmsMgr = new RecyclableMemoryStreamManager();

    public static async Task<ColumnChunk> WriteAsync(
        FieldPath fullPath, DataColumn column, Stream stream, SchemaElement schemaElement,
        CompressionLevel compressionLevel, CompressionMethod compressionMethod,
        Dictionary<string, string>? keyValueMetadata, ParquetOptions options,
        CancellationToken cancellationToken = default) {
        long startPos = stream.Position;

        _rmsMgr.Settings.MaximumSmallPoolFreeBytes = options.MaximumSmallPoolFreeBytes;
        _rmsMgr.Settings.MaximumLargePoolFreeBytes = options.MaximumLargePoolFreeBytes;

        (ColumnSizes columnSizes, bool setDBP) = await WriteColumnAsync(column, stream, schemaElement, options, compressionLevel, compressionMethod, cancellationToken);

        // Num_values in the chunk does include null values - I have validated this by dumping spark-generated file.
        ColumnChunk chunk = ThriftFooter.CreateColumnChunk(
            compressionMethod, startPos, schemaElement, fullPath, column,
            keyValueMetadata, setDBP, columnSizes);

        return chunk;
    }


    readonly record struct CompressResult(ColumnSizes ColumnSizes, MemoryStream HeaderMs, IMemoryOwner<byte> PageData) {
    }


    private static async Task<CompressResult> CompressAsync(
        PageHeader ph, MemoryStream uncompressedData, CompressionLevel compressionLevel, CompressionMethod compressionMethod) {
        int uncompressedLength = (int)uncompressedData.Length;
        IMemoryOwner<byte> pageData = await Compressor.Instance.CompressAsync(
            compressionMethod, compressionLevel, uncompressedData);
        int compressedLength = pageData.Memory.Length;

        ph.UncompressedPageSize = uncompressedLength;
        ph.CompressedPageSize = compressedLength;
        int headerSize;

        //write the header in
        MemoryStream headerMs = _rmsMgr.GetStream();
        ph.Write(new Meta.Proto.ThriftCompactProtocolWriter(headerMs));
        headerSize = (int)headerMs.Length;
        headerMs.Position = 0;

        var cs = new ColumnSizes(
            compressedLength + headerSize,
            uncompressedLength + headerSize
        );

        return new CompressResult(
            cs, headerMs, pageData
        );
    }

    private static async Task WriteAsync(Stream stream, CompressResult compressResult) {
        stream.Flush();

        await compressResult.HeaderMs.CopyToAsync(stream);
        await compressResult.PageData.Memory.CopyToAsync(stream);
    }

    private static async Task<(ColumnSizes cs, bool setDBP)> WriteColumnAsync(DataColumn column,
       Stream stream,
       SchemaElement tse, ParquetOptions options, CompressionLevel compressionLevel, CompressionMethod compressionMethod,
       CancellationToken cancellationToken = default) {

        column.Field.EnsureAttachedToSchema(nameof(column));

        bool setDBP = false;
        var r = new ColumnSizes();

        /*
         * Page header must preceeed actual data (compressed or not) however it contains both
         * the uncompressed and compressed data size which we don't know! This somehow limits
         * the write efficiency.
         */

        using var pc = new PackedColumn(column);
        pc.Pack(options.UseDictionaryEncoding, options.DictionaryEncodingThreshold);

        (CompressResult dictCompressResult, MemoryStream ms)? dictWriteState = null;
        try {
            // dictionary page
            if(pc.HasDictionary) {
                PageHeader phDict = ThriftFooter.CreateDictionaryPage(pc.Dictionary!.Length);
                using MemoryStream msDict = _rmsMgr.GetStream();
                ParquetPlainEncoder.Encode(pc.Dictionary, 0, pc.Dictionary.Length,
                       tse,
                       msDict, column.Statistics);

                CompressResult dictCompressResult = await CompressAsync(phDict, msDict, compressionLevel, compressionMethod);
                r = r.Add(dictCompressResult.ColumnSizes);
                dictWriteState = (dictCompressResult, msDict);
            }


            // data page
            using MemoryStream ms = _rmsMgr.GetStream();

            Array data = pc.GetPlainData(out int offset, out int count);
            bool deltaEncode = column.IsDeltaEncodable && options.UseDeltaBinaryPackedEncoding && DeltaBinaryPackedEncoder.CanEncode(data, offset, count);


            if(pc.HasRepetitionLevels) {
                WriteLevels(ms, pc.RepetitionLevels!, pc.RepetitionLevels!.Length, column.Field.MaxRepetitionLevel);
            }
            if(pc.HasDefinitionLevels) {
                WriteLevels(ms, pc.DefinitionLevels!, column.DefinitionLevels!.Length, column.Field.MaxDefinitionLevel);
            }



            if(pc.HasDictionary) {
                // dictionary indexes are always encoded with RLE
                int[] indexes = pc.GetDictionaryIndexes(out int indexesLength)!;
                int bitWidth = pc.Dictionary!.Length.GetBitWidth();
                ms.WriteByte((byte)bitWidth);   // bit width is stored as 1 byte before encoded data
                RleBitpackedHybridEncoder.Encode(ms, indexes.AsSpan(0, indexesLength), bitWidth);
            } else {
                if(deltaEncode) {
                    DeltaBinaryPackedEncoder.Encode(data, offset, count, ms, column.Statistics);
                    setDBP = true;
                } else {
                    ParquetPlainEncoder.Encode(data, offset, count, tse, ms, pc.HasDictionary ? null : column.Statistics);
                }
            }

            Statistics statistics = column.Statistics.ToThriftStatistics(tse);

            // data page Num_values also does include NULLs
            PageHeader ph = ThriftFooter.CreateDataPage(column.NumValues, pc.HasDictionary, deltaEncode, statistics);
            CompressResult cr = await CompressAsync(ph, ms, compressionLevel, compressionMethod);
            using IMemoryOwner<byte> _ = cr.PageData;
            using MemoryStream _1 = cr.HeaderMs;
            r = r.Add(cr.ColumnSizes);


            // from this point on, we are back to writing on the stream
            if(dictWriteState.HasValue) {
                await WriteAsync(stream, dictWriteState.Value.dictCompressResult);
            }

            await WriteAsync(stream, cr);

        } finally { // sadly need to cleanup manually due to the optional lifetime.
            if(dictWriteState.HasValue) {
                (CompressResult dictCompressResult, MemoryStream ms) = dictWriteState.Value;
                ms.Dispose();
                dictCompressResult.PageData.Dispose();
                dictCompressResult.HeaderMs.Dispose();
            }
            
        }

        return (r, setDBP);
    }

    private static void WriteLevels(Stream s, Span<int> levels, int count, int maxValue) {
        int bitWidth = maxValue.GetBitWidth();
        RleBitpackedHybridEncoder.EncodeWithLength(s, bitWidth, levels.Slice(0, count));
    }
}
