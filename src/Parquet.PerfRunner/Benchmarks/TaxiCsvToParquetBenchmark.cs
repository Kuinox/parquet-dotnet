using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ParquetSharp;
using ParquetSharp.IO;
using Column = ParquetSharp.Column;
using ParquetReaderNet = Parquet.ParquetReader;
using ParquetWriterNet = Parquet.ParquetWriter;
using IOFile = System.IO.File;
using ParquetSharpEncoding = ParquetSharp.Encoding;

namespace Parquet.PerfRunner.Benchmarks;

public abstract class TaxiCsvBenchmarkBase : IConfigSource
{
    protected static readonly string[] IntColumnNames = ["vendorid", "ratecodeid", "payment_type"];
    protected static readonly bool KeepArtifacts = string.Equals(
        Environment.GetEnvironmentVariable("PARQUET_PERF_KEEP_FILES"),
        "1",
        StringComparison.Ordinal);
    protected static readonly ConcurrentDictionary<string, long> FileSizes = new();

    protected TaxiCsvBenchmarkBase()
    {
        Config = ManualConfig.Create(DefaultConfig.Instance)
            .AddColumn(FileSizeColumn.Instance);
    }

    public IConfig Config { get; }

    [Params("tripdata", "tripdata-large")]
    public string Dataset { get; set; } = "tripdata";

    [Params(LogicalEncoding.RleDictionary, LogicalEncoding.DeltaBinaryPacked, LogicalEncoding.Plain)]
    public LogicalEncoding Encoding { get; set; } = LogicalEncoding.RleDictionary;

    protected int?[]? _vendorIds;
    protected int?[]? _rateCodes;
    protected double?[]? _passengerCounts;
    protected double?[]? _tripDistances;
    protected int?[]? _paymentTypes;
    protected double?[]? _fareAmounts;

    protected ParquetSchema? _parquetSchema;
    protected DataColumn[]? _parquetNetColumns;
    protected Column[]? _parquetSharpColumns;
    protected ParquetOptions? _parquetOptions;
    protected WriterProperties? _parquetSharpWriterProperties;

    [GlobalSetup]
    public async Task LoadDataset()
    {
        string[] parquetPaths = await EnsureDatasetsAsync();
        TaxiColumns columns = await LoadParquetColumnsAsync(parquetPaths);

        _vendorIds = columns.VendorIds;
        _rateCodes = columns.RateCodes;
        _passengerCounts = columns.PassengerCounts;
        _tripDistances = columns.TripDistances;
        _paymentTypes = columns.PaymentTypes;
        _fareAmounts = columns.FareAmounts;

        _parquetSchema = new ParquetSchema(
            new DataField<int?>("vendorid"),
            new DataField<int?>("ratecodeid"),
            new DataField<double?>("passenger_count"),
            new DataField<double?>("trip_distance"),
            new DataField<int?>("payment_type"),
            new DataField<double?>("fare_amount"));

        DataField[] fields = _parquetSchema.DataFields;
        _parquetNetColumns =
        [
            new DataColumn(fields[0], _vendorIds),
            new DataColumn(fields[1], _rateCodes),
            new DataColumn(fields[2], _passengerCounts),
            new DataColumn(fields[3], _tripDistances),
            new DataColumn(fields[4], _paymentTypes),
            new DataColumn(fields[5], _fareAmounts)
        ];

        _parquetSharpColumns =
        [
            new Column<int?>("vendorid"),
            new Column<int?>("ratecodeid"),
            new Column<double?>("passenger_count"),
            new Column<double?>("trip_distance"),
            new Column<int?>("payment_type"),
            new Column<double?>("fare_amount")
        ];

        _parquetOptions = CreateParquetOptions(Encoding);
        _parquetSharpWriterProperties = CreateParquetSharpWriterProperties(Encoding);
        AfterLoadDataset();
    }

    [IterationCleanup]
    public void BaseIterationCleanup()
    {
        OnIterationCleanup();
    }

    protected virtual void OnIterationCleanup() { }

    private static ParquetOptions CreateParquetOptions(LogicalEncoding encoding) =>
        encoding switch
        {
            LogicalEncoding.RleDictionary => new ParquetOptions
            {
                UseDictionaryEncoding = true,
                DictionaryEncodingThreshold = 1.0,
                UseDeltaBinaryPackedEncoding = false
            },
            LogicalEncoding.DeltaBinaryPacked => new ParquetOptions
            {
                UseDictionaryEncoding = false,
                UseDeltaBinaryPackedEncoding = true
            },
            LogicalEncoding.Plain => new ParquetOptions
            {
                UseDictionaryEncoding = false,
                UseDeltaBinaryPackedEncoding = false
            },
            _ => throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unknown logical encoding")
        };

    private static WriterProperties CreateParquetSharpWriterProperties(LogicalEncoding encoding)
    {
        var builder = new WriterPropertiesBuilder()
            .Compression(Compression.Snappy);

        switch (encoding)
        {
            case LogicalEncoding.RleDictionary:
                builder.EnableDictionary();
                builder.Encoding(ParquetSharpEncoding.Plain);
                break;
            case LogicalEncoding.DeltaBinaryPacked:
                builder.DisableDictionary();
                builder.Encoding(ParquetSharpEncoding.Plain);
                foreach (string columnName in IntColumnNames)
                {
                    builder.Encoding(columnName, ParquetSharpEncoding.DeltaBinaryPacked);
                }

                break;
            case LogicalEncoding.Plain:
                builder.DisableDictionary();
                builder.Encoding(ParquetSharpEncoding.Plain);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(encoding), encoding, "Unknown logical encoding");
        }

        return builder.Build();
    }

    protected virtual void AfterLoadDataset()
    {
    }

    private static string GetDataDirectory()
    {
        string? overrideDir = Environment.GetEnvironmentVariable("PARQUET_PERF_DATA_DIR");
        if (!string.IsNullOrWhiteSpace(overrideDir))
        {
            Directory.CreateDirectory(overrideDir);
            return overrideDir;
        }

        return Path.Combine(AppContext.BaseDirectory, "Data");
    }

    private static string? TryProjectDataPath(string fileName)
    {
        string projectPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../Data", fileName));
        return IOFile.Exists(projectPath) ? projectPath : null;
    }

    private async Task<string[]> EnsureDatasetsAsync()
    {
        if (!DatasetFiles.TryGetValue(Dataset, out string[]? files))
        {
            throw new ArgumentOutOfRangeException(nameof(Dataset), Dataset, "Unknown dataset alias");
        }

        string dataDir = GetDataDirectory();
        Directory.CreateDirectory(dataDir);

        var paths = new List<string>(files.Length);
        using var httpClient = new HttpClient();

        foreach (string file in files)
        {
            string dataPath = Path.Combine(dataDir, file);
            if (IOFile.Exists(dataPath))
            {
                paths.Add(dataPath);
                continue;
            }

            string? projectPath = TryProjectDataPath(file);
            if (projectPath != null)
            {
                paths.Add(projectPath);
                continue;
            }

            string url = $"{BaseUrl}{file}";
            byte[] bytes = await httpClient.GetByteArrayAsync(url);
            await IOFile.WriteAllBytesAsync(dataPath, bytes);
            paths.Add(dataPath);
        }

        return paths.ToArray();
    }

    private static async Task<TaxiColumns> LoadParquetColumnsAsync(IEnumerable<string> paths)
    {
        var vendorIds = new List<int?>();
        var rateCodes = new List<int?>();
        var passengerCounts = new List<double?>();
        var tripDistances = new List<double?>();
        var paymentTypes = new List<int?>();
        var fareAmounts = new List<double?>();

        foreach (string path in paths)
        {
            using FileStream fs = IOFile.OpenRead(path);
            using ParquetReaderNet reader = await ParquetReaderNet.CreateAsync(fs);

            DataField vendor = FindField(reader.Schema, "vendorid");
            DataField rateCode = FindField(reader.Schema, "ratecodeid");
            DataField passengerCount = FindField(reader.Schema, "passenger_count");
            DataField tripDistance = FindField(reader.Schema, "trip_distance");
            DataField paymentType = FindField(reader.Schema, "payment_type");
            DataField fareAmount = FindField(reader.Schema, "fare_amount");

            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using ParquetRowGroupReader rg = reader.OpenRowGroupReader(i);

                vendorIds.AddRange(ToNullableInt((await rg.ReadColumnAsync(vendor)).Data));
                rateCodes.AddRange(ToNullableInt((await rg.ReadColumnAsync(rateCode)).Data));
                passengerCounts.AddRange(ToNullableDouble((await rg.ReadColumnAsync(passengerCount)).Data));
                tripDistances.AddRange(ToNullableDouble((await rg.ReadColumnAsync(tripDistance)).Data));
                paymentTypes.AddRange(ToNullableInt((await rg.ReadColumnAsync(paymentType)).Data));
                fareAmounts.AddRange(ToNullableDouble((await rg.ReadColumnAsync(fareAmount)).Data));
            }
        }

        return new TaxiColumns(
            vendorIds.ToArray(),
            rateCodes.ToArray(),
            passengerCounts.ToArray(),
            tripDistances.ToArray(),
            paymentTypes.ToArray(),
            fareAmounts.ToArray());
    }

    private static DataField FindField(ParquetSchema schema, string name)
    {
        return schema.DataFields.First(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));
    }

    private static IReadOnlyList<int?> ToNullableInt(Array data) =>
        data switch
        {
            int?[] v => v,
            int[] v => v.Select(i => (int?)i).ToArray(),
            long?[] v => v.Select(i => i.HasValue ? (int?)checked((int)i.Value) : null).ToArray(),
            long[] v => v.Select(i => (int?)checked((int)i)).ToArray(),
            double?[] v => v.Select(i => i.HasValue ? (int?)checked((int)i.Value) : null).ToArray(),
            double[] v => v.Select(i => (int?)checked((int)i)).ToArray(),
            float?[] v => v.Select(i => i.HasValue ? (int?)checked((int)i.Value) : null).ToArray(),
            float[] v => v.Select(i => (int?)checked((int)i)).ToArray(),
            _ => throw new InvalidOperationException($"Unsupported numeric type: {data.GetType()}")
        };

    private static IReadOnlyList<double?> ToNullableDouble(Array data) =>
        data switch
        {
            double?[] v => v,
            double[] v => v.Select(i => (double?)i).ToArray(),
            float?[] v => v.Select(i => i.HasValue ? (double?)i.Value : null).ToArray(),
            float[] v => v.Select(i => (double?)i).ToArray(),
            int?[] v => v.Select(i => i.HasValue ? (double?)i.Value : null).ToArray(),
            int[] v => v.Select(i => (double?)i).ToArray(),
            long?[] v => v.Select(i => i.HasValue ? (double?)i.Value : null).ToArray(),
            long[] v => v.Select(i => (double?)i).ToArray(),
            _ => throw new InvalidOperationException($"Unsupported numeric type: {data.GetType()}")
        };

    protected static void TryDelete(string path)
    {
        try
        {
            if (IOFile.Exists(path))
            {
                IOFile.Delete(path);
            }
        }
        catch
        {
            // best effort cleanup
        }
    }

    protected static void RecordFileSize(string method, LogicalEncoding encoding, long length)
    {
        try
        {
            string key = BuildKey(method, encoding);
            FileSizes[key] = length;
        }
        catch
        {
            // ignore size recording failures
        }
    }

    protected static string BuildKey(string method, LogicalEncoding encoding) => $"{method}|{encoding}";

    private sealed class FileSizeColumn : IColumn
    {
        public static readonly IColumn Instance = new FileSizeColumn();

        public string Id => "FileSizeBytes";
        public string ColumnName => "FileSizeBytes";
        public bool IsNumeric => true;
        public UnitType UnitType => UnitType.Size;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 0;
        public bool AlwaysShow => true;
        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
        public bool IsAvailable(Summary summary) => true;
        public string Legend => "Size of the last produced parquet payload (bytes)";

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
            => GetValue(summary, benchmarkCase, SummaryStyle.Default);

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            string method = benchmarkCase.Descriptor.WorkloadMethod.Name;
            var encoding = (LogicalEncoding)benchmarkCase.Parameters[nameof(TaxiCsvBenchmarkBase.Encoding)];
            string key = BuildKey(method, encoding);

            return FileSizes.TryGetValue(key, out long bytes)
                ? bytes.ToString()
                : "?";
        }
    }

    // Official NYC TLC parquet. The "tripdata" dataset is a single month; "tripdata-large" combines multiple months.
    private const string BaseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/";
    private static readonly IReadOnlyDictionary<string, string[]> DatasetFiles = new Dictionary<string, string[]>
    {
        { "tripdata", new[] { "yellow_tripdata_2024-01.parquet" } },
        { "tripdata-large", new[] { "yellow_tripdata_2024-01.parquet", "yellow_tripdata_2024-02.parquet", "yellow_tripdata_2024-03.parquet" } }
    };
}

public class TaxiParquetNetVsSharpBenchmark : TaxiCsvBenchmarkBase
{
    private long? _netMem;
    private long? _sharpMem;
    private long? _netDisk;
    private long? _sharpDisk;
    private string? _netDiskPath;
    private string? _sharpDiskPath;

    [Benchmark(Description = "Parquet.Net -> MemoryStream")]
    public async Task ParquetNetMemory()
    {
        using var output = new MemoryStream();
        using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_parquetSchema!, output, _parquetOptions);
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

        foreach (DataColumn column in _parquetNetColumns!)
        {
            await rowGroup.WriteColumnAsync(column);
        }

        _netMem = output.Length;
    }

    [Benchmark(Description = "ParquetSharp -> MemoryStream")]
    public void ParquetSharpMemory()
    {
        using var output = new MemoryStream();
        using var managedOutput = new ManagedOutputStream(output);
        using var writer = new ParquetFileWriter(managedOutput, _parquetSharpColumns!, _parquetSharpWriterProperties!, null);
        using RowGroupWriter rowGroup = writer.AppendRowGroup();

        using (LogicalColumnWriter<int?> vendorWriter = rowGroup.NextColumn().LogicalWriter<int?>())
        {
            vendorWriter.WriteBatch(_vendorIds!);
        }

        using (LogicalColumnWriter<int?> rateCodeWriter = rowGroup.NextColumn().LogicalWriter<int?>())
        {
            rateCodeWriter.WriteBatch(_rateCodes!);
        }

        using (LogicalColumnWriter<double?> passengerCountWriter = rowGroup.NextColumn().LogicalWriter<double?>())
        {
            passengerCountWriter.WriteBatch(_passengerCounts!);
        }

        using (LogicalColumnWriter<double?> tripDistanceWriter = rowGroup.NextColumn().LogicalWriter<double?>())
        {
            tripDistanceWriter.WriteBatch(_tripDistances!);
        }

        using (LogicalColumnWriter<int?> paymentTypeWriter = rowGroup.NextColumn().LogicalWriter<int?>())
        {
            paymentTypeWriter.WriteBatch(_paymentTypes!);
        }

        using (LogicalColumnWriter<double?> fareWriter = rowGroup.NextColumn().LogicalWriter<double?>())
        {
            fareWriter.WriteBatch(_fareAmounts!);
        }

        writer.Close();
        _sharpMem = output.Length;
    }

    [Benchmark(Description = "Parquet.Net -> Disk")]
    public async Task ParquetNetDisk()
    {
        string path = Path.Combine(Path.GetTempPath(), $"taxi-parquetnet-{Guid.NewGuid():N}.parquet");
        try
        {
            await using FileStream output = IOFile.Create(path);
            using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_parquetSchema!, output, _parquetOptions);
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

            foreach (DataColumn column in _parquetNetColumns!)
            {
                await rowGroup.WriteColumnAsync(column);
            }

            _netDisk = new FileInfo(path).Length;
            _netDiskPath = path;
        }
        catch
        {
            TryDelete(path);
            throw;
        }
    }

    [Benchmark(Description = "ParquetSharp -> Disk")]
    public void ParquetSharpDisk()
    {
        string path = Path.Combine(Path.GetTempPath(), $"taxi-parquetsharp-{Guid.NewGuid():N}.parquet");
        try
        {
            using FileStream output = IOFile.Create(path);
            using var managedOutput = new ManagedOutputStream(output);
            using var writer = new ParquetFileWriter(managedOutput, _parquetSharpColumns!, _parquetSharpWriterProperties!, null);
            using RowGroupWriter rowGroup = writer.AppendRowGroup();

            using (LogicalColumnWriter<int?> vendorWriter = rowGroup.NextColumn().LogicalWriter<int?>())
            {
                vendorWriter.WriteBatch(_vendorIds!);
            }

            using (LogicalColumnWriter<int?> rateCodeWriter = rowGroup.NextColumn().LogicalWriter<int?>())
            {
                rateCodeWriter.WriteBatch(_rateCodes!);
            }

            using (LogicalColumnWriter<double?> passengerCountWriter = rowGroup.NextColumn().LogicalWriter<double?>())
            {
                passengerCountWriter.WriteBatch(_passengerCounts!);
            }

            using (LogicalColumnWriter<double?> tripDistanceWriter = rowGroup.NextColumn().LogicalWriter<double?>())
            {
                tripDistanceWriter.WriteBatch(_tripDistances!);
            }

            using (LogicalColumnWriter<int?> paymentTypeWriter = rowGroup.NextColumn().LogicalWriter<int?>())
            {
                paymentTypeWriter.WriteBatch(_paymentTypes!);
            }

            using (LogicalColumnWriter<double?> fareWriter = rowGroup.NextColumn().LogicalWriter<double?>())
            {
                fareWriter.WriteBatch(_fareAmounts!);
            }

            writer.Close();
            _sharpDisk = new FileInfo(path).Length;
            _sharpDiskPath = path;
        }
        catch
        {
            TryDelete(path);
            throw;
        }
    }

    protected override void OnIterationCleanup()
    {
        if (_netMem.HasValue) RecordFileSize(nameof(ParquetNetMemory), Encoding, _netMem.Value);
        if (_sharpMem.HasValue) RecordFileSize(nameof(ParquetSharpMemory), Encoding, _sharpMem.Value);
        if (_netDisk.HasValue) RecordFileSize(nameof(ParquetNetDisk), Encoding, _netDisk.Value);
        if (_sharpDisk.HasValue) RecordFileSize(nameof(ParquetSharpDisk), Encoding, _sharpDisk.Value);

        if (_netDiskPath != null)
        {
            if (KeepArtifacts) Console.WriteLine($"ParquetNetDisk file kept at: {_netDiskPath}");
            else TryDelete(_netDiskPath);
        }

        if (_sharpDiskPath != null)
        {
            if (KeepArtifacts) Console.WriteLine($"ParquetSharpDisk file kept at: {_sharpDiskPath}");
            else TryDelete(_sharpDiskPath);
        }

        _netMem = _sharpMem = _netDisk = _sharpDisk = null;
        _netDiskPath = _sharpDiskPath = null;
    }
}

[MemoryDiagnoser]
[MarkdownExporter]
[ShortRunJob]
public class TaxiParquetNetNugetBenchmark : TaxiCsvBenchmarkBase
{
    private object? _nugetSchema;
    private object[]? _nugetColumns;
    private MethodInfo? _nugetCreateAsync;
    private Assembly? _nugetAssembly;
    private Type? _nugetRowGroupWriterType;

    private long? _localMem;
    private long? _nugetMem;
    private long? _localDisk;
    private long? _nugetDisk;
    private string? _localDiskPath;
    private string? _nugetDiskPath;

    protected override void AfterLoadDataset()
    {
        string dllPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".nuget", "packages", "parquet.net", "4.24.0", "lib", "net8.0", "Parquet.dll");

        _nugetAssembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(dllPath);

        Type dataFieldType = _nugetAssembly.GetType("Parquet.Data.DataField")!;
        Type schemaType = _nugetAssembly.GetType("Parquet.Schema.ParquetSchema")!;
        Type dataColumnType = _nugetAssembly.GetType("Parquet.Data.DataColumn")!;
        Type writerType = _nugetAssembly.GetType("Parquet.ParquetWriter")!;
        _nugetRowGroupWriterType = _nugetAssembly.GetType("Parquet.ParquetRowGroupWriter")!;

        object CreateDataField(string name, Type clrType) =>
            Activator.CreateInstance(dataFieldType, new object?[] { name, clrType, null, null, null })!;

        object[] fields =
        [
            CreateDataField("vendorid", typeof(int?)),
            CreateDataField("ratecodeid", typeof(int?)),
            CreateDataField("passenger_count", typeof(double?)),
            CreateDataField("trip_distance", typeof(double?)),
            CreateDataField("payment_type", typeof(int?)),
            CreateDataField("fare_amount", typeof(double?))
        ];

        _nugetSchema = Activator.CreateInstance(schemaType, new object?[] { fields })!;

        _nugetColumns =
        [
            Activator.CreateInstance(dataColumnType, fields[0], _vendorIds)!,
            Activator.CreateInstance(dataColumnType, fields[1], _rateCodes)!,
            Activator.CreateInstance(dataColumnType, fields[2], _passengerCounts)!,
            Activator.CreateInstance(dataColumnType, fields[3], _tripDistances)!,
            Activator.CreateInstance(dataColumnType, fields[4], _paymentTypes)!,
            Activator.CreateInstance(dataColumnType, fields[5], _fareAmounts)!
        ];

        var createCandidates = new List<Type?>()
        {
            schemaType,
            typeof(Stream),
            _nugetAssembly.GetType("Parquet.ParquetOptions"),
            typeof(bool),
            typeof(CancellationToken)
        };

        _nugetCreateAsync = writerType.GetMethod("CreateAsync", createCandidates.Where(t => t != null).Cast<Type>().ToArray())
            ?? writerType.GetMethod("CreateAsync", new[] { schemaType, typeof(Stream) });
    }

    [Benchmark(Description = "Parquet.Net Local -> MemoryStream")]
    public async Task ParquetLocalMemory()
    {
        using var output = new MemoryStream();
        using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_parquetSchema!, output, _parquetOptions);
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

        foreach (DataColumn column in _parquetNetColumns!)
        {
            await rowGroup.WriteColumnAsync(column);
        }

        _localMem = output.Length;
    }

    [Benchmark(Description = "Parquet.Net NuGet -> MemoryStream")]
    public async Task ParquetNugetMemory()
    {
        using var output = new MemoryStream();
        dynamic writer = await CreateNugetWriterAsync(output);
        dynamic rowGroup = writer.CreateRowGroup();

        foreach (dynamic column in _nugetColumns!)
        {
            await rowGroup.WriteColumnAsync(column);
        }

        DisposeNuget(writer, rowGroup);
        _nugetMem = output.Length;
    }

    [Benchmark(Description = "Parquet.Net Local -> Disk")]
    public async Task ParquetLocalDisk()
    {
        string path = Path.Combine(Path.GetTempPath(), $"taxi-parquetnetlocal-{Guid.NewGuid():N}.parquet");
        try
        {
            await using FileStream output = IOFile.Create(path);
            using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_parquetSchema!, output, _parquetOptions);
            using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

            foreach (DataColumn column in _parquetNetColumns!)
            {
                await rowGroup.WriteColumnAsync(column);
            }

            _localDisk = new FileInfo(path).Length;
            _localDiskPath = path;
        }
        catch
        {
            TryDelete(path);
            throw;
        }
    }

    [Benchmark(Description = "Parquet.Net NuGet -> Disk")]
    public async Task ParquetNugetDisk()
    {
        string path = Path.Combine(Path.GetTempPath(), $"taxi-parquetnetnuget-{Guid.NewGuid():N}.parquet");
        try
        {
            await using FileStream output = IOFile.Create(path);
            dynamic writer = await CreateNugetWriterAsync(output);
            dynamic rowGroup = writer.CreateRowGroup();

            foreach (dynamic column in _nugetColumns!)
            {
                await rowGroup.WriteColumnAsync(column);
            }

            DisposeNuget(writer, rowGroup);
            _nugetDisk = new FileInfo(path).Length;
            _nugetDiskPath = path;
        }
        catch
        {
            TryDelete(path);
            throw;
        }
    }

    private async Task<dynamic> CreateNugetWriterAsync(Stream output)
    {
        if (_nugetCreateAsync == null || _nugetSchema == null)
        {
            throw new InvalidOperationException("NuGet schema not initialized");
        }

        object? taskObj = _nugetCreateAsync.Invoke(null, _nugetCreateAsync.GetParameters().Length == 5
            ? new object?[] { _nugetSchema, output, null, false, CancellationToken.None }
            : new object?[] { _nugetSchema, output });

        if (taskObj is not Task writerTask)
        {
            throw new InvalidOperationException("Unexpected writer task type for NuGet Parquet");
        }

        await writerTask.ConfigureAwait(false);
        dynamic writer = ((dynamic)writerTask).Result;
        return writer;
    }

    private void DisposeNuget(dynamic writer, dynamic rowGroup)
    {
        try
        {
            rowGroup?.Dispose();
        }
        catch
        {
            // ignore
        }

        try
        {
            writer?.Dispose();
        }
        catch
        {
            // ignore
        }
    }

    protected override void OnIterationCleanup()
    {
        if (_localMem.HasValue) RecordFileSize(nameof(ParquetLocalMemory), Encoding, _localMem.Value);
        if (_nugetMem.HasValue) RecordFileSize(nameof(ParquetNugetMemory), Encoding, _nugetMem.Value);
        if (_localDisk.HasValue) RecordFileSize(nameof(ParquetLocalDisk), Encoding, _localDisk.Value);
        if (_nugetDisk.HasValue) RecordFileSize(nameof(ParquetNugetDisk), Encoding, _nugetDisk.Value);

        if (_localDiskPath != null)
        {
            if (KeepArtifacts) Console.WriteLine($"ParquetLocalDisk file kept at: {_localDiskPath}");
            else TryDelete(_localDiskPath);
        }

        if (_nugetDiskPath != null)
        {
            if (KeepArtifacts) Console.WriteLine($"ParquetNugetDisk file kept at: {_nugetDiskPath}");
            else TryDelete(_nugetDiskPath);
        }

        _localMem = _nugetMem = _localDisk = _nugetDisk = null;
        _localDiskPath = _nugetDiskPath = null;
    }
}

internal readonly struct TaxiColumns
{
    public TaxiColumns(int?[] vendorIds, int?[] rateCodes, double?[] passengerCounts, double?[] tripDistances, int?[] paymentTypes, double?[] fareAmounts)
    {
        VendorIds = vendorIds;
        RateCodes = rateCodes;
        PassengerCounts = passengerCounts;
        TripDistances = tripDistances;
        PaymentTypes = paymentTypes;
        FareAmounts = fareAmounts;
    }

    public int?[] VendorIds { get; }
    public int?[] RateCodes { get; }
    public double?[] PassengerCounts { get; }
    public double?[] TripDistances { get; }
    public int?[] PaymentTypes { get; }
    public double?[] FareAmounts { get; }
}

public enum LogicalEncoding
{
    Plain,
    RleDictionary,
    DeltaBinaryPacked
}
