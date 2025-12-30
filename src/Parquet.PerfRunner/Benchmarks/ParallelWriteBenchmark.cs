using System.Threading;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Parquet.Data;
using Parquet.PerfRunner.Taxis;

namespace Parquet.PerfRunner.Benchmarks;

[MemoryDiagnoser]
[MarkdownExporter]
public class ParallelWriteBenchmark {
    [Params("tripdata-large")]
    public string Dataset { get; set; } = "tripdata-large";

    [Params(LogicalEncoding.Plain)]
    public LogicalEncoding LogicalEncoding { get; set; }

    TaxiSchema _schema = null!;
    TaxiDataset _dataset;
    ParquetOptions _options = null!;
    MemoryStream _output = new(2_000_000_000);

    [GlobalSetup]
    public async Task LoadDatasetAsync() {
        _dataset = await TaxiDatasetLoader.LoadAsync(Dataset);
        _schema = TaxiSchema.Full(_dataset);
        _options = LogicalEncoding.CreateOptions();
    }

    [Benchmark(Description = "Parallel Write (Task per column)")]
    public async Task ParallelAsync() {
        ThreadPool.GetMinThreads(out _, out int minIOThreads);
        ThreadPool.SetMinThreads(Environment.ProcessorCount * 2, minIOThreads);

        _output.Position = 0;
        using ParquetWriter writer = await ParquetWriter.CreateAsync(_schema.Schema, _output, _options);
        writer.CompressionMethod = CompressionMethod.None;
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

        Task[] tasks = _schema.Columns
            .Select(column =>
                rowGroup.WriteColumnAsync(column))
            .ToArray();

        await Task.WhenAll(tasks);
    }

    [Benchmark(Description = "Sequential Write")]
    public async Task SequentialAsync() {
        _output.Position = 0;
        using ParquetWriter writer = await ParquetWriter.CreateAsync(_schema.Schema, _output, _options);
        writer.CompressionMethod = CompressionMethod.None;
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();

        foreach(DataColumn column in _schema.Columns) {
            await rowGroup.WriteColumnAsync(column);
        }
    }
}
