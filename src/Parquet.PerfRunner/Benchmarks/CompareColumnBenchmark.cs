using BenchmarkDotNet.Attributes;
using Microsoft.VSDiagnostics;
using Parquet.Data;
using Parquet.PerfRunner.Taxis;
using Parquet.Schema;
using ParquetWriterNet = Parquet.ParquetWriter;

namespace Parquet.PerfRunner.Benchmarks;

[MemoryDiagnoser]
[MarkdownExporter]
[CPUUsageDiagnoser]
public class CompareColumnBenchmark {
    private static readonly string[] ColumnNames = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee"
    ];

    [Params("tripdata-large")]
    public string Dataset { get; set; } = "tripdata-large";

    [ParamsSource(nameof(Columns))]
    public string ColumnName { get; set; } = ColumnNames[0];

    [Params(LogicalEncoding.Plain/*, LogicalEncoding.RleDictionary, LogicalEncoding.DeltaBinaryPacked*/)]
    public LogicalEncoding LogicalEncoding { get; set; }

    public IEnumerable<string> Columns => ColumnNames;

    private ParquetSchema _schema = null!;
    private DataColumn _column = null!;
    private ParquetOptions _options = null!;

    [GlobalSetup]
    public async Task LoadDatasetAsync() {
        TaxiDataset dataset = await TaxiDatasetLoader.LoadAsync(Dataset);
        TaxiSchema schema = TaxiSchema.Full(dataset);

        _column = schema.Columns.First(c => c.Field.Name == ColumnName);
        _schema = new ParquetSchema(_column.Field);
        _options = LogicalEncoding.CreateOptions();
    }

    [Benchmark]
    public async Task ParquetNetAsync() {
        using var output = new MemoryStream();
        using ParquetWriterNet writer = await ParquetWriterNet.CreateAsync(_schema, output, _options);
        using ParquetRowGroupWriter rowGroup = writer.CreateRowGroup();
        await rowGroup.WriteColumnAsync(_column);
    }
}
