// for performance tests only

using BenchmarkDotNet.Running;
using Parquet;
using Parquet.PerfRunner.Benchmarks;

if (args.Length == 0)
{
    await new DataTypes().NullableInts();
    return;
}

if (args.Length == 1)
{
    switch (args[0])
    {
        case "write":
            BenchmarkRunner.Run<WriteBenchmark>();
            return;
        case "progression":
            VersionedBenchmark.Run();
            return;
        case "taxi":
            BenchmarkRunner.Run(new[]
            {
                typeof(TaxiParquetNetVsSharpBenchmark),
                typeof(TaxiParquetNetNugetBenchmark)
            });
            return;
    }
}

// fall back to full BenchmarkSwitcher to honor filters/arguments
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
