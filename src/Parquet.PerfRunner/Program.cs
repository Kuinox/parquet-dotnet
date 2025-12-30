// for performance tests only

using BenchmarkDotNet.Running;
using Parquet.PerfRunner.Benchmarks;

bool haveArgs = args.Length > 0;
string benchName;
if(!haveArgs) {
    Console.WriteLine("Enter the benchmark name to run:");
    benchName = Console.ReadLine()!;
} else {
    benchName = args[0];
}

switch(benchName) {
    case "write":
        BenchmarkRunner.Run<WriteBenchmark>();
        break;
    case "progression":
        BenchmarkRunner.Run<VersionedBenchmark>();
        break;
    case "parallel":
        BenchmarkRunner.Run<ParallelWriteBenchmark>();
        break;
    case "sharp-compare-taxi":
        BenchmarkRunner.Run<ParquetSharpComparisonBenchmark>();
        break;
    case "self-compare-taxi":
        BenchmarkRunner.Run<SelfComparisonBenchmark>();
        break;
    case "taxi-per-column":
        BenchmarkRunner.Run<CompareColumnBenchmark>();
        break;
    case "highLevel":
        HighLevel.Run();
        break;
}
