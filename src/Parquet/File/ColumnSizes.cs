using System.Diagnostics.Contracts;

namespace Parquet.File;

internal readonly struct ColumnSizes(int compressedSize, int uncompressedSize) {
    public readonly int CompressedSize = compressedSize;
    public readonly int UncompressedSize = uncompressedSize;

    [Pure]
    public ColumnSizes Add(ColumnSizes other) => new(
            CompressedSize + other.CompressedSize,
            UncompressedSize + other.UncompressedSize
        );
}
