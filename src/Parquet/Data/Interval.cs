using System;
using System.Buffers.Binary;

namespace Parquet.File.Values.Primitives; 

/// <summary>
/// A parquet interval type compatible with a Spark INTERVAL type
/// 12 byte little Endian structure fits in an INT96 original type with an INTERVAL converted type
/// </summary>
public struct Interval {

    /// <summary>
    /// Binary serialised size
    /// </summary>
    public const int BinarySize = 12;

    /// <summary>
    /// Used to create an interval type
    /// </summary>
    /// <param name="months">The month interval</param>
    /// <param name="days">The days interval</param>
    /// <param name="millis">The milliseconds interval</param>
    public Interval(int months, int days, int millis) {
        Months = months;
        Days = days;
        Millis = millis;
    }
    /// <summary>
    /// Returns the number of milliseconds in the type
    /// </summary>
    public int Millis { get; set; }

    /// <summary>
    /// Returns the number of days in the type
    /// </summary>
    public int Days { get; set; }

    /// <summary>
    /// Returns the number of months in type
    /// </summary>
    public int Months { get; set; }

    /// <summary>
    /// Converts to bytes representation
    /// </summary>
    /// <returns></returns>
    public byte[] GetBytes() {
        byte[] r = new byte[BinarySize];
        BinaryPrimitives.WriteInt32LittleEndian(r.AsSpan(0), Months);
        BinaryPrimitives.WriteInt32LittleEndian(r.AsSpan(4), Days);
        BinaryPrimitives.WriteInt32LittleEndian(r.AsSpan(8), Millis);
        return r;
    }

    /// <summary>
    /// String repr
    /// </summary>
    public override string ToString() => $"{Months}m {Days}d {Millis}ms";
}