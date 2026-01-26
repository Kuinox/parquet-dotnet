namespace Parquet.Schema {
    /// <summary>
    /// Per-column encoding overrides. Null values inherit <see cref="ParquetOptions"/> defaults.
    /// </summary>
    public sealed class ColumnEncodingOptions {
        /// <summary>
        /// When set, overrides <see cref="ParquetOptions.UseDictionaryEncoding"/> for this column.
        /// </summary>
        public bool? UseDictionaryEncoding { get; set; }

        /// <summary>
        /// When set, overrides <see cref="ParquetOptions.DictionaryEncodingThreshold"/> for this column.
        /// </summary>
        public double? DictionaryEncodingThreshold { get; set; }

        /// <summary>
        /// When set, overrides <see cref="ParquetOptions.UseDeltaBinaryPackedEncoding"/> for this column.
        /// </summary>
        public bool? UseDeltaBinaryPackedEncoding { get; set; }
    }
}
