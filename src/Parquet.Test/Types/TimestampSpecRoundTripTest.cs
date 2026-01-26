using System;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;
namespace Parquet.Test.Types {
    public class TimestampSpecRoundTripTest : TestBase {
        private static bool IsUtcTimezone() {
            return TimeZoneInfo.Local.BaseUtcOffset == TimeSpan.Zero;
        }

        [Fact]
        public async Task Timestamp_adjusted_to_utc_millis_local_is_normalized() {
            if(IsUtcTimezone())
                return;

            DateTime local = DateTime.SpecifyKind(new DateTime(2020, 6, 10, 11, 12, 13), DateTimeKind.Local);
            DateTime expectedUtc = local.ToUniversalTime();

            var field = new DateTimeDataField("ts", DateTimeFormat.Timestamp, isAdjustedToUTC: true, unit: DateTimeTimeUnit.Millis);
            object actualValue = await WriteReadSingle(field, local);
            DateTime actual = Assert.IsType<DateTime>(actualValue);
            Assert.Equal(DateTimeKind.Utc, actual.Kind);
            Assert.Equal(expectedUtc.Ticks, actual.Ticks);
        }

#if NET7_0_OR_GREATER
        [Fact]
        public async Task Timestamp_not_adjusted_micros_local_is_not_normalized() {
            if(IsUtcTimezone())
                return;

            DateTime local = DateTime.SpecifyKind(new DateTime(2020, 6, 10, 11, 12, 13, 456).AddTicks(7000), DateTimeKind.Local);
            DateTime expected = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);

            var field = new DateTimeDataField("ts", DateTimeFormat.Timestamp, isAdjustedToUTC: false, unit: DateTimeTimeUnit.Micros);
            object actualValue = await WriteReadSingle(field, local);
            DateTime actual = Assert.IsType<DateTime>(actualValue);
            Assert.Equal(DateTimeKind.Unspecified, actual.Kind);
            Assert.Equal(expected.Ticks, actual.Ticks);
        }
#endif
    }
}
