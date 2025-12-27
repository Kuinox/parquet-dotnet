using System;
using System.Globalization;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

namespace Parquet.Test.Types {
    public class EndToEndDateTimeKindTest : TestBase {
        private static DateTime BaseSeconds => DateTime.Parse("2020-06-10T11:12:13", CultureInfo.InvariantCulture);
        private static DateTime BaseMillis => DateTime.Parse("2020-06-10T11:12:13.456", CultureInfo.InvariantCulture);
        private static DateTime BaseMicros => BaseMillis.AddTicks(7890);
        private static DateTime BaseDateOnly => DateTime.Parse("2020-06-10T00:00:00", CultureInfo.InvariantCulture);

        [Fact]
        public async Task Int96_simple_datetime_round_trip_as_unknown() {
            DateTime baseDate = BaseSeconds;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = DateTime.SpecifyKind(local.ToUniversalTime(), DateTimeKind.Unspecified);

            await AssertRoundTrip(
                new DataField<DateTime>("datetime"),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    expectedLocal,
                ]);
        }

        [Fact]
        public async Task Int96_dateTime_round_trip_as_unknown() {
            DateTime baseDate = BaseSeconds;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = DateTime.SpecifyKind(local.ToUniversalTime(), DateTimeKind.Unspecified);

            await AssertRoundTrip(
                new DataField<DateTime>("dateTime"),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    expectedLocal,
                ]);
        }

        [Fact]
        public async Task Int96_impala_date_round_trip_as_unknown() {
            DateTime baseDate = BaseSeconds;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = DateTime.SpecifyKind(local.ToUniversalTime(), DateTimeKind.Unspecified);

            await AssertRoundTrip(
                new DateTimeDataField("dateImpala", DateTimeFormat.Impala),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    expectedLocal,
                ]);
        }

        [Fact]
        public async Task DateAndTime_millis_round_trip_as_utc() {
            DateTime baseDate = BaseMillis;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = local.ToUniversalTime();

            await AssertRoundTrip(
                new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTime),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(expectedLocal, DateTimeKind.Utc),
                ]);
        }

#if NET7_0_OR_GREATER
        [Fact]
        public async Task DateAndTime_micros_round_trip_as_utc() {
            DateTime baseDate = BaseMicros;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = local.ToUniversalTime();

            await AssertRoundTrip(
                new DateTimeDataField("dateDateAndTime", DateTimeFormat.DateAndTimeMicros),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(expectedLocal, DateTimeKind.Utc),
                ]);
        }
#endif

        [Fact]
        public async Task Int96_dateTime_unknown_kind_round_trip_as_unknown() {
            DateTime baseDate = BaseSeconds;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = DateTime.SpecifyKind(local.ToUniversalTime(), DateTimeKind.Unspecified);

            await AssertRoundTrip(
                new DataField<DateTime>("dateTime unknown kind"),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    expectedLocal,
                ]);
        }

        [Fact]
        public async Task Int96_impala_date_unknown_kind_round_trip_as_unknown() {
            DateTime baseDate = BaseSeconds;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = DateTime.SpecifyKind(local.ToUniversalTime(), DateTimeKind.Unspecified);

            await AssertRoundTrip(
                new DateTimeDataField("dateImpala unknown kind", DateTimeFormat.Impala),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    expectedLocal,
                ]);
        }

        [Fact]
        public async Task DateAndTime_unknown_kind_round_trip_as_utc() {
            DateTime baseDate = BaseMillis;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = local.ToUniversalTime();

            await AssertRoundTrip(
                new DateTimeDataField("dateDateAndTime unknown kind", DateTimeFormat.DateAndTime, isAdjustedToUTC: false),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(expectedLocal, DateTimeKind.Utc),
                ]);
        }

        [Fact]
        public async Task Int96_dateTime_local_kind_round_trip_as_unknown() {
            DateTime baseDate = BaseSeconds;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = DateTime.SpecifyKind(local.ToUniversalTime(), DateTimeKind.Unspecified);

            await AssertRoundTrip(
                new DataField<DateTime>("dateTime local kind"),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    expectedLocal,
                ]);
        }

        [Fact]
        public async Task Int96_impala_date_local_kind_round_trip_as_unknown() {
            DateTime baseDate = BaseSeconds;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = DateTime.SpecifyKind(local.ToUniversalTime(), DateTimeKind.Unspecified);

            await AssertRoundTrip(
                new DateTimeDataField("dateImpala local kind", DateTimeFormat.Impala),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    expectedLocal,
                ]);
        }

        [Fact]
        public async Task DateAndTime_local_kind_round_trip_as_utc() {
            DateTime baseDate = BaseMillis;
            DateTime local = DateTime.SpecifyKind(baseDate, DateTimeKind.Local);
            DateTime expectedLocal = local.ToUniversalTime();

            await AssertRoundTrip(
                new DateTimeDataField("dateDateAndTime local kind", DateTimeFormat.DateAndTime),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    local,
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(expectedLocal, DateTimeKind.Utc),
                ]);
        }

        [Fact]
        public async Task Timestamp_utc_kind_round_trip_as_utc() {
            DateTime baseDate = BaseMillis;

            await AssertRoundTrip(
                new DateTimeDataField("timestamp utc kind", DateTimeFormat.Timestamp, true),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Local),
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                ]);
        }

        [Fact]
        public async Task Timestamp_local_kind_round_trip_as_unknown() {
            DateTime baseDate = BaseMillis;

            await AssertRoundTrip(
                new DateTimeDataField("timestamp local kind", DateTimeFormat.Timestamp, false),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Local),
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                ]);
        }

        [Fact]
        public async Task Date_round_trip_as_utc() {
            DateTime baseDate = BaseDateOnly;

            await AssertRoundTrip(
                new DateTimeDataField("dateDate", DateTimeFormat.Date),
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Unspecified),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Local),
                ],
                [
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                    DateTime.SpecifyKind(baseDate, DateTimeKind.Utc),
                ]);
        }

        private async Task AssertRoundTrip(DataField field, DateTime[] values, DateTime[] expected) {
            var schema = new ParquetSchema(field);
            DataField schemaField = schema.DataFields[0];
            var column = new DataColumn(schemaField, values);
            DataColumn? actualColumn = await WriteReadSingleColumn(column);
            Assert.NotNull(actualColumn);

            DateTime[] actualValues = Assert.IsType<DateTime[]>(actualColumn.Data);
            Assert.Equal(expected.Length, actualValues.Length);
            for(int i = 0; i < expected.Length; i++) {
                Assert.Equal(expected[i].Ticks, actualValues[i].Ticks);
                Assert.Equal(expected[i].Kind, actualValues[i].Kind);
            }
        }
    }
}
