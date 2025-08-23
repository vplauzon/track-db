using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.Tests.DbTests
{
    public class QueryWithDeleteTest
    {
        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task IntOnly(bool doPushPendingData)
        {
            await using (var testTable = DbTestTables.CreateIntOnly())
            {
                testTable.Table.AppendRecord(new DbTestTables.IntOnly(1));
                testTable.Table.AppendRecord(new DbTestTables.IntOnly(2));
                testTable.Table.AppendRecord(new DbTestTables.IntOnly(3));
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);

                //  Delete
                testTable.Table.Query()
                    .Where(i => i.Integer == 2)
                    .Delete();

                var resultsAll = testTable.Table.Query()
                    .ToImmutableList();

                Assert.Equal(2, resultsAll.Count);
                Assert.Contains(1, resultsAll.Select(r => r.Integer));
                Assert.Contains(3, resultsAll.Select(r => r.Integer));

                var resultsEqual = testTable.Table.Query()
                    .Where(i => i.Integer == 2)
                    .ToImmutableList();

                Assert.Empty(resultsEqual);

                var resultsNotEqual = testTable.Table.Query()
                    .Where(i => i.Integer != 2)
                    .ToImmutableList();

                Assert.Equal(2, resultsNotEqual.Count);
                Assert.Contains(1, resultsNotEqual.Select(r => r.Integer));
                Assert.Contains(3, resultsNotEqual.Select(r => r.Integer));

                var resultsLessThan = testTable.Table.Query()
                    .Where(i => i.Integer < 2)
                    .ToImmutableList();

                Assert.Single(resultsLessThan);
                Assert.Equal(1, resultsLessThan[0].Integer);

                var resultsLessThanOrEqual = testTable.Table.Query()
                    .Where(i => i.Integer <= 2)
                    .ToImmutableList();

                Assert.Single(resultsLessThanOrEqual);
                Assert.Contains(1, resultsLessThanOrEqual.Select(r => r.Integer));

                var resultsGreaterThan = testTable.Table.Query()
                    .Where(i => i.Integer > 2)
                    .ToImmutableList();

                Assert.Single(resultsGreaterThan);
                Assert.Equal(3, resultsGreaterThan[0].Integer);

                var resultsGreaterThanOrEqual = testTable.Table.Query()
                    .Where(i => i.Integer >= 2)
                    .ToImmutableList();

                Assert.Single(resultsGreaterThanOrEqual);
                Assert.Contains(3, resultsGreaterThanOrEqual.Select(r => r.Integer));
            }
        }
    }
}