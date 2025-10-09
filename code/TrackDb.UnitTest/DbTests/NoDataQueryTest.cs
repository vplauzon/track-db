using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.UnitTest.DbTests;
using Xunit;

namespace TrackDb.UnitTest.DbTests
{
    public class NoDataQueryTest
    {
        [Fact]
        public async Task IntOnly()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var resultsAll = db.PrimitiveTable.Query()
                    .ToImmutableList();
            }
        }
    }
}