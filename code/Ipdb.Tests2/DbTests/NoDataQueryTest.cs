using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2.DbTests
{
    public class NoDataQueryTest
    {
        [Fact]
        public async Task IntOnly()
        {
            await using (var testTable = DbTestTables.CreateIntOnly())
            {
                var resultsAll = testTable.Table.Query()
                    .ToImmutableList();
            }
        }
    }
}