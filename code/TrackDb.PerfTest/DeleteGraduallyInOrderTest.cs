using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace TrackDb.PerfTest
{
    public class DeleteGraduallyInOrderTest : DeleteGraduallyTestBase
    {
        protected override IImmutableList<string> ManipulateEmployeeIds(
            IImmutableList<string> employeeIds)
        {
            return employeeIds
                .OrderBy(x => x)
                .ToImmutableArray();
        }
    }
}