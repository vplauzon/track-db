using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class ReadOnlyBlock : ReadOnlyBlockBase
    {
        #region Constructors
        public ReadOnlyBlock(TableSchema schema, SerializedBlock serializedBlock)
            :base(schema, CreateColumns(serializedBlock))
        {
        }

        private static IEnumerable<IReadOnlyDataColumn> CreateColumns(SerializedBlock serializedBlock)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}