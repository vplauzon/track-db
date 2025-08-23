using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class JitReadOnlyDataColumn : IReadOnlyDataColumn
    {
        private readonly Lazy<IDataColumn> _innerColumn;

        public JitReadOnlyDataColumn(
            Func<int, IDataColumn> columnFactory,
            SerializedColumn serializedColumn)
        {
            _innerColumn = new Lazy<IDataColumn>(
                () =>
                {
                    var column = columnFactory(serializedColumn.ItemCount);

                    column.Deserialize(serializedColumn);

                    return column;
                },
                LazyThreadSafetyMode.ExecutionAndPublication);
        }

        int IReadOnlyDataColumn.RecordCount => _innerColumn.Value.RecordCount;

        object? IReadOnlyDataColumn.GetValue(int index)
        {
            return _innerColumn.Value.GetValue(index);
        }

        IEnumerable<int> IReadOnlyDataColumn.Filter(BinaryOperator binaryOperator, object? value)
        {
            return _innerColumn.Value.Filter(binaryOperator, value);
        }
    }
}