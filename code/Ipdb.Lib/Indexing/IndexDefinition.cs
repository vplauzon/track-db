using System;
using System.Collections.Immutable;

namespace Ipdb.Lib
{
public partial class TableSchema<T>
    {
        private record IndexDefinition(
            Func<T, object> objectExtractor,
            IImmutableList<IndexType> IndexTypes)
        {
            public static IndexDefinition CreateIndex<PT>(Func<T, PT> propertyExtractor)
                where PT : notnull
            {
                return new IndexDefinition(
                    o => propertyExtractor(o),
                    GetIndexTypes<PT>());
            }
        }
    }
}
