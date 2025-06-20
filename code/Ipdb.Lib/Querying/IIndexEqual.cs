using Ipdb.Lib.DbStorage;

namespace Ipdb.Lib.Querying
{
    internal interface IIndexEqual<T>
    {
        IndexDefinition<T> IndexDefinition{ get; }

        short KeyHash { get; }
    }
}