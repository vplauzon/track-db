using System.Collections.Immutable;

namespace Ipdb.Lib2.Cache
{
    internal record ImmutableTransactionLog(
        IImmutableDictionary<string, ImmutableTableTransactionLog> TableTransactionLogs);
}