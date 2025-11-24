using TrackDb.Lib.InMemory;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    /// <summary>
    /// Whole state for a database, including all in-memory data in the "main branch"
    /// and each transaction.
    /// </summary>
    /// <param name="InMemoryDatabase"></param>
    /// <param name="TableMap"></param>
    internal record DatabaseState(
        InMemoryDatabase InMemoryDatabase,
        IImmutableDictionary<string, TableProperties> TableMap)
    {
        public DatabaseState(IImmutableDictionary<string, TableProperties> tableMap)
            : this(new InMemoryDatabase(), tableMap)
        {
        }
    }
}