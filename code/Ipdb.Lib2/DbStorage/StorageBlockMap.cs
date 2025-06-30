using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.DbStorage
{
    internal class StorageBlockMap
    {
        private readonly IImmutableDictionary<string, IImmutableList<RecordBlock>> _tableMap;

        #region Constructors
        private StorageBlockMap(
            IImmutableDictionary<string, IImmutableList<RecordBlock>> tableMap)
        {
            _tableMap = tableMap;
        }
        #endregion

        public static StorageBlockMap Empty { get; } = new(
            ImmutableDictionary<string, IImmutableList<RecordBlock>>.Empty);
    }
}