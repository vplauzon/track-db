using System;

namespace TrackDb.Lib.Policies
{
    public record InMemoryPolicy(
        int MaxBlocksPerTable,
        int MaxNonMetaDataRecords,
        int MaxMetaDataRecords)
    {
        public static InMemoryPolicy Create(
            int? MaxBlocksPerTable = null,
            int? MaxNonMetaDataRecords = null,
            int? MaxMetaDataRecords = null)
        {
            return new InMemoryPolicy(
                 MaxBlocksPerTable ?? 10,
                 MaxNonMetaDataRecords ?? 500,
                 MaxMetaDataRecords ?? 300);
        }
    }
}