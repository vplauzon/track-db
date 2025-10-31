using System.Collections.Immutable;

namespace TrackDb.Lib.Logging
{
    internal record TombstoneContent(
        IImmutableList<long> RecordId,
        IImmutableList<long> DeletedRecordId);
}