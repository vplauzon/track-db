namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal record MetaMetaBlockStat(
        int? BlockId,
        long MinRecordId,
        long MaxRecordId,
        //  This is estimated, i.e. according to min-max in tombstone tables 
        long TombstonedRecordCount);
}