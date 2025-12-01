namespace TrackDb.Lib
{
    internal record TableProperties(
        Table Table,
        int Generation,
        string? MetaDataTableName,
        bool IsSystemTable,
        bool IsPersisted)
    {
        public bool IsUserTable => !IsSystemTable && Generation == 1;

        public bool IsMetaDataTable => Generation != 1;
    }
}