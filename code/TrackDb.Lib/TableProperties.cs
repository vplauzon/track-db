namespace TrackDb.Lib
{
    internal record TableProperties(
        Table Table,
        int Generation,
        string? MetaDataTableName,
        bool IsSystemTable)
    {
        public bool IsUserTable => !IsSystemTable && Generation == 1;

        public bool IsPersisted => IsUserTable;

        public bool IsMetaDataTable => Generation != 1;
    }
}