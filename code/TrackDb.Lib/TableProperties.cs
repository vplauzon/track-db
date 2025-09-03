namespace TrackDb.Lib
{
    internal record TableProperties(
        Table Table,
        string? MetaDataTableName,
        bool IsUserTable,
        bool IsMetaDataTable,
        bool IsTombstone)
    {
        public bool IsPersisted => IsUserTable || IsMetaDataTable;
        
        public bool IsLogged => IsUserTable;
    }
}