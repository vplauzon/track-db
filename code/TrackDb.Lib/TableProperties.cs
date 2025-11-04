namespace TrackDb.Lib
{
    internal record TableProperties(
        Table Table,
        string? MetaDataTableName,
        bool IsUserTable,
        bool IsMetaDataTable,
        bool IsSystemTable,
        bool IsPersisted);
}