namespace TrackDb.Lib
{
    internal record MetadataColumnCorrespondance(
        int ColumnIndex,
        ColumnSchema ColumnSchema,
        int MetaMinColumnIndex,
        int MetaMaxColumnIndex);
}