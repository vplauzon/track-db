namespace TrackDb.Lib
{
    internal record MetadataColumnCorrespondance(
        int ColumnIndex,
        ColumnSchema ColumnSchema,
        int? MetaColumnIndex,
        int? MetaMinColumnIndex,
        int? MetaMaxColumnIndex);
}