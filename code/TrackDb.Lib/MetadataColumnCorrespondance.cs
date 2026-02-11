namespace TrackDb.Lib
{
    internal record MetadataColumnCorrespondance(
        int columnIndex,
        ColumnSchema columnSchema,
        int? metaColumnIndex,
        int? metaMinColumnIndex,
        int? metaMaxColumnIndex);
}