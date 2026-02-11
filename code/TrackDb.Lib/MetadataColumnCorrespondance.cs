namespace TrackDb.Lib
{
    internal record MetadataColumnCorrespondance(
        int columnIndex,
        int? metaColumnIndex,
        int? metaMinColumnIndex,
        int? metaMaxColumnIndex);
}