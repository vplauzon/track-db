namespace TrackDb.Lib
{
    internal record struct BlockTrace(
        //  Schema of the block's table
        TableSchema Schema,
        //  Block ID:  if positive, it's on disk, if <=0, it's the negative
        //  index of the in-memory block
        int BlockId,
        //  Row index within the block
        int RowIndex);
}