namespace Ipdb.Lib.Indexing
{
    internal record IndexBlock(
        int BlockId,
        short MinHash,
        short MaxHash,
        short RecordCount);
}