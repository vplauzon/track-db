namespace Ipdb.Lib.Cache
{
    internal record IndexBlock(
        int BlockId,
        short MinHash,
        short MaxHash,
        short RecordCount);
}