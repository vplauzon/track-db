namespace Ipdb.Lib.DbStorage
{
    internal readonly record struct IndexBlock(
        int BlockId,
        short Size,
        short MinHash,
        short MaxHash);
}