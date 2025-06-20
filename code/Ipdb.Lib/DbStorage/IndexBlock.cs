namespace Ipdb.Lib.DbStorage
{
    internal readonly record struct IndexBlock(
        Block Block,
        short MinHash,
        short MaxHash);
}