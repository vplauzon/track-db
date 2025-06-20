namespace Ipdb.Lib.DbStorage
{
    internal readonly record struct DocumentBlock(
        Block Block,
        long MinRevisionId,
        long MaxRevisionId);
}