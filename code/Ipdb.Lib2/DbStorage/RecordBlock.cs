namespace Ipdb.Lib2.DbStorage
{
    internal readonly record struct RecordBlock(
        Block Block,
        long MinRevisionId,
        long MaxRevisionId);
}