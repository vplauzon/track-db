namespace Ipdb.Lib.DbStorage
{
    internal readonly record struct DocumentBlock(
        int BlockId,
        long MinRevisionId,
        long MaxRevisionId,
        short RecordCount);
}