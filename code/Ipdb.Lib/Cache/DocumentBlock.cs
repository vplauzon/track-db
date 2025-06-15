namespace Ipdb.Lib.Cache
{
    internal record DocumentBlock(
        int BlockId,
        long MinRevisionId,
        long MaxRevisionId,
        short RecordCount);
}