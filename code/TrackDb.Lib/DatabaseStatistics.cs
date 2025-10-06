namespace TrackDb.Lib
{
    public record DatabaseStatistics(
        int InMemoryUserTableRecords,
        int InMemoryTombstoneRecords);
}