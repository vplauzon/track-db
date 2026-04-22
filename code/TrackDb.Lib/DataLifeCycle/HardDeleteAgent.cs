namespace TrackDb.Lib.DataLifeCycle
{
    /// <summary>
    /// Hard delete records by compacting blocks (and merging).
    /// Performs the following:
    /// 
    /// <list type="bullet">
    /// <item>
    /// Remove blocks that are fully tombstoned.  This is done at the cadence of
    /// <see cref="TombstonePolicy.FullBlockPeriod"/>.
    /// </item>
    /// <item>
    /// Compact blocks that are partially tombstoned.  This is done at the cadence of
    /// <see cref="TombstonePolicy.PartialBlockPeriod"/> for blocks having at least
    /// a ratio of <see cref="TombstonePolicy.PartialBlockRatio"/>.
    /// </item>
    /// <item>
    /// Compact blocks that have been tombstoned and untouched for at least
    /// <see cref="TombstonePolicy.TombstoneRetentionPeriod"/>.
    /// </item>
    /// </list>
    /// </summary>
    internal class HardDeleteAgent : DataLifeCycleAgentBase
    {
        public HardDeleteAgent(Database database)
            : base(database)
        {
        }

        public override void Run(DataManagementActivity forcedDataManagementActivity)
        {
            using (var tx = Database.CreateTransaction())
            {
                var tombstoneMovementAgent = new TombstoneMovementLogic(Database);

                tombstoneMovementAgent.MoveTombstones(tx);

                tx.Complete();
            }
        }
    }
}