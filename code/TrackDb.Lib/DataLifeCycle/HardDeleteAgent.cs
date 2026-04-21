namespace TrackDb.Lib.DataLifeCycle
{
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