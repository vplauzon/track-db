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
        }
    }
}