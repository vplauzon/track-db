namespace TrackDb.Lib.DataLifeCycle
{
    internal class LogicBase
    {
        protected LogicBase(Database database)
        {
            Database = database;
        }

        protected Database Database { get; }
    }
}