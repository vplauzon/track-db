using System.Linq;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TombstoneMovementLogic : LogicBase
    {
        public TombstoneMovementLogic(Database database)
            : base(database)
        {
        }

        public void MoveTombstones(TransactionContext tx)
        {
            var recordIdsbyTable = Database.TombstoneTable.Query(tx)
                .GroupBy(t => t.TableName)
                .ToDictionary(g => g.Key, g => g.Select(t => t.DeletedRecordId).ToArray());
        }
    }
}