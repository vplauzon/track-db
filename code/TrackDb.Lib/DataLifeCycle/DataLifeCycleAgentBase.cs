using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class DataLifeCycleAgentBase : LogicBase
    {
        protected DataLifeCycleAgentBase(Database database)
            : base(database)
        {
        }

        /// <summary>Runs an agent logic.</summary>
        /// <param name="forcedDataManagementActivity"></param>
        public abstract void Run(DataManagementActivity forcedDataManagementActivity);
    }
}