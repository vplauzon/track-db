using System;
using System.Collections.Generic;
using System.Text;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    internal class AvailabilityBlockManager
    {
        private readonly TypedTable<AvailableBlockRecord> _availableBlockTable;

        public AvailabilityBlockManager(TypedTable<AvailableBlockRecord> availableBlockTable)
        {
            _availableBlockTable = availableBlockTable;
        }
    }
}