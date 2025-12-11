using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal record PersistanceCandidate(Table Table, bool DoPersistAll);
}