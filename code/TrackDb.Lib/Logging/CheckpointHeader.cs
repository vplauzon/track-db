using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace TrackDb.Lib.Logging
{
    internal record CheckpointHeader(Version Version)
        : ContentBase<CheckpointHeader>
    {
    }
}