using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.Logging
{
    internal class LogStorageBase
    {
        protected const string CHECKPOINT_BLOB_FOLDER = "checkpoint";

        protected static readonly string SEPARATOR = "\n";

        protected static Version CURRENT_HEADER_VERSION = new(1, 0);

        protected LogStorageBase(LogPolicy logPolicy)
        {
            if (logPolicy.StorageConfiguration == null)
            {
                throw new ArgumentNullException(nameof(logPolicy.StorageConfiguration));
            }

            LogPolicy = logPolicy;
        }

        public LogPolicy LogPolicy { get; }

        protected static string GetCheckpointFileName(long index)
        {
            return $"checkpoint-{index:D19}.json";
        }

        protected static string GetLogFileName(long index)
        {
            return $"log-{index:D19}.json";
        }
    }
}