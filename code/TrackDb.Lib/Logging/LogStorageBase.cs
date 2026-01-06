using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
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
        protected static readonly string SEPARATOR = "\n";

        protected static Version CURRENT_HEADER_VERSION = new(1, 0);

        protected LogStorageBase(LogPolicy logPolicy, string localFolder, BlobClients blobClients)
        {
            if (logPolicy.StorageConfiguration == null)
            {
                throw new ArgumentNullException(nameof(logPolicy.StorageConfiguration));
            }

            LogPolicy = logPolicy;
            LocalFolder = localFolder;
            BlobClients = blobClients;
        }

        public LogPolicy LogPolicy { get; }

        protected string LocalFolder { get; }

        protected BlobClients BlobClients { get; }

        protected static string GetCheckpointFileName(long index)
        {
            return $"{index:D19}-checkpoint.json";
        }

        protected static string GetLogFileName(long index)
        {
            return $"{index:D19}-log.json";
        }
    }
}