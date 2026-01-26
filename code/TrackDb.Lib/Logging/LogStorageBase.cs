using Azure;
using Polly;
using Polly.Retry;
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

        /// <summary>Workaround for Data lake SDK.</summary>
        protected static readonly AsyncRetryPolicy Handle409Policy = Policy
            .Handle<RequestFailedException>(ex => ex.Status == 409 && ex.ErrorCode == "PathAlreadyExists")
            .RetryAsync(0); // 0 retries = just swallow the exception

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