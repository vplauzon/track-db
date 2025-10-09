using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.Logging
{
    internal class LogManager : IAsyncDisposable
    {
        private static readonly string SEPARATOR = "\n";

        private readonly LogPolicy _logPolicy;
        private readonly ConcurrentQueue<string> _contentQueue = new();
        private readonly DataLakeDirectoryClient _loggingDirectory;
        private volatile int _contentLength;

        public LogManager(LogPolicy logPolicy)
        {
            if (logPolicy.StorageConfiguration == null)
            {
                throw new ArgumentNullException(nameof(logPolicy.StorageConfiguration));
            }

            _logPolicy = logPolicy;
            _loggingDirectory = new DataLakeDirectoryClient(
                _logPolicy.StorageConfiguration.LogFolderUri,
                _logPolicy.StorageConfiguration.TokenCredential);
        }

        public async Task InitLogsAsync()
        {
            await Task.CompletedTask;
        }

        public void QueueContent(string contentText)
        {
            _contentQueue.Enqueue(contentText);

            var totalLength =
                Interlocked.Add(ref _contentLength, contentText.Length + SEPARATOR.Length);
        }

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            throw new NotImplementedException();
        }
    }
}