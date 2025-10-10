using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
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
        private readonly LogStorageManager _logStorageManager;
        private readonly ConcurrentQueue<string> _contentQueue = new();
        private volatile int _contentLength;

        public LogManager(LogPolicy logPolicy)
        {
            _logPolicy = logPolicy;
            _logStorageManager = new LogStorageManager(logPolicy);
        }

        public async Task InitLogsAsync(CancellationToken ct)
        {
            await _logStorageManager.InitLogsAsync(ct);
        }

        public void QueueContent(string contentText)
        {
            _contentQueue.Enqueue(contentText);

            var totalLength =
                Interlocked.Add(ref _contentLength, contentText.Length + SEPARATOR.Length);
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)_logStorageManager).DisposeAsync();
        }
    }
}