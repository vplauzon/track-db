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
        private readonly DataLakeDirectoryClient _loggingDirectory;
        private readonly BlobContainerClient _loggingContainer;
        private readonly ConcurrentQueue<string> _contentQueue = new();
        private volatile int _contentLength;
        private long _currentLogBlobIndex = 1;
        private AppendBlobClient? _currentLogBlob;

        public LogManager(LogPolicy logPolicy)
        {
            if (logPolicy.StorageConfiguration == null)
            {
                throw new ArgumentNullException(nameof(logPolicy.StorageConfiguration));
            }

            _logPolicy = logPolicy;
            if (_logPolicy.StorageConfiguration.TokenCredential != null)
            {
                var dummyBlob = new AppendBlobClient(
                    _logPolicy.StorageConfiguration.LogFolderUri,
                    _logPolicy.StorageConfiguration.TokenCredential);

                _loggingDirectory = new DataLakeDirectoryClient(
                    _logPolicy.StorageConfiguration.LogFolderUri,
                    _logPolicy.StorageConfiguration.TokenCredential);
                _loggingContainer = dummyBlob.GetParentBlobContainerClient();
            }
            else
            {
                var dummyBlob = new AppendBlobClient(
                    _logPolicy.StorageConfiguration.LogFolderUri,
                    _logPolicy.StorageConfiguration.SasCredential);

                _loggingDirectory = new DataLakeDirectoryClient(
                    _logPolicy.StorageConfiguration.LogFolderUri,
                    _logPolicy.StorageConfiguration.SasCredential);
                _loggingContainer = dummyBlob.GetParentBlobContainerClient();
            }
        }

        public async Task InitLogsAsync(CancellationToken ct)
        {
            await _loggingDirectory.CreateIfNotExistsAsync(cancellationToken: ct);

            var blobList = await _loggingDirectory.GetPathsAsync(cancellationToken: ct)
                .ToImmutableListAsync();
            var lastCheckpoint = blobList
                .Select(i => i.Name)
                .Where(n => n.StartsWith("checkpoint-"))
                .Where(n => n.EndsWith(".json"))
                .OrderBy(n => n)
                .LastOrDefault();

            if (lastCheckpoint != null)
            {
                throw new NotImplementedException();
            }
            else
            {
                _currentLogBlob = _loggingContainer.GetAppendBlobClient(
                    _loggingDirectory.GetFileClient($"log-{_currentLogBlobIndex:D19}.json").Path);
                await _currentLogBlob.CreateIfNotExistsAsync();
            }
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