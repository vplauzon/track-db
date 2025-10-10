using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.Logging
{
    internal class LogStorageManager : IAsyncDisposable
    {
        private readonly LogPolicy _logPolicy;
        private readonly DataLakeDirectoryClient _loggingDirectory;
        private readonly BlobContainerClient _loggingContainer;
        private readonly long _currentCheckpointBlobIndex = 1;
        private long _currentLogBlobIndex = 1;
        private AppendBlobClient? _currentLogBlob;

        #region Constructor
        public LogStorageManager(LogPolicy logPolicy)
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
        #endregion

        public int MaxBlockSize => _currentLogBlob!.AppendBlobMaxAppendBlockBytes;

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        #region Initialization
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
                await CreateInitialCheckpointAsync(ct);
                _currentLogBlob = _loggingContainer.GetAppendBlobClient(
                    _loggingDirectory.GetFileClient($"log-{_currentLogBlobIndex:D19}.json").Path);
                await _currentLogBlob.CreateIfNotExistsAsync();
            }
        }

        private async Task CreateInitialCheckpointAsync(CancellationToken ct)
        {
            var checkpointBlob = _loggingContainer.GetAppendBlobClient(
                _loggingDirectory.GetFileClient(
                    $"log-{_currentCheckpointBlobIndex:D19}.json").Path);
            var checkpointHeader = new CheckpointHeader(new Version(1, 0), 1);
            var checkpointHeaderText = JsonSerializer.Serialize(checkpointHeader);

            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            {
                writer.WriteLine(checkpointHeaderText);
                writer.Flush();
                stream.Position = 0;
                await checkpointBlob.AppendBlockAsync(stream);
            }
        }
        #endregion

        public async Task PersistBlockAsync(byte[] buffer)
        {
            using (var stream = new MemoryStream(buffer))
            {
                await _currentLogBlob!.AppendBlockAsync(stream);
            }
        }
    }
}