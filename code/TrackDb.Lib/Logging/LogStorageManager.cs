using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Specialized;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.Logging
{
    /// <summary>
    /// Manages log storage, i.e. Azure storage blobs.
    /// Abstract checkpoint + batch size + multiple log files.
    /// </summary>
    internal class LogStorageManager : IAsyncDisposable
    {
        private const string TEMP_FOLDER = "temp";

        private static readonly string SEPARATOR = "\n";

        private readonly LogPolicy _logPolicy;
        private readonly string _localFolder;
        private readonly DataLakeDirectoryClient _loggingDirectory;
        private readonly BlobContainerClient _loggingContainer;
        private long _currentCheckpointBlobIndex = 0;
        private long _currentLogBlobIndex = 0;
        private AppendBlobClient? _currentLogBlob;

        #region Constructor
        public LogStorageManager(LogPolicy logPolicy, string localFolder)
        {
            if (logPolicy.StorageConfiguration == null)
            {
                throw new ArgumentNullException(nameof(logPolicy.StorageConfiguration));
            }

            _logPolicy = logPolicy;
            _localFolder = localFolder;
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
                    _logPolicy.StorageConfiguration.KeyCredential);

                _loggingDirectory = new DataLakeDirectoryClient(
                    _logPolicy.StorageConfiguration.LogFolderUri,
                    _logPolicy.StorageConfiguration.KeyCredential);
                _loggingContainer = dummyBlob.GetParentBlobContainerClient();
            }
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        #region Load
        public async Task<LogStorageLoadOutput> LoadLogsAsync(CancellationToken ct)
        {
            var deleteTempTask = _loggingDirectory
                .GetSubDirectoryClient(TEMP_FOLDER)
                .DeleteIfExistsAsync(cancellationToken: ct);
            var createDirectoryTask =
                _loggingDirectory.CreateIfNotExistsAsync(cancellationToken: ct);
            var accountInfoTask =
                _loggingContainer.GetParentBlobServiceClient().GetAccountInfoAsync(ct);

            await Task.WhenAll(deleteTempTask, createDirectoryTask, accountInfoTask);
            if (!accountInfoTask.Result.Value.IsHierarchicalNamespaceEnabled)
            {
                throw new InvalidOperationException(
                    $"Storage account {_loggingContainer.GetParentBlobServiceClient().Uri} " +
                    $"must support hierarchical namespace");
            }

            var blobList = await _loggingDirectory.GetPathsAsync(cancellationToken: ct)
                .ToImmutableListAsync();
            var lastCheckpoint = blobList
                .Where(i => i.IsDirectory == false)
                .Select(i => _loggingDirectory.GetParentFileSystemClient()
                .GetFileClient(i.Name))
                .Where(f => f.Name.StartsWith("checkpoint-"))
                .Where(f => f.Name.EndsWith(".json"))
                .OrderBy(f => f.Name)
                .LastOrDefault();

            if (lastCheckpoint != null)
            {
                throw new NotImplementedException();
            }
            else
            {   //  Nothing in storage, we need to create checkpoint
                return new LogStorageLoadOutput(true, AsyncEnumerable.Empty<string>());
            }
        }
        #endregion

        #region Checkpoint
        public async Task CreateCheckpointAsync(
            IAsyncEnumerable<string> transactionTexts,
            CancellationToken ct)
        {
            var checkpointFileName = $"checkpoint-{++_currentCheckpointBlobIndex:D19}.json";
            var logFileName = $"log-{++_currentLogBlobIndex:D19}.json";
            var tempLocalPath = Path.Combine(_localFolder, checkpointFileName);
            var tempCloudDirectory = _loggingDirectory.GetSubDirectoryClient(TEMP_FOLDER);
            var checkpointHeader = new CheckpointHeader(new Version(1, 0), 1);
            var checkpointHeaderText = JsonSerializer.Serialize(checkpointHeader);

            using (var stream = File.Create(tempLocalPath))
            using (var writer = new StreamWriter(stream))
            {
                writer.Write(checkpointHeaderText);
                await foreach (var tx in transactionTexts)
                {
                    ct.ThrowIfCancellationRequested();
                    writer.Write(SEPARATOR);
                    writer.Write(tx);
                }
            }

            var checkpointFileClient = tempCloudDirectory.GetFileClient(checkpointFileName);
            var logFileClient = _loggingDirectory.GetFileClient(logFileName);

            await checkpointFileClient.UploadAsync(
                tempLocalPath,
                true,
                cancellationToken: ct);
            await checkpointFileClient.RenameAsync(
                _loggingDirectory.GetFileClient(checkpointFileName).Path,
                cancellationToken: ct);
            _currentLogBlob = _loggingContainer.GetAppendBlobClient(logFileClient.Path);
            await _currentLogBlob.CreateIfNotExistsAsync(cancellationToken: ct);
        }
        #endregion

        #region Batch persistance
        public bool CanFitInBatch(IEnumerable<string> transactionTexts)
        {
            int totalLength = GetTotalLength(transactionTexts);

            return totalLength > _currentLogBlob!.AppendBlobMaxAppendBlockBytes;
        }

        public async Task PersistBatchAsync(IEnumerable<string> transactionTexts)
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            {
                var totalLength = GetTotalLength(transactionTexts);

                if (totalLength > _currentLogBlob!.AppendBlobMaxAppendBlockBytes)
                {
                    if (transactionTexts.Count() > 1)
                    {
                        throw new ArgumentException(
                            $"'{transactionTexts.Count()}' transactions",
                            nameof(transactionTexts));
                    }
                    else
                    {
                        throw new NotSupportedException("Too large transaction");
                    }
                }
                else
                {
                    foreach (var transactionText in transactionTexts)
                    {
                        writer.Write(SEPARATOR);
                        writer.Write(transactionText);
                    }
                    writer.Flush();
                    stream.Position = 0;
                    if (totalLength != stream.Length)
                    {
                        throw new InvalidDataException(
                            $"Expected batch size to be '{totalLength}' but is '{stream.Length}'");
                    }
                    await _currentLogBlob!.AppendBlockAsync(stream);
                }
            }
        }

        private static int GetTotalLength(IEnumerable<string> transactionTexts)
        {
            var contentLength = transactionTexts.Sum(t => t.Length);
            var separatorsLength = transactionTexts.Count() * SEPARATOR.Length;
            var totalLength = contentLength + separatorsLength;

            return totalLength;
        }
        #endregion
    }
}