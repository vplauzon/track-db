using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Specialized;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
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
        #region Inner Types
        private class TempFile : IDisposable
        {
            private readonly string _localPath;

            #region Constructor
            private TempFile(string localPath)
            {
                _localPath = localPath;
            }

            public async static Task<TempFile?> LoadTempFileAsync(
                string localFolder,
                DataLakeFileClient file,
                CancellationToken ct)
            {
                try
                {
                    var tempLocalCheckpointPath = Path.Combine(localFolder, Guid.NewGuid().ToString());

                    await file.ReadToAsync(tempLocalCheckpointPath, cancellationToken: ct);

                    return new TempFile(tempLocalCheckpointPath);
                }
                catch (RequestFailedException)
                {
                    return null;
                }
            }
            #endregion

            void IDisposable.Dispose()
            {
                File.Delete(_localPath);
            }

            public FileStream OpenRead()
            {
                return File.OpenRead(_localPath);
            }
        }
        #endregion

        private const string TEMP_FOLDER = "temp";
        private const string CHECKPOINT_FOLDER = "checkpoint";

        private static readonly Version HEADER_VERSION = new(1, 0);
        private static readonly string SEPARATOR = "\n";

        private readonly LogPolicy _logPolicy;
        private readonly string _localFolder;
        private readonly DataLakeDirectoryClient _loggingDirectory;
        private readonly BlobContainerClient _loggingContainer;
        private bool _isCheckpointCreationRequired = false;
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

        public bool IsCheckpointCreationRequired => _isCheckpointCreationRequired;

        #region Load
        public async IAsyncEnumerable<string> LoadAsync(
            [EnumeratorCancellation]
            CancellationToken ct)
        {
            var deleteTempTask = _loggingDirectory
                .GetSubDirectoryClient(TEMP_FOLDER)
                .DeleteIfExistsAsync(cancellationToken: ct);
            var checkpointDirectory = _loggingDirectory.GetSubDirectoryClient(CHECKPOINT_FOLDER);
            var createCheckpointDirectoryTask =
                checkpointDirectory.CreateIfNotExistsAsync(cancellationToken: ct);
            var accountInfo =
                await _loggingContainer.GetParentBlobServiceClient().GetAccountInfoAsync(ct);

            if (!accountInfo.Value.IsHierarchicalNamespaceEnabled)
            {
                throw new InvalidOperationException(
                    $"Storage account {_loggingContainer.GetParentBlobServiceClient().Uri} " +
                    $"must support hierarchical namespace");
            }
            await Task.WhenAll(deleteTempTask, createCheckpointDirectoryTask);

            var checkpointPathList = await checkpointDirectory.GetPathsAsync(cancellationToken: ct)
                .ToImmutableListAsync();
            var lastCheckpoint = checkpointPathList
                .Where(i => i.IsDirectory == false)
                .Select(i => _loggingDirectory.GetParentFileSystemClient().GetFileClient(i.Name))
                .Where(f => f.Name.EndsWith(".json"))
                .Where(f => f.Name.StartsWith("checkpoint-"))
                .OrderBy(f => f.Name)
                .LastOrDefault();

            //  Enables GC
            checkpointPathList = checkpointPathList.Clear();
            if (lastCheckpoint != null)
            {
                await foreach (var text in LoadLinesFromCheckpointAsync(lastCheckpoint, ct))
                {
                    yield return text;
                }
            }
            else
            {   //  Nothing in storage, we need to create checkpoint
                _isCheckpointCreationRequired = true;
            }
        }

        private async IAsyncEnumerable<string> LoadLinesFromCheckpointAsync(
            DataLakeFileClient lastCheckpoint,
            [EnumeratorCancellation]
            CancellationToken ct)
        {
            _currentLogBlobIndex = long.Parse(lastCheckpoint.Name.Split('.')[0].Split('-')[1]);

            var nextLogTask = TempFile.LoadTempFileAsync(_localFolder, lastCheckpoint, ct);
            var isFirstLine = true;

            while (true)
            {
                using (var currentLogFile = await nextLogTask)
                {
                    if (currentLogFile == null)
                    {
                        if (isFirstLine)
                        {
                            _isCheckpointCreationRequired = true;
                        }
                        yield break;
                    }
                    else
                    {   //  Always read forward
                        nextLogTask = LoadNextLogAsync(isFirstLine, ct);
                        using (currentLogFile)
                        using (var fileStream = currentLogFile.OpenRead())
                        using (var reader = new StreamReader(fileStream))
                        {
                            string? line;

                            while ((line = reader.ReadLine()) != null)
                            {
                                if (isFirstLine)
                                {
                                    if (line == null)
                                    {
                                        throw new InvalidDataException("Checkpoint has no header");
                                    }

                                    var checkpointHeader = CheckpointHeader.FromJson(line);

                                    ValidateHeaderVersion(checkpointHeader.Version);
                                    isFirstLine = false;
                                }
                                else if(!string.IsNullOrEmpty(line))
                                {
                                    yield return line;
                                }
                            }
                            if (isFirstLine)
                            {
                                throw new InvalidDataException("Checkpoint is empty");
                            }
                        }
                    }
                }
            }
        }

        private async Task<TempFile?> LoadNextLogAsync(bool isFirstLog, CancellationToken ct)
        {
            var nextLogBlobIndex = isFirstLog
                ? _currentLogBlobIndex
                : _currentLogBlobIndex + 1;
            var tempFile = await TempFile.LoadTempFileAsync(
                _localFolder,
                _loggingDirectory.GetFileClient($"log-{GetPaddedIndex(nextLogBlobIndex)}.json"),
                ct);

            _currentLogBlobIndex = nextLogBlobIndex;

            return tempFile;
        }

        private void ValidateHeaderVersion(Version version)
        {
            if (version != HEADER_VERSION)
            {
                throw new NotSupportedException(
                    $"Unsupported checkpoint header version:  {version}");
            }
        }
        #endregion

        #region Checkpoint
        public async Task CreateCheckpointAsync(
            IAsyncEnumerable<string> transactionTexts,
            CancellationToken ct)
        {
            ++_currentLogBlobIndex;

            var checkpointFileName = $"checkpoint-{GetPaddedIndex(_currentLogBlobIndex)}.json";
            var logFileName = $"log-{GetPaddedIndex(_currentLogBlobIndex)}.json";
            var tempCheckpointFileName = Path.Combine(_localFolder, checkpointFileName);
            var tempCloudDirectory = _loggingDirectory.GetSubDirectoryClient(TEMP_FOLDER);
            var checkpointHeader = new CheckpointHeader(HEADER_VERSION);
            var checkpointHeaderText = checkpointHeader.ToJson();

            using (var stream = File.Create(tempCheckpointFileName))
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
                tempCheckpointFileName,
                true,
                cancellationToken: ct);
            await checkpointFileClient.RenameAsync(
                _loggingDirectory
                .GetSubDirectoryClient(CHECKPOINT_FOLDER)
                .GetFileClient(checkpointFileName).Path,
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

        private string GetPaddedIndex(long index)
        {
            return $"{index:D19}";
        }
    }
}