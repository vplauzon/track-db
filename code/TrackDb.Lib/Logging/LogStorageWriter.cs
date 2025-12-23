using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.Logging
{
    /// <summary>
    /// Manages log storage, i.e. Azure storage blobs.
    /// Abstract checkpoint + batch size + multiple log files.
    /// </summary>
    internal class LogStorageWriter : LogStorageBase, IAsyncDisposable
    {
        private readonly string _localFolder;
        private readonly DataLakeDirectoryClient _loggingDirectory;
        private readonly BlobContainerClient _loggingContainer;
        private long _currentLogBlobIndex = 0;
        private AppendBlobClient? _currentLogBlob;

        #region Constructor
        public LogStorageWriter(
            LogPolicy logPolicy,
            string localFolder,
            DataLakeDirectoryClient loggingDirectory,
            BlobContainerClient loggingContainer,
            long currentLogBlobIndex)
            : base(logPolicy)
        {
            _localFolder = localFolder;
            _loggingDirectory = loggingDirectory;
            _loggingContainer = loggingContainer;
            _currentLogBlobIndex = currentLogBlobIndex;
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        #region Checkpoint
        public async Task CreateCheckpointAsync(
            IAsyncEnumerable<string> transactionTexts,
            CancellationToken ct)
        {
            var currentLogBlobIndex = ++_currentLogBlobIndex;
            var checkpointFileName = GetCheckpointFileName(currentLogBlobIndex);
            var checkpointFilePath = Path.Combine(_localFolder, checkpointFileName);
            var tempCloudDirectory = _loggingDirectory.GetSubDirectoryClient("temp");
            var checkpointHeader = new CheckpointHeader(CURRENT_HEADER_VERSION);
            var checkpointHeaderText = checkpointHeader.ToJson();
            var deleteTempCloudDirectoryTask =
                tempCloudDirectory.DeleteIfExistsAsync(cancellationToken: ct);

            //  Write locally
            using (var stream = File.Create(checkpointFilePath))
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

            //  Copy to temp folder in cloud
            var checkpointFileClient = tempCloudDirectory.GetFileClient(checkpointFileName);
            var logFileName = GetCheckpointFileName(currentLogBlobIndex);

            await deleteTempCloudDirectoryTask;
            await checkpointFileClient.UploadAsync(
                checkpointFilePath,
                true,
                cancellationToken: ct);
            await checkpointFileClient.RenameAsync(
                _loggingDirectory
                .GetSubDirectoryClient(CHECKPOINT_BLOB_FOLDER)
                .GetFileClient(checkpointFileName).Path,
                cancellationToken: ct);
            _currentLogBlob = _loggingContainer.GetAppendBlobClient(
                $"{_loggingDirectory.Path}/{logFileName}");
            await _currentLogBlob.CreateIfNotExistsAsync(cancellationToken: ct);
        }
        #endregion

        #region Batch persistance
        public bool CanFitInBatch(IEnumerable<string> transactionTexts)
        {
            int totalLength = GetTotalLength(transactionTexts);

            return totalLength <= _currentLogBlob!.AppendBlobMaxAppendBlockBytes;
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