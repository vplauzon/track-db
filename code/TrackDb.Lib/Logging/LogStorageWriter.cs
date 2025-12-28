using Azure;
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
        private long _currentLogBlobIndex = 0;
        private AppendBlobClient _currentLogBlob;

        #region Constructor
        public static async Task<LogStorageWriter> CreateLogStorageWriterAsync(
            LogPolicy logPolicy,
            string localFolder,
            DataLakeDirectoryClient loggingDirectory,
            BlobContainerClient loggingContainer,
            long? currentLogBlobIndex,
            CancellationToken ct = default)
        {
            var logStorageWriter = new LogStorageWriter(
                logPolicy,
                localFolder,
                loggingDirectory,
                loggingContainer,
                currentLogBlobIndex ?? 0);

            if (currentLogBlobIndex == null)
            {
                await logStorageWriter.CreateCheckpointAsync(
                Array.Empty<string>().ToAsyncEnumerable(),
                ct);
            }

            return logStorageWriter;
        }

        private LogStorageWriter(
            LogPolicy logPolicy,
            string localFolder,
            DataLakeDirectoryClient loggingDirectory,
            BlobContainerClient loggingContainer,
            long currentLogBlobIndex)
            : base(logPolicy, localFolder, loggingDirectory, loggingContainer)
        {
            _currentLogBlobIndex = currentLogBlobIndex;
            _currentLogBlob = GetAppendBlobClient();
        }

        private async Task EnsureCurrentLogBlobAsync(CancellationToken ct)
        {
            _currentLogBlob = GetAppendBlobClient();
            await _currentLogBlob.CreateIfNotExistsAsync(cancellationToken: ct);
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
            var checkpointFilePath = Path.Combine(LocalFolder, checkpointFileName);
            var tempCloudDirectory = LoggingDirectory.GetSubDirectoryClient("temp");
            var checkpointHeader = new CheckpointHeader(CURRENT_HEADER_VERSION);
            var checkpointHeaderText = checkpointHeader.ToJson();
            var deleteTempCloudDirectoryTask =
                tempCloudDirectory.DeleteIfExistsAsync(cancellationToken: ct);

            Directory.CreateDirectory(LocalFolder);
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

            await deleteTempCloudDirectoryTask;
            await checkpointFileClient.UploadAsync(
                checkpointFilePath,
                true,
                cancellationToken: ct);
            File.Delete(checkpointFilePath);
            await checkpointFileClient.RenameAsync(
                LoggingDirectory
                .GetFileClient(checkpointFileName).Path,
                cancellationToken: ct);
            _currentLogBlob = GetAppendBlobClient();
            deleteTempCloudDirectoryTask =
                tempCloudDirectory.DeleteIfExistsAsync(cancellationToken: ct);
            await EnsureCurrentLogBlobAsync(ct);
            await deleteTempCloudDirectoryTask;
        }

        private AppendBlobClient GetAppendBlobClient()
        {
            var logFileName = GetLogFileName(_currentLogBlobIndex);

            return LoggingContainer.GetAppendBlobClient(
                $"{LoggingDirectory.Path}/{logFileName}");
        }
        #endregion

        #region Batch persistance
        public bool CanFitInBatch(IEnumerable<string> transactionTexts)
        {
            int totalLength = GetTotalLength(transactionTexts);

            return totalLength <= Math.Min(
                _currentLogBlob!.AppendBlobMaxAppendBlockBytes,
                LogPolicy.MaxBatchSizeInBytes);
        }

        public async Task PersistBatchAsync(
            IEnumerable<string> transactionTexts,
            CancellationToken ct)
        {
            try
            {
                await PersistBatchInternalAsync(transactionTexts, ct);
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "MaxBlobBlocksExceeded")
            {
                ++_currentLogBlobIndex;
                await EnsureCurrentLogBlobAsync(ct);
                await PersistBatchAsync(transactionTexts, ct);
            }
        }

        private async Task PersistBatchInternalAsync(
            IEnumerable<string> transactionTexts,
            CancellationToken ct)
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
                        var transactionText = transactionTexts.First();
                        var isFirst = true;

                        while (transactionText.Length > 0)
                        {
                            var text = transactionText.Substring(
                                0,
                                Math.Min(
                                    transactionText.Length,
                                    _currentLogBlob!.AppendBlobMaxAppendBlockBytes - SEPARATOR.Length));

                            stream.Position = 0;
                            stream.SetLength(0);
                            if (isFirst)
                            {
                                writer.Write(SEPARATOR);
                                isFirst = false;
                            }
                            writer.Write(text);
                            writer.Flush();
                            stream.Position = 0;
                            await _currentLogBlob!.AppendBlockAsync(stream, cancellationToken: ct);
                            transactionText = transactionText.Substring(text.Length);
                        }
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
                    await _currentLogBlob!.AppendBlockAsync(stream, cancellationToken: ct);
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