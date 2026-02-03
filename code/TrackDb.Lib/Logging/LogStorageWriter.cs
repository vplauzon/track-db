using Azure;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        #region Inner types
        private record CheckpointState(
            long CheckpointIndex,
            long LogBlobIndex,
            AppendBlobClient LogBlob,
            Task CreateBlobTask)
        {
            public CheckpointState(
                long CheckpointIndex,
                long LogBlobIndex,
                AppendBlobClient LogBlob,
                CancellationToken ct)
                : this(CheckpointIndex, LogBlobIndex, LogBlob, CreateIfNotExistsAsync(LogBlob, ct))
            {
            }

            private static async Task CreateIfNotExistsAsync(
                AppendBlobClient LogBlob,
                CancellationToken ct)
            {
                if (!await LogBlob.ExistsAsync(cancellationToken: ct))
                {
                    await LogBlob.CreateIfNotExistsAsync(cancellationToken: ct);
                }
            }
        }
        #endregion

        private CheckpointState _checkpointState;

        #region Constructor
        public static async Task<LogStorageWriter> CreateLogStorageWriterAsync(
            LogPolicy logPolicy,
            string localFolder,
            BlobClients blobClients,
            long? currentCheckpointIndex,
            long? currentLogBlobIndex,
            CancellationToken ct = default)
        {
            var logStorageWriter = new LogStorageWriter(
                logPolicy,
                localFolder,
                blobClients,
                currentCheckpointIndex ?? 1,
                currentLogBlobIndex ?? 1,
                ct);

            if (currentLogBlobIndex == null)
            {
                await logStorageWriter.CreateCheckpointAsync(false, Array.Empty<string>(), ct);
            }

            return logStorageWriter;
        }

        private LogStorageWriter(
            LogPolicy logPolicy,
            string localFolder,
            BlobClients blobClients,
            long currentCheckpointIndex,
            long currentLogBlobIndex,
            CancellationToken ct)
            : base(logPolicy, localFolder, blobClients)
        {
            _checkpointState = new CheckpointState(
                currentCheckpointIndex,
                currentLogBlobIndex,
                GetAppendBlobClient(currentLogBlobIndex),
                ct);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        #region Checkpoint
        public async Task CreateCheckpointAsync(
            bool createNewCheckpoint,
            IEnumerable<string> transactionTexts,
            CancellationToken ct)
        {
            if (createNewCheckpoint)
            {
                _checkpointState = new CheckpointState(
                    _checkpointState.CheckpointIndex + 1,
                    _checkpointState.CheckpointIndex + 1,
                    GetAppendBlobClient(_checkpointState.CheckpointIndex + 1),
                    ct);
            }

            var checkpointFileName = GetCheckpointFileName(_checkpointState.CheckpointIndex);
            var checkpointFilePath = Path.Combine(LocalFolder, checkpointFileName);
            var tempCloudDirectory = BlobClients.Directory.GetSubDirectoryClient("temp");
            var checkpointHeader = new CheckpointHeader(CURRENT_HEADER_VERSION);
            var checkpointHeaderText = checkpointHeader.ToJson();
            var deleteTempCloudDirectoryTask = DeleteIfExistsAsync(tempCloudDirectory, ct);

            Directory.CreateDirectory(LocalFolder);
            //  Write locally
            using (var stream = File.Create(checkpointFilePath))
            using (var writer = new StreamWriter(stream))
            {
                writer.Write(checkpointHeaderText);
                foreach (var tx in transactionTexts)
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
                BlobClients.Directory
                .GetFileClient(checkpointFileName).Path,
                cancellationToken: ct);
            deleteTempCloudDirectoryTask = DeleteIfExistsAsync(tempCloudDirectory, ct);
            await deleteTempCloudDirectoryTask;
        }

        private static async Task DeleteIfExistsAsync(
            DataLakeDirectoryClient directoryClient,
            CancellationToken ct)
        {
            if (await directoryClient.ExistsAsync(cancellationToken: ct))
            {
                await directoryClient.DeleteIfExistsAsync(cancellationToken: ct);
            }
        }

        private AppendBlobClient GetAppendBlobClient(long logBlobIndex)
        {
            var logFileName = GetLogFileName(logBlobIndex);

            return BlobClients.Container.GetAppendBlobClient(
                $"{BlobClients.Directory.Path}/{logFileName}");
        }
        #endregion

        #region Batch persistance
        public bool CanFitInBatch(IEnumerable<string> transactionTexts)
        {
            int totalLength = GetTotalLength(transactionTexts);

            return totalLength <= Math.Min(
                _checkpointState.LogBlob.AppendBlobMaxAppendBlockBytes,
                LogPolicy.MaxBatchSizeInBytes);
        }

        public async Task PersistBatchAsync(IEnumerable<string> transactionTexts, CancellationToken ct)
        {
            try
            {
                await PersistBatchInternalAsync(transactionTexts, ct);
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "MaxBlobBlocksExceeded")
            {   //  Increment log blob index
                var logBlob = GetAppendBlobClient(_checkpointState.LogBlobIndex + 1);

                _checkpointState = new CheckpointState(
                    _checkpointState.CheckpointIndex,
                    _checkpointState.LogBlobIndex + 1,
                    logBlob,
                    ct);
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

                await _checkpointState.CreateBlobTask;
                if (totalLength > _checkpointState.LogBlob.AppendBlobMaxAppendBlockBytes)
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
                                    _checkpointState.LogBlob.AppendBlobMaxAppendBlockBytes
                                    - SEPARATOR.Length));

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
                            await _checkpointState.LogBlob.AppendBlockAsync(
                                stream,
                                cancellationToken: ct);
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
                    await _checkpointState.LogBlob.AppendBlockAsync(stream, cancellationToken: ct);
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