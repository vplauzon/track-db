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
                AppendBlobClient LogBlob)
                : this(CheckpointIndex, LogBlobIndex, LogBlob, CreateIfNotExistsAsync(LogBlob))
            {
            }

            private static async Task CreateIfNotExistsAsync(AppendBlobClient LogBlob)
            {
                await Handle409Policy.ExecuteAsync(
                    async () => await LogBlob.CreateIfNotExistsAsync());
            }
        }
        #endregion

        private readonly CheckpointState[] _checkpointStates;

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
                currentLogBlobIndex ?? 1);
            var checkpointState = logStorageWriter.GetCheckpointState(currentCheckpointIndex ?? 1);

            if (currentLogBlobIndex == null)
            {
                await logStorageWriter.CreateCheckpointAsync(
                    1,
                    Array.Empty<string>().ToAsyncEnumerable(),
                    ct);
            }

            return logStorageWriter;
        }

        private LogStorageWriter(
            LogPolicy logPolicy,
            string localFolder,
            BlobClients blobClients,
            long currentCheckpointIndex,
            long currentLogBlobIndex)
            : base(logPolicy, localFolder, blobClients)
        {
            var currentState = new CheckpointState(
                currentCheckpointIndex,
                currentLogBlobIndex,
                GetAppendBlobClient(currentLogBlobIndex));
            var dummyState = new CheckpointState(0, 0, GetAppendBlobClient(0), Task.CompletedTask);

            _checkpointStates = [currentState, dummyState];
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        public long LastCheckpointIndex =>
            Math.Max(_checkpointStates[0].CheckpointIndex, _checkpointStates[1].CheckpointIndex);

        #region Checkpoint
        public async Task CreateCheckpointAsync(
            long checkpointIndex,
            IAsyncEnumerable<string> transactionTexts,
            CancellationToken ct)
        {
            var state = GetCheckpointState(checkpointIndex);
            var checkpointFileName = GetCheckpointFileName(state.CheckpointIndex);
            var checkpointFilePath = Path.Combine(LocalFolder, checkpointFileName);
            var tempCloudDirectory = BlobClients.Directory.GetSubDirectoryClient("temp");
            var checkpointHeader = new CheckpointHeader(CURRENT_HEADER_VERSION);
            var checkpointHeaderText = checkpointHeader.ToJson();
            var deleteTempCloudDirectoryTask =
                DeleteIfExistsAsync(tempCloudDirectory, ct);

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
                BlobClients.Directory
                .GetFileClient(checkpointFileName).Path,
                cancellationToken: ct);
            deleteTempCloudDirectoryTask =
                tempCloudDirectory.DeleteIfExistsAsync(cancellationToken: ct);
            await deleteTempCloudDirectoryTask;
        }

        private static async Task DeleteIfExistsAsync(
            DataLakeDirectoryClient tempCloudDirectory,
            CancellationToken ct)
        {
            await Handle409Policy.ExecuteAsync(
                async () => await tempCloudDirectory.DeleteIfExistsAsync(cancellationToken: ct));
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
                _checkpointStates[0].LogBlob.AppendBlobMaxAppendBlockBytes,
                LogPolicy.MaxBatchSizeInBytes);
        }

        public async Task PersistBatchAsync(
            long checkpointIndex,
            IEnumerable<string> transactionTexts,
            CancellationToken ct)
        {
            var state = GetCheckpointState(checkpointIndex);

            try
            {
                await PersistBatchInternalAsync(state, transactionTexts, ct);
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "MaxBlobBlocksExceeded")
            {   //  Increment log blob index
                var logBlob = GetAppendBlobClient(state.LogBlobIndex + 1);
                var newState = new CheckpointState(
                    state.CheckpointIndex,
                    state.LogBlobIndex + 1,
                    logBlob);

                if (_checkpointStates[0].CheckpointIndex == checkpointIndex)
                {
                    _checkpointStates[0] = newState;
                }
                else
                {
                    _checkpointStates[1] = newState;
                }
                await PersistBatchAsync(checkpointIndex, transactionTexts, ct);
            }
        }

        private async Task PersistBatchInternalAsync(
            CheckpointState checkpointState,
            IEnumerable<string> transactionTexts,
            CancellationToken ct)
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            {
                var totalLength = GetTotalLength(transactionTexts);

                await checkpointState.CreateBlobTask;
                if (totalLength > checkpointState.LogBlob.AppendBlobMaxAppendBlockBytes)
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
                                    checkpointState.LogBlob.AppendBlobMaxAppendBlockBytes
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
                            await checkpointState.LogBlob.AppendBlockAsync(
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
                    await checkpointState.LogBlob.AppendBlockAsync(stream, cancellationToken: ct);
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

        private CheckpointState GetCheckpointState(long checkpointIndex)
        {
            var state1 = _checkpointStates[0];
            var state2 = _checkpointStates[1];

            if (state1.CheckpointIndex == checkpointIndex)
            {
                return state1;
            }
            else if (state2.CheckpointIndex == checkpointIndex)
            {
                return state2;
            }
            else if (checkpointIndex > state1.CheckpointIndex
                && checkpointIndex > state2.CheckpointIndex)
            {
                var state = new CheckpointState(
                    checkpointIndex,
                    checkpointIndex,
                    GetAppendBlobClient(checkpointIndex));

                if (state1.CheckpointIndex < state2.CheckpointIndex)
                {
                    _checkpointStates[0] = state;
                }
                else
                {
                    _checkpointStates[1] = state;
                }

                return state;
            }
            else
            {
                throw new InvalidOperationException(
                    $"Checkpoint {checkpointIndex} is a regression");
            }
        }
    }
}