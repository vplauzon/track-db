using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Specialized;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.Logging
{
    internal class LogStorageReader : LogStorageBase, IAsyncDisposable
    {
        private readonly string _localReadFolder;
        private readonly long? _checkpointIndex;
        private readonly long? _lastLogFileIndex;

        #region Constructor
        /// <summary>
        /// Loads all relevant blobs to a local folder.
        /// Actual data can be read with
        /// <see cref="LoadTransactionTextsAsync(CancellationToken)"/>
        /// </summary>
        /// <param name="logPolicy"></param>
        /// <param name="localFolder"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public static async Task<LogStorageReader> CreateAsync(
            LogPolicy logPolicy,
            string localFolder,
            CancellationToken ct = default)
        {
            if (logPolicy.StorageConfiguration == null)
            {
                throw new ArgumentNullException(nameof(logPolicy.StorageConfiguration));
            }

            if (logPolicy.StorageConfiguration.TokenCredential != null)
            {
                var dummyBlob = new AppendBlobClient(
                    logPolicy.StorageConfiguration.LogFolderUri,
                    logPolicy.StorageConfiguration.TokenCredential);
                var loggingDirectory = new DataLakeDirectoryClient(
                    logPolicy.StorageConfiguration.LogFolderUri,
                    logPolicy.StorageConfiguration.TokenCredential);
                var loggingContainer = dummyBlob.GetParentBlobContainerClient();

                return await CreateAsync(
                    logPolicy, loggingDirectory, loggingContainer, localFolder, ct);
            }
            else
            {
                var dummyBlob = new AppendBlobClient(
                    logPolicy.StorageConfiguration.LogFolderUri,
                    logPolicy.StorageConfiguration.KeyCredential);
                var loggingDirectory = new DataLakeDirectoryClient(
                    logPolicy.StorageConfiguration.LogFolderUri,
                    logPolicy.StorageConfiguration.KeyCredential);
                var loggingContainer = dummyBlob.GetParentBlobContainerClient();

                return await CreateAsync(
                    logPolicy, loggingDirectory, loggingContainer, localFolder, ct);
            }
        }

        private static async Task<LogStorageReader> CreateAsync(
            LogPolicy logPolicy,
            DataLakeDirectoryClient loggingDirectory,
            BlobContainerClient loggingContainer,
            string localFolder,
            CancellationToken ct)
        {
            var localReadFolder = Path.Combine(localFolder, "read");
            var lastCheckpointIndex =
                await CopyCheckpointAsync(loggingDirectory, localReadFolder, ct);

            if (lastCheckpointIndex != null)
            {
                var lastLogFileIndex = await CopyLogFilesAsync(
                    loggingDirectory,
                    localReadFolder,
                    lastCheckpointIndex.Value,
                    ct);
                var checkpointPath = Path.Combine(
                    localReadFolder,
                    GetCheckpointFileName(lastCheckpointIndex.Value));
                var checkpointFirstLine = File.ReadLines(checkpointPath).FirstOrDefault();
                var checkpointHeader = checkpointFirstLine != null
                    ? CheckpointHeader.FromJson(checkpointFirstLine)
                    : null;
                var version = checkpointHeader != null
                    ? checkpointHeader.Version
                    : CURRENT_HEADER_VERSION;

                return new LogStorageReader(
                    logPolicy,
                    localFolder,
                    localReadFolder,
                    loggingDirectory,
                    loggingContainer,
                    lastCheckpointIndex,
                    lastLogFileIndex,
                    version);
            }
            else
            {
                return new LogStorageReader(
                    logPolicy,
                    localFolder,
                    localReadFolder,
                    loggingDirectory,
                    loggingContainer,
                    lastCheckpointIndex,
                    null,
                    CURRENT_HEADER_VERSION);
            }
        }

        private static async Task<long?> CopyCheckpointAsync(
            DataLakeDirectoryClient loggingDirectory,
            string localReadFolder,
            CancellationToken ct)
        {
            async Task<DataLakeDirectoryClient> EnsureCheckpointBlobFolderAsync(
                DataLakeDirectoryClient loggingDirectory,
                CancellationToken ct)
            {
                var checkpointDirectory = loggingDirectory.GetSubDirectoryClient(CHECKPOINT_BLOB_FOLDER);

                await checkpointDirectory.CreateIfNotExistsAsync(cancellationToken: ct);

                return checkpointDirectory;
            }

            async Task<DataLakeFileClient?> GetLastCheckpointAsync(
                DataLakeDirectoryClient loggingDirectory,
                DataLakeDirectoryClient checkpointDirectory,
                CancellationToken ct)
            {
                var checkpointPathList = await checkpointDirectory.GetPathsAsync(cancellationToken: ct)
                    .ToImmutableListAsync();
                var lastCheckpoint = checkpointPathList
                    .Where(i => i.IsDirectory == false)
                    .Select(i => loggingDirectory.GetParentFileSystemClient().GetFileClient(i.Name))
                    .Where(f => f.Name.EndsWith(".json"))
                    .Where(f => f.Name.StartsWith("checkpoint-"))
                    .OrderBy(f => f.Name)
                    .LastOrDefault();

                return lastCheckpoint;
            }

            var checkpointDirectory = await EnsureCheckpointBlobFolderAsync(loggingDirectory, ct);
            var lastCheckpoint = await GetLastCheckpointAsync(loggingDirectory, checkpointDirectory, ct);

            if (lastCheckpoint != null)
            {
                var lastCheckpointIndex = long.Parse(
                    lastCheckpoint.Name.Split('.')[0].Split('-')[1]);
                var checkpointLocalPath = Path.Combine(localReadFolder, lastCheckpoint.Name);

                Directory.CreateDirectory(localReadFolder);
                await lastCheckpoint.ReadToAsync(checkpointLocalPath, cancellationToken: ct);

                return lastCheckpointIndex;
            }
            else
            {
                return null;
            }
        }

        private static async Task<long?> CopyLogFilesAsync(
            DataLakeDirectoryClient loggingDirectory,
            string localReadFolder,
            long lastCheckpointIndex,
            CancellationToken ct)
        {
            var allLogPathsList = await loggingDirectory.GetPathsAsync(cancellationToken: ct)
                .ToImmutableListAsync();
            var logPathList = allLogPathsList
                .Where(i => i.IsDirectory == false)
                .Select(i => loggingDirectory.GetParentFileSystemClient().GetFileClient(i.Name))
                .Where(f => f.Name.EndsWith(".json"))
                .Where(f => f.Name.StartsWith("log-"))
                .Select(f => new
                {
                    Client = f,
                    Index = long.Parse(f.Name.Split('.')[0].Split('-')[1])
                })
                .Where(o => o.Index >= lastCheckpointIndex)
                .Select(o => o.Client)
                .OrderBy(f => f.Name)
                .ToImmutableArray();
            var copyTasks = logPathList
                .Select(f => f.ReadToAsync(
                    Path.Combine(localReadFolder, f.Name),
                    cancellationToken: ct));

            await Task.WhenAll(copyTasks);

            if (logPathList.Any())
            {
                return logPathList
                    .Select(f => long.Parse(f.Name.Split('.')[0].Split('-')[1]))
                    .Max();
            }
            else
            {
                return null;
            }
        }

        private LogStorageReader(
            LogPolicy logPolicy,
            string localFolder,
            string localReadFolder,
            DataLakeDirectoryClient loggingDirectory,
            BlobContainerClient loggingContainer,
            long? checkpointIndex,
            long? lastLogFileIndex,
            Version storeageVersion)
            : base(logPolicy, localFolder, loggingDirectory, loggingContainer)
        {
            _localReadFolder = localReadFolder;
            _checkpointIndex = checkpointIndex;
            _lastLogFileIndex = lastLogFileIndex;
            StorageVersion = storeageVersion;
        }
        #endregion

        public Version StorageVersion { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            if (Directory.Exists(_localReadFolder))
            {
                Directory.Delete(_localReadFolder);
            }

            await ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<string> LoadTransactionTextsAsync(
            [EnumeratorCancellation]
            CancellationToken ct)
        {
            if (_checkpointIndex != null)
            {
                var checkpointPath = Path.Combine(
                    _localReadFolder,
                    GetCheckpointFileName(_checkpointIndex.Value));
                //  We skip the first line which is the version payload
                var checkpointLines = File.ReadLines(checkpointPath).Skip(1);

                foreach (var line in checkpointLines)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return line;
                }
                if (_lastLogFileIndex != null)
                {
                    for (var i = _checkpointIndex.Value; i <= _lastLogFileIndex.Value; ++i)
                    {
                        var logPath = Path.Combine(_localReadFolder, GetLogFileName(i));
                        var logLines = File.ReadLines(logPath).Skip(1);

                        foreach (var line in logLines)
                        {
                            ct.ThrowIfCancellationRequested();
                            yield return line;
                        }
                    }
                }
            }
        }

        public async Task<LogStorageWriter> CreateLogStorageManagerAsync(CancellationToken ct)
        {
            return await LogStorageWriter.CreateLogStorageWriterAsync(
                LogPolicy,
                LocalFolder,
                LoggingDirectory,
                LoggingContainer,
                _lastLogFileIndex,
                ct);
        }
    }
}