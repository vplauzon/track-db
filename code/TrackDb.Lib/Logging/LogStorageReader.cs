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
        /// <param name="blobClients"></param>
        /// <param name="localFolder"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public static async Task<LogStorageReader> CreateAsync(
            LogPolicy logPolicy,
            BlobClients blobClients,
            string localFolder,
            CancellationToken ct)
        {
            if (logPolicy.StorageConfiguration == null)
            {
                throw new ArgumentNullException(nameof(logPolicy.StorageConfiguration));
            }

            var localReadFolder = Path.Combine(localFolder, "read");
            var lastCheckpointIndex =
                await CopyCheckpointAsync(blobClients.Directory, localReadFolder, ct);

            if (lastCheckpointIndex != null)
            {
                var lastLogFileIndex = await CopyLogFilesAsync(
                    blobClients.Directory,
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
                    blobClients,
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
                    blobClients,
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
            async Task<DataLakeFileClient?> GetLastCheckpointAsync(
                DataLakeDirectoryClient loggingDirectory,
                CancellationToken ct)
            {
                var checkpointPathList = await loggingDirectory.GetPathsAsync(cancellationToken: ct)
                    .ToImmutableListAsync();
                var lastCheckpoint = checkpointPathList
                    .Where(i => i.IsDirectory == false)
                    .Select(i => loggingDirectory.GetParentFileSystemClient().GetFileClient(i.Name))
                    .Where(f => f.Name.EndsWith("-checkpoint.json"))
                    .OrderBy(f => f.Name)
                    .LastOrDefault();

                return lastCheckpoint;
            }

            var lastCheckpoint = await GetLastCheckpointAsync(loggingDirectory, ct);

            if (lastCheckpoint != null)
            {
                var lastCheckpointIndex = long.Parse(lastCheckpoint.Name.Split('-')[0]);
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
                .Where(f => f.Name.EndsWith("-log.json"))
                .Select(f => new
                {
                    Client = f,
                    Index = long.Parse(f.Name.Split('-')[0])
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
                    .Select(f => long.Parse(f.Name.Split('-')[0]))
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
            BlobClients blobClients,
            long? checkpointIndex,
            long? lastLogFileIndex,
            Version storeageVersion)
            : base(logPolicy, localFolder, blobClients)
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

                        if (Path.Exists(logPath))
                        {   //  There can be gaps in log blobs
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
        }

        public async Task<LogStorageWriter> CreateLogStorageManagerAsync(CancellationToken ct)
        {
            return await LogStorageWriter.CreateLogStorageWriterAsync(
                LogPolicy,
                LocalFolder,
                BlobClients,
                _checkpointIndex,
                _lastLogFileIndex,
                ct);
        }
    }
}