using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.Policies;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.Logging
{
    /// <summary>
    /// Reads transactions from persisted storage.  Relies on
    /// <see cref="LogStorageReader"/>.
    /// </summary>
    internal class LogTransactionReader : IAsyncDisposable
    {
        private readonly LogStorageReader _logStorageReader;
        private readonly IImmutableDictionary<string, TableSchema> _tableSchemaMap;
        private readonly TypedTable<TombstoneRecord> _tombstoneTable;

        #region Constructor
        public static async Task<LogTransactionReader> CreateAsync(
            LogPolicy logPolicy,
            string localFolder,
            BlobClients blobClients,
            IEnumerable<TableSchema> tableSchemas,
            TypedTable<TombstoneRecord> tombstoneTable,
            CancellationToken ct = default)
        {
            var logStorageReader = await LogStorageReader.CreateAsync(
                logPolicy,
                blobClients,
                localFolder,
                ct);
            var tableSchemaMap = tableSchemas.ToImmutableDictionary(s => s.TableName);

            return new LogTransactionReader(logStorageReader, tableSchemaMap, tombstoneTable);
        }

        private LogTransactionReader(
            LogStorageReader logStorageReader,
            IImmutableDictionary<string, TableSchema> tableSchemaMap,
            TypedTable<TombstoneRecord> tombstoneTable)
        {
            _logStorageReader = logStorageReader;
            _tableSchemaMap = tableSchemaMap;
            _tombstoneTable = tombstoneTable;
        }
        #endregion

        public Version StorageVersion => _logStorageReader.StorageVersion;

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)_logStorageReader).DisposeAsync();
        }

        public async IAsyncEnumerable<TransactionLog> LoadTransactionsAsync(
            [EnumeratorCancellation]
            CancellationToken ct)
        {
            await foreach (var text in _logStorageReader.LoadTransactionTextsAsync(ct))
            {
                TransactionLog? log = null;
                try
                {
                    var logContent = TransactionContent.FromJson(text);

                    log = logContent.ToTransactionLog(_tombstoneTable, _tableSchemaMap);
                }
                catch (JsonException)
                {   //  This happens when a transaction got split in two blob blocks
                    //  and the second one didn't get persisted
                    //  because the process crashed / terminated
                }
                if (log != null)
                {
                    yield return log;
                }
            }
        }

        public async Task<LogTransactionWriter> CreateLogTransactionWriterAsync(
            CancellationToken ct)
        {
            var logStorageWriter = await _logStorageReader.CreateLogStorageManagerAsync(ct);

            return new LogTransactionWriter(logStorageWriter, _tableSchemaMap, _tombstoneTable);
        }
    }
}