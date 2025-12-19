using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.Policies;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.Logging
{
    /// <summary>
    /// Manages <see cref="TransactionLog">.
    /// Abstract buffering many transactions and schema-check.
    /// </summary>
    internal class LogTransactionManager : IAsyncDisposable
    {
        #region Inner types
        private record ContentItem(string Content, DateTime Timestamp, TaskCompletionSource? Tcs)
        {
            public ContentItem(string content)
                : this(content, DateTime.Now, null)
            {
            }

            public ContentItem(string content, TaskCompletionSource tcs)
                : this(content, DateTime.Now, tcs)
            {
            }
        }
        #endregion

        private readonly LogPolicy _logPolicy;
        private readonly IImmutableDictionary<string, TableSchema> _tableSchemaMap;
        private readonly TypedTable<TombstoneRecord> _tombstoneTable;
        private readonly LogStorageManager _logStorageManager;
        private readonly Task _backgroundProcessingTask;
        private readonly TaskCompletionSource _stopBackgroundProcessingSource =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Channel<ContentItem> _channel = Channel.CreateUnbounded<ContentItem>();

        public LogTransactionManager(
            LogPolicy logPolicy,
            string localFolder,
            IImmutableDictionary<string, TableSchema> tableSchemaMap,
			TypedTable<TombstoneRecord> tombstoneTable)
        {
            _logPolicy = logPolicy;
            _tableSchemaMap = tableSchemaMap;
            _tombstoneTable = tombstoneTable;
            _logStorageManager = new LogStorageManager(logPolicy, localFolder);
            _backgroundProcessingTask = ProcessContentItemsAsync();
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _stopBackgroundProcessingSource.TrySetResult();
            await _backgroundProcessingTask;
            await ((IAsyncDisposable)_logStorageManager).DisposeAsync();
        }

        #region Load
        public async IAsyncEnumerable<TransactionLog> LoadLogsAsync(
            [EnumeratorCancellation]
            CancellationToken ct)
        {
            var enumerator = _logStorageManager.LoadAsync(ct).GetAsyncEnumerator();

            //  Try read first element
            if (!await enumerator.MoveNextAsync())
            {   //  No items
                yield break;
            }
            else
            {
                var schemaContent = SchemaContent.FromJson(enumerator.Current);

                ValidateSchema(schemaContent);
                while (await enumerator.MoveNextAsync())
                {
                    var logContent = TransactionContent.FromJson(enumerator.Current);
                    var log = logContent.ToTransactionLog(_tombstoneTable, _tableSchemaMap);

                    yield return log;
                }
            }
        }

        private bool ValidateSchema(SchemaContent schemaContent)
        {
            //  Extra table persisted isn't permitted
            var extraTableName = schemaContent.Tables.Select(t => t.TableName)
                .Except(_tableSchemaMap.Keys)
                .FirstOrDefault();

            if (extraTableName != null)
            {
                throw new InvalidDataException(
                    $"Table '{extraTableName}' is present in checkpoint but ins't defined" +
                    $" in the database");
            }
            foreach (var tableContent in schemaContent.Tables)
            {
                var tableName = tableContent.TableName;
                var tableSchema = _tableSchemaMap[tableName];
                var extraColumnName = tableContent.Columns.Select(c => c.ColumnName)
                    .Except(tableSchema.Columns.Select(c => c.ColumnName))
                    .FirstOrDefault();
                var missingColumnName = tableSchema.Columns.Select(c => c.ColumnName)
                    .Except(tableContent.Columns.Select(c => c.ColumnName))
                    .Where(columnName => tableSchema.Columns.Where(c => c.ColumnName == columnName).Any())
                    .FirstOrDefault();

                if (extraColumnName != null)
                {
                    throw new InvalidDataException(
                        $"Column '{tableName}'.'{extraColumnName}' is present in checkpoint " +
                        $"but ins't defined in the database");
                }
                if (missingColumnName != null)
                {
                    throw new InvalidDataException(
                        $"Column '{tableName}'.'{missingColumnName}' is defined in the database " +
                        $"but ins't present in checkpoint");
                }
                foreach (var checkpointColumn in tableContent.Columns)
                {
                    var columnName = checkpointColumn.ColumnName;
                    var columnType = checkpointColumn.ColumnType;
                    var columnSchema = tableSchema.Columns
                        .Where(c => c.ColumnName == columnName)
                        .First();

                    if (columnType != columnSchema.ColumnType.Name)
                    {
                        throw new InvalidDataException(
                            $"Column '{tableName}'.'{columnName}' is defined as " +
                            $"'{columnSchema.ColumnType.Name}' in the database " +
                            $"but is '{columnType}' in checkpoint");
                    }
                }
            }

            //  Missing table in checkpoint is permitted, but requires new checkpoint
            return _tableSchemaMap.Keys
                .Except(schemaContent.Tables.Select(t => t.TableName))
                .Any();
        }
        #endregion

        #region Checkpoint
        public async Task CreateCheckpointAsync(
            IAsyncEnumerable<TransactionLog> transactions,
            CancellationToken ct)
        {
            await _logStorageManager.CreateCheckpointAsync(
                ToTransactionText(transactions, ct),
                ct);
        }

        private async IAsyncEnumerable<string> ToTransactionText(
            IAsyncEnumerable<TransactionLog> transactions,
            [EnumeratorCancellation]
            CancellationToken ct)
        {
            var schemaContent = SchemaContent.FromSchemas(_tableSchemaMap.Values);

            yield return schemaContent.ToJson();
            await foreach (var tx in transactions.WithCancellation(ct))
            {
                var txContent = TransactionContent.FromTransactionLog(
                    tx,
                    _tombstoneTable.Schema,
                    _tableSchemaMap);

                if (txContent != null)
                {
                    yield return txContent.ToJson();
                }
            }
        }
        #endregion

        #region Push transaction
        public void QueueContent(TransactionLog transactionLog)
        {
            var content = TransactionContent.FromTransactionLog(
                transactionLog,
                _tombstoneTable.Schema,
                _tableSchemaMap);

            if (content != null)
            {
                var item = new ContentItem(content.ToJson());

                if (!_channel.Writer.TryWrite(item))
                {
                    throw new InvalidOperationException("Couldn't write content");
                }
            }
        }

        public async Task CommitContentAsync(TransactionLog transactionLog)
        {
            var content = TransactionContent.FromTransactionLog(
                transactionLog,
                _tombstoneTable.Schema,
                _tableSchemaMap);

            if (content != null)
            {
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var item = new ContentItem(content.ToJson(), tcs);

                if (!_channel.Writer.TryWrite(item))
                {
                    throw new InvalidOperationException("Couldn't write content");
                }

                await tcs.Task;
            }
        }
        #endregion

        #region Content items processing
        private async Task ProcessContentItemsAsync()
        {
            var queue = new Queue<ContentItem>();

            do
            {
                //  Process items
                if (queue.Any()
                    && (_stopBackgroundProcessingSource.Task.IsCompleted
                    || IsBufferingTimeOver(queue.Peek())
                    || IsBlockComplete(queue)))
                {
                    await PersistBlockAsync(queue);
                }
                if (!DrainChannel(queue))
                {
                    var itemTask = _channel.Reader.WaitToReadAsync().AsTask();

                    if (queue.Any())
                    {
                        var delay = queue.Peek().Timestamp.Add(_logPolicy.BufferingTimeWindow)
                            - DateTime.Now;
                        var delayTask = Task.Delay(delay > TimeSpan.Zero ? delay : TimeSpan.Zero);

                        await Task.WhenAny(
                            itemTask,
                            _stopBackgroundProcessingSource.Task,
                            delayTask);
                    }
                    else
                    {
                        await Task.WhenAny(itemTask, _stopBackgroundProcessingSource.Task);
                    }
                    DrainChannel(queue);
                }
            }
            while (!_stopBackgroundProcessingSource.Task.IsCompleted || queue.Any());
        }

        private async Task PersistBlockAsync(Queue<ContentItem> queue)
        {
            var tcsList = new List<TaskCompletionSource>(queue.Count);
            var transactionTextList = new List<string>();

            while (queue.Any())
            {
                if (queue.TryPeek(out var item))
                {
                    var canFit = _logStorageManager.CanFitInBatch(
                        transactionTextList.Append(item.Content));

                    if (canFit || !transactionTextList.Any())
                    {   //  Cumulate
                        transactionTextList.Add(item.Content);
                        if (item.Tcs != null)
                        {
                            tcsList.Add(item.Tcs);
                        }
                        //  Actually dequeue the peeked item
                        queue.Dequeue();
                    }
                    if (!canFit || !queue.Any())
                    {
                        await _logStorageManager.PersistBatchAsync(transactionTextList);
                        //  Confirm persistance
                        foreach (var tcs in tcsList)
                        {
                            tcs.TrySetResult();
                        }

                        return;
                    }
                }
            }
        }

        private bool IsBlockComplete(IEnumerable<ContentItem> items)
        {
            return !_logStorageManager.CanFitInBatch(items.Select(i => i.Content));
        }

        private bool IsBufferingTimeOver(ContentItem contentItem)
        {
            var triggerTime = contentItem.Timestamp.Add(_logPolicy.BufferingTimeWindow);
            var now = DateTime.Now;
            var isTrigger = triggerTime <= now;
            
            return isTrigger;
        }

        private bool DrainChannel(Queue<ContentItem> queue)
        {
            bool hasOne = false;

            while (_channel.Reader.TryRead(out var item))
            {
                queue.Enqueue(item);
                hasOne = true;
            }

            return hasOne;
        }
        #endregion
    }
}