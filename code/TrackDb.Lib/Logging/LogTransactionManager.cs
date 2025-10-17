using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.Marshalling;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Policies;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.Logging
{
    /// <summary>
    /// Manages log transactions, i.e. string-serialized transactions.
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
        private readonly TypedTableSchema<TombstoneRecord> _tombstoneSchema;
        private readonly LogStorageManager _logStorageManager;
        private readonly Task _backgroundProcessingTask;
        private readonly TaskCompletionSource _stopBackgroundProcessingSource =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Channel<ContentItem> _channel = Channel.CreateUnbounded<ContentItem>();

        public LogTransactionManager(
            LogPolicy logPolicy,
            string localFolder,
            IImmutableDictionary<string, TableSchema> tableSchemaMap,
            TypedTableSchema<TombstoneRecord> tombstoneSchema)
        {
            _logPolicy = logPolicy;
            _tableSchemaMap = tableSchemaMap;
            _tombstoneSchema = tombstoneSchema;
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
        public async Task<LogTransactionLoadOutput> LoadLogsAsync(CancellationToken ct)
        {
            var logStorageLoadOutput = await _logStorageManager.LoadLogsAsync(ct);

            await using (var enumerator = logStorageLoadOutput.TransactionTexts.GetAsyncEnumerator())
            {
                //  Try read first element
                if (!await enumerator.MoveNextAsync())
                {   //  No items
                    return new LogTransactionLoadOutput(
                        true,
                        AsyncEnumerable.Empty<TransactionLog>());
                }
                else
                {
                    var isSchemaValid = ValidateSchema(enumerator.Current);

                    return new LogTransactionLoadOutput(
                        logStorageLoadOutput.IsCheckpointRequired
                        && isSchemaValid,
                        ParseTransactions(enumerator));
                }
            }
        }

        private IAsyncEnumerable<TransactionLog> ParseTransactions(IAsyncEnumerator<string> enumerator)
        {
            throw new NotImplementedException();
        }

        private bool ValidateSchema(string schemaText)
        {
            throw new NotImplementedException();
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
            await foreach (var tx in transactions.WithCancellation(ct))
            {
                var text = ToTransactionText(tx);

                if (text != null)
                {
                    yield return text;
                }
            }
        }
        #endregion

        #region Push transaction
        public void QueueContent(TransactionLog transactionLog)
        {
            var contentText = ToTransactionText(transactionLog);

            if (contentText != null)
            {
                if (!_channel.Writer.TryWrite(new ContentItem(contentText)))
                {
                    throw new InvalidOperationException("Couldn't write content");
                }
            }
        }

        public async Task CommitContentAsync(TransactionLog transactionLog)
        {
            var contentText = ToTransactionText(transactionLog);

            if (contentText != null)
            {
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var item = new ContentItem(contentText, tcs);

                if (!_channel.Writer.TryWrite(item))
                {
                    throw new InvalidOperationException("Couldn't write content");
                }

                await tcs.Task;
            }
        }

        private string? ToTransactionText(TransactionLog transactionLog)
        {
            IImmutableDictionary<string, List<long>> ToTombstones(IBlock block)
            {
                var pf = new QueryPredicateFactory<TombstoneRecord>(_tombstoneSchema);
                var isUserTablePredicate = pf
                    .In(t => t.TableName, _tableSchemaMap.Keys)
                    .QueryPredicate;
                var rowIndexes = block.Filter(isUserTablePredicate, false).RowIndexes;
                var columnIndexes = _tombstoneSchema.GetColumnIndexSubset(t => t.TableName)
                    .Concat(_tombstoneSchema.GetColumnIndexSubset(t => t.RecordId));
                var buffer = new object[2];
                var content = block.Project(buffer, columnIndexes, rowIndexes, 0)
                    .Select(data => new
                    {
                        Table = (string)data.Span[0]!,
                        RecordId = (long)data.Span[1]!
                    })
                    .GroupBy(o => o.Table)
                    .ToImmutableDictionary(g => g.Key, g => g.Select(i => i.RecordId).ToList());

                return content;
            }

            var userTables = transactionLog.TableBlockBuilderMap
                .Where(p => _tableSchemaMap.ContainsKey(p.Key))
                .Select(p => KeyValuePair.Create(p.Key, p.Value.ToLog()))
                .ToImmutableDictionary();
            var tombstoneRecordMap = transactionLog.TableBlockBuilderMap.ContainsKey(
                _tombstoneSchema.TableName)
                ? ToTombstones(
                    transactionLog.TableBlockBuilderMap[_tombstoneSchema.TableName])
                : ImmutableDictionary<string, List<long>>.Empty;

            if (userTables.Any() || tombstoneRecordMap.Any())
            {
                var content = new TransactionContent(userTables, tombstoneRecordMap);
                var contentText = JsonSerializer.Serialize(content);

                return contentText;
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Content items processing
        private async Task ProcessContentItemsAsync()
        {
            var queue = new Queue<ContentItem>();

            //  We must drain the queue even if a stop has been called
            while (!_stopBackgroundProcessingSource.Task.IsCompleted || queue.Any())
            {
                //  Buffer items
                while (!queue.Any()
                    || (!IsBlockComplete(queue)
                    && IsBufferingTimeOver(queue.Peek())))
                {
                    if (!DrainChannel(queue))
                    {
                        var itemTask = _channel.Reader.ReadAsync().AsTask();

                        await Task.WhenAny(itemTask, _stopBackgroundProcessingSource.Task);
                        if (itemTask.IsCompleted)
                        {
                            queue.Enqueue(itemTask.Result);
                        }
                    }
                    else if (_stopBackgroundProcessingSource.Task.IsCompleted)
                    {
                        break;
                    }
                }
                //  Process items
                while (queue.Any()
                    && (_stopBackgroundProcessingSource.Task.IsCompleted
                    || IsBufferingTimeOver(queue.Peek())
                    || IsBlockComplete(queue)))
                {
                    await PersistBlockAsync(queue);
                }
            }
        }

        private async Task PersistBlockAsync(Queue<ContentItem> queue)
        {
            var tcsList = new List<TaskCompletionSource>(queue.Count);
            var transactionTextList = new List<string>();

            while (queue.Any())
            {
                var item = queue.Dequeue();
                var canFit =
                    _logStorageManager.CanFitInBatch(transactionTextList.Append(item.Content));

                if (canFit || !transactionTextList.Any())
                {   //  Cumulate
                    transactionTextList.Add(item.Content);
                    if (item.Tcs != null)
                    {
                        tcsList.Add(item.Tcs);
                    }
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

        private bool IsBlockComplete(IEnumerable<ContentItem> items)
        {
            return !_logStorageManager.CanFitInBatch(items.Select(i => i.Content));
        }

        private bool IsBufferingTimeOver(ContentItem contentItem)
        {
            return contentItem.Timestamp.Add(_logPolicy.BufferingTimeWindow) > DateTime.Now;
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