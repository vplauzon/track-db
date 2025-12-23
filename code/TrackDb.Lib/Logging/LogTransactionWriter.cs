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
    internal class LogTransactionWriter : IAsyncDisposable
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

        private readonly LogStorageWriter _logStorageWriter;
        private readonly IImmutableDictionary<string, TableSchema> _tableSchemaMap;
        private readonly TypedTable<TombstoneRecord> _tombstoneTable;
        private readonly Task _backgroundProcessingTask;
        private readonly TaskCompletionSource _stopBackgroundProcessingSource =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Channel<ContentItem> _channel = Channel.CreateUnbounded<ContentItem>();

        public LogTransactionWriter(
            LogStorageWriter logStorageWriter,
            IImmutableDictionary<string, TableSchema> tableSchemaMap,
            TypedTable<TombstoneRecord> tombstoneTable)
        {
            _logStorageWriter = logStorageWriter;
            _tableSchemaMap = tableSchemaMap;
            _tombstoneTable = tombstoneTable;
            _backgroundProcessingTask = ProcessContentItemsAsync();
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)_logStorageWriter).DisposeAsync();
            _stopBackgroundProcessingSource.TrySetResult();
            await _backgroundProcessingTask;
        }

        #region Checkpoint
        public async Task CreateCheckpointAsync(
            IAsyncEnumerable<TransactionLog> transactions,
            CancellationToken ct)
        {
            await _logStorageWriter.CreateCheckpointAsync(
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
                        var delay = queue.Peek().Timestamp.Add(
                            _logStorageWriter.LogPolicy.BufferingTimeWindow)
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
                    var canFit = _logStorageWriter.CanFitInBatch(
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
                        await _logStorageWriter.PersistBatchAsync(transactionTextList);
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
            return !_logStorageWriter.CanFitInBatch(items.Select(i => i.Content));
        }

        private bool IsBufferingTimeOver(ContentItem contentItem)
        {
            var triggerTime = contentItem.Timestamp.Add(
                _logStorageWriter.LogPolicy.BufferingTimeWindow);
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