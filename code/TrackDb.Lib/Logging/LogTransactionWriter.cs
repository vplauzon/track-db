using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.Logging
{
    /// <summary>
    /// Manages <see cref="TransactionLog">.
    /// Abstract buffering many transactions.
    /// </summary>
    internal class LogTransactionWriter : IAsyncDisposable
    {
        #region Inner types
        private record ContentItem(
            string Content,
            DateTime Timestamp,
            TaskCompletionSource? Tcs)
        {
            public ContentItem(string content, TaskCompletionSource? tcs = null)
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

        public void ObserveBackgroundTask()
        {
            if (_backgroundProcessingTask.IsCompleted)
            {
                _backgroundProcessingTask.Wait();
            }
        }

        #region Checkpoint
        private async Task CreateCheckpointAsync(
            IEnumerable<TransactionLog> transactions,
            CancellationToken ct)
        {
            await _logStorageWriter.CreateCheckpointAsync(
                true,
                ToTransactionText(transactions),
                ct);
        }

        private IEnumerable<string> ToTransactionText(IEnumerable<TransactionLog> transactions)
        {
            foreach (var tx in transactions)
            {
                if (_stopBackgroundProcessingSource.Task.IsCompletedSuccessfully)
                {
                    throw new OperationCanceledException();
                }
                var txContent = TransactionContent.FromTransactionLog(
                    tx,
                    _tombstoneTable.Schema,
                    _tableSchemaMap);

                if (txContent != null)
                {
                    var json = txContent.ToJson();

                    if (!string.IsNullOrWhiteSpace(json))
                    {
                        yield return json;
                    }
                }
            }
        }
        #endregion

        #region Push transaction
        public async Task QueueTransactionLogItemAsync(
            TransactionLogItem transactionLogItem,
            CancellationToken ct)
        {
            ObserveBackgroundTask();
            if (transactionLogItem.TransactionLog != null)
            {
                var content = TransactionContent.FromTransactionLog(
                    transactionLogItem.TransactionLog,
                    _tombstoneTable.Schema,
                    _tableSchemaMap);
                var contentItem = new ContentItem(content.ToJson(), transactionLogItem.Tcs);

                if (!_channel.Writer.TryWrite(contentItem))
                {
                    throw new InvalidOperationException("Couldn't write content");
                }
            }
            else if (transactionLogItem.TransactionLogsFunc != null)
            {   //  First flush the queue
                var tcs = new TaskCompletionSource();
                var waitItem = new ContentItem(string.Empty, tcs);

                if (!_channel.Writer.TryWrite(waitItem))
                {
                    throw new InvalidOperationException("Couldn't write content");
                }
                //  Wait for all writes to happen
                await tcs.Task;
                //  Then create checkpoint
                await CreateCheckpointAsync(transactionLogItem.TransactionLogsFunc(), ct);
            }
            else if (transactionLogItem.Tcs == null)
            {
                var contentItem = new ContentItem(string.Empty, transactionLogItem.Tcs);

                if (!_channel.Writer.TryWrite(contentItem))
                {
                    throw new InvalidOperationException("Couldn't write content");
                }
            }
        }
        #endregion

        #region Content items processing
        private async Task ProcessContentItemsAsync(CancellationToken ct = default)
        {
            var contentItemQueue = new Queue<ContentItem>();

            do
            {
                //  Persist items
                if (contentItemQueue.Count > 0)
                {
                    await PersistItemsAsync(
                        contentItemQueue,
                        _stopBackgroundProcessingSource.Task.IsCompleted,
                        ct);
                }
                //  Get items from channel
                if (!DrainChannel(contentItemQueue))
                {   //  Wait a little
                    if (contentItemQueue.Count > 0)
                    {   //  Wait for the oldest element to age
                        var oldestTimestamp = contentItemQueue.Peek().Timestamp;
                        var delay = oldestTimestamp
                            + _logStorageWriter.LogPolicy.BufferingTimeWindow
                            - DateTime.Now;

                        if (delay > TimeSpan.Zero)
                        {
                            var delayTask = Task.Delay(delay);

                            await Task.WhenAny(_stopBackgroundProcessingSource.Task, delayTask);
                        }
                    }
                    else
                    {   //  Wait the full period since there are no elements
                        var delayTask = Task.Delay(_logStorageWriter.LogPolicy.BufferingTimeWindow);

                        await Task.WhenAny(_stopBackgroundProcessingSource.Task, delayTask);
                    }
                    //  Drain again after waiting
                    DrainChannel(contentItemQueue);
                }
            }
            while (!_stopBackgroundProcessingSource.Task.IsCompleted || contentItemQueue.Count > 0);
        }

        private async Task PersistItemsAsync(
            Queue<ContentItem> contentItemQueue,
            bool doForcePersistance,
            CancellationToken ct)
        {
            var tcsList = new List<TaskCompletionSource>(contentItemQueue.Count());
            var transactionTextList = new List<string>();
            var bufferingTimeWindow = _logStorageWriter.LogPolicy.BufferingTimeWindow;

            while (contentItemQueue.Count > 0
                && (doForcePersistance
                || DateTime.Now > contentItemQueue.Peek().Timestamp + bufferingTimeWindow))
            {   //  Persist one batch
                while (contentItemQueue.TryPeek(out var item)
                    && (!transactionTextList.Any()
                    || _logStorageWriter.CanFitInBatch(transactionTextList.Append(item.Content))))
                {
                    if (!string.IsNullOrWhiteSpace(item.Content))
                    {
                        transactionTextList.Add(item.Content);
                    }
                    if (item.Tcs != null)
                    {
                        tcsList.Add(item.Tcs);
                    }
                    contentItemQueue.Dequeue();
                }
                if (transactionTextList.Any())
                {
                    await _logStorageWriter.PersistBatchAsync(transactionTextList, ct);
                }
                //  Confirm persistance
                foreach (var tcs in tcsList)
                {
                    tcs.TrySetResult();
                }
                tcsList.Clear();
                transactionTextList.Clear();
            }
        }

        private bool DrainChannel(Queue<ContentItem> contentItemQueue)
        {
            bool hasOne = false;

            while (_channel.Reader.TryRead(out var item))
            {
                contentItemQueue.Enqueue(item);
                hasOne = true;
            }

            return hasOne;
        }
        #endregion
    }
}