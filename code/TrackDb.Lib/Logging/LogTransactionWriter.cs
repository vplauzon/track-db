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
            long CheckpointIndex,
            string Content,
            DateTime Timestamp,
            TaskCompletionSource? Tcs)
        {
            public ContentItem(long checkpointIndex, string content)
                : this(checkpointIndex, content, DateTime.Now, null)
            {
            }

            public ContentItem(long checkpointIndex, string content, TaskCompletionSource tcs)
                : this(checkpointIndex, content, DateTime.Now, tcs)
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
        private Task _checkpointTask = Task.CompletedTask;

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

        public long LastCheckpointIndex => _logStorageWriter.LastCheckpointIndex;

        #region Checkpoint
        public void QueueCheckpoint(
            long checkpointIndex,
            IAsyncEnumerable<TransactionLog> transactionLogs,
            Action postCheckpointAction)
        {
            if (!_checkpointTask.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException("Last checkpoint isn't completed or failed");
            }

            _checkpointTask = CreateCheckpointAsync(
                checkpointIndex,
                transactionLogs,
                postCheckpointAction);
        }

        private async Task CreateCheckpointAsync(
            long checkpointIndex,
            IAsyncEnumerable<TransactionLog> transactions,
            Action postCheckpointAction)
        {
            var cts = new CancellationTokenSource();

            await _logStorageWriter.CreateCheckpointAsync(
                checkpointIndex,
                ToTransactionText(transactions, cts),
                cts.Token);
            postCheckpointAction();
        }

        private async IAsyncEnumerable<string> ToTransactionText(
            IAsyncEnumerable<TransactionLog> transactions,
            CancellationTokenSource cts)
        {
            await foreach (var tx in transactions)
            {
                if (_stopBackgroundProcessingSource.Task.IsCompletedSuccessfully)
                {
                    cts.Cancel();
                }
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
        public void QueueContent(long checkpointIndex, TransactionLog transactionLog)
        {
            var content = TransactionContent.FromTransactionLog(
                transactionLog,
                _tombstoneTable.Schema,
                _tableSchemaMap);
            var item = new ContentItem(checkpointIndex, content.ToJson());

            if (!_channel.Writer.TryWrite(item))
            {
                throw new InvalidOperationException("Couldn't write content");
            }
        }

        public async Task CommitContentAsync(
            long checkpointIndex,
            TransactionLog transactionLog,
            CancellationToken ct)
        {
            var content = TransactionContent.FromTransactionLog(
                transactionLog,
                _tombstoneTable.Schema,
                _tableSchemaMap);
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var item = new ContentItem(
                checkpointIndex,
                content.Tables.Count > 0 ? content.ToJson() : string.Empty,
                tcs);

            if (!_channel.Writer.TryWrite(item))
            {
                throw new InvalidOperationException("Couldn't write content");
            }

            await tcs.Task.WaitAsync(ct);
        }
        #endregion

        #region Content items processing
        private async Task ProcessContentItemsAsync(CancellationToken ct = default)
        {
            var queueMap = new Dictionary<long, Queue<ContentItem>>();

            do
            {
                //  Persist items
                if (queueMap.Any())
                {
                    await PersistItemsAsync(
                        queueMap,
                        _stopBackgroundProcessingSource.Task.IsCompleted,
                        ct);
                }
                //  Get items from channel
                if (!DrainChannel(queueMap))
                {   //  Wait a little
                    if (queueMap.Count > 0)
                    {   //  Wait for the oldest element to age
                        var oldestTimestamp = queueMap.Values
                            .Select(q => q.Peek().Timestamp)
                            .Min();
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
                    DrainChannel(queueMap);
                }
            }
            while (!_stopBackgroundProcessingSource.Task.IsCompleted || queueMap.Count > 0);
        }

        private async Task PersistItemsAsync(
            Dictionary<long, Queue<ContentItem>> queueMap,
            bool doForcePersistance,
            CancellationToken ct)
        {
            var tcsList = new List<TaskCompletionSource>(queueMap.Values.Sum(q => q.Count()));
            var transactionTextList = new List<string>();
            var bufferingTimeWindow = _logStorageWriter.LogPolicy.BufferingTimeWindow;

            //  Materialize the queue map so we can delete keys within the loop
            foreach (var pair in queueMap.ToImmutableArray())
            {
                var checkpointIndex = pair.Key;
                var queue = pair.Value;

                while (queue.Any()
                    && (doForcePersistance
                    || DateTime.Now > queue.Peek().Timestamp + bufferingTimeWindow))
                {   //  Persist one batch
                    while (queue.TryPeek(out var item)
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
                        queue.Dequeue();
                    }
                    if (transactionTextList.Any())
                    {
                        await _logStorageWriter.PersistBatchAsync(
                            checkpointIndex,
                            transactionTextList,
                            ct);
                    }
                    //  Confirm persistance
                    foreach (var tcs in tcsList)
                    {
                        tcs.TrySetResult();
                    }
                    tcsList.Clear();
                    transactionTextList.Clear();
                }
                if (!queue.Any())
                {
                    queueMap.Remove(checkpointIndex);
                }
            }
        }

        private bool DrainChannel(Dictionary<long, Queue<ContentItem>> queueMap)
        {
            bool hasOne = false;

            while (_channel.Reader.TryRead(out var item))
            {
                if (!queueMap.ContainsKey(item.CheckpointIndex))
                {
                    queueMap[item.CheckpointIndex] = new();
                }
                queueMap[item.CheckpointIndex].Enqueue(item);
                hasOne = true;
            }

            return hasOne;
        }
        #endregion
    }
}