using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.Logging
{
    internal class LogManager : IAsyncDisposable
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

        private static readonly string SEPARATOR = "\n";

        private readonly LogPolicy _logPolicy;
        private readonly LogStorageManager _logStorageManager;
        private readonly Task _backgroundProcessingTask;
        private readonly TaskCompletionSource _stopBackgroundProcessingSource =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Channel<ContentItem> _channel = Channel.CreateUnbounded<ContentItem>();

        public LogManager(LogPolicy logPolicy)
        {
            _logPolicy = logPolicy;
            _logStorageManager = new LogStorageManager(logPolicy);
            _backgroundProcessingTask = ProcessContentItemsAsync();
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _stopBackgroundProcessingSource.TrySetResult();
            await _backgroundProcessingTask;
            await ((IAsyncDisposable)_logStorageManager).DisposeAsync();
        }

        public async Task InitLogsAsync(CancellationToken ct)
        {
            await _logStorageManager.InitLogsAsync(ct);
        }

        public void QueueContent(string contentText)
        {
            if (!_channel.Writer.TryWrite(new ContentItem(contentText)))
            {
                throw new InvalidOperationException("Couldn't write content");
            }
        }

        public async Task CommitContentAsync(string contentText)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var item = new ContentItem(contentText, tcs);

            if (!_channel.Writer.TryWrite(item))
            {
                throw new InvalidOperationException("Couldn't write content");
            }

            await tcs.Task;
        }

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
            var builder = new StringBuilder();

            while (queue.Any()
                && builder.Length + queue.Peek().Content.Length + SEPARATOR.Length
                < _logStorageManager.MaxBlockSize)
            {
                var item = queue.Dequeue();

                builder.Append(item.Content);
                builder.Append(SEPARATOR);
                if (item.Tcs != null)
                {
                    tcsList.Add(item.Tcs);
                }
            }
            if (builder.Length == 0)
            {   //  Item too big
                throw new NotImplementedException("Item too big");
            }

            var buffer = Encoding.UTF8.GetBytes(builder.ToString());

            if(buffer.Length != builder.Length)
            {
                throw new NotSupportedException("Character bigger than 1 byte");
            }
            await _logStorageManager.PersistBlockAsync(buffer);
            //  Confirm persistance
            foreach (var tcs in tcsList)
            {
                tcs.TrySetResult();
            }
        }

        private bool IsBlockComplete(IEnumerable<ContentItem> items)
        {
            return items.Sum(i => i.Content.Length + SEPARATOR.Length)
                >= _logStorageManager.MaxBlockSize;
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