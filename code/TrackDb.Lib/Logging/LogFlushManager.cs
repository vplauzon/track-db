using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace TrackDb.Lib.Logging
{
    internal class LogFlushManager : IAsyncDisposable
    {
        private readonly Func<IEnumerable<TransactionLogItem>> _flushTransactionLogItems;
        private readonly LogTransactionWriter _logTransactionWriter;
        private readonly Channel<bool> _channel = Channel.CreateUnbounded<bool>();
        private readonly TaskCompletionSource _stopSource = new();
        private readonly Task _backgroundTask;

        public LogFlushManager(
            Func<IEnumerable<TransactionLogItem>> flushTransactionLogItems,
            LogTransactionWriter logTransactionWriter)
        {
            _flushTransactionLogItems = flushTransactionLogItems;
            _logTransactionWriter = logTransactionWriter;
            _backgroundTask = BackgroundAsync();
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _stopSource.TrySetResult();
            await _backgroundTask;
            await ((IAsyncDisposable)_logTransactionWriter).DisposeAsync();
        }

        public void Push()
        {
            if (!_channel.Writer.TryWrite(true))
            {
                throw new InvalidOperationException("Can't write to channel");
            }
        }

        private async Task BackgroundAsync()
        {
            while (!_stopSource.Task.IsCompleted
                || _channel.Reader.WaitToReadAsync().IsCompleted)
            {
                var pushTask = _channel.Reader.WaitToReadAsync().AsTask();

                //  Wait for push
                await Task.WhenAny(pushTask, _stopSource.Task);
                //  Flush queue
                while (_channel.Reader.TryRead(out var _))
                {
                }

                var logItems = _flushTransactionLogItems();

                foreach (var logItem in logItems)
                {
                    await _logTransactionWriter.QueueTransactionLogItemAsync(
                        logItem,
                        CancellationToken.None);
                }
            }
        }
    }
}