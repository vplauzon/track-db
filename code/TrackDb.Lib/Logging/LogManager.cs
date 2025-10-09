using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;

namespace TrackDb.Lib.Logging
{
    internal class LogManager : IAsyncDisposable
    {
        private readonly LogPolicy _logPolicy;

        public LogManager(LogPolicy logPolicy)
        {
            _logPolicy = logPolicy;
        }

        public async Task InitLogsAsync()
        {
            await Task.CompletedTask;
        }

        public void QueueContent(string? contentText)
        {
            throw new NotImplementedException();
        }

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            throw new NotImplementedException();
        }
    }
}