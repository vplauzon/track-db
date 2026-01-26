using Azure;
using Azure.Storage.Blobs.Specialized;
using Polly;
using Polly.Retry;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TrackDb.Lib.Logging
{
    internal class BlobLock : IAsyncDisposable
    {
#if DEBUG
        private static readonly TimeSpan DEFAULT_LEASE_DURATION = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan DEFAULT_LEASE_RENEWAL_PERIOD = TimeSpan.FromSeconds(10);
#else
        private static readonly TimeSpan DEFAULT_LEASE_DURATION = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan DEFAULT_LEASE_RENEWAL_PERIOD = TimeSpan.FromSeconds(40);
#endif
        /// <summary>Workaround for Data lake SDK.</summary>
        private static readonly AsyncRetryPolicy _handle409Policy = Policy
            .Handle<RequestFailedException>(ex => ex.Status == 409 && ex.ErrorCode == "PathAlreadyExists")
            .RetryAsync(0); // 0 retries = just swallow the exception

        private readonly Task _backgroundTask;
        private readonly TaskCompletionSource _backgroundCompletedSource = new();

        #region Constructors
        internal static async Task<BlobLock> CreateAsync(
            BlobClients blobClients,
            CancellationToken ct)
        {
            var fileClient = blobClients.Directory.GetFileClient("lock");
            var blobClient = blobClients.Container.GetBlockBlobClient(fileClient.Path);
            var leaseClient = blobClient.GetBlobLeaseClient();

            try
            {
                await _handle409Policy.ExecuteAsync(
                    async () => await blobClients.Directory.CreateIfNotExistsAsync(
                        cancellationToken: ct));
                await _handle409Policy.ExecuteAsync(
                    async () => await fileClient.CreateIfNotExistsAsync(cancellationToken: ct));
                await leaseClient.AcquireAsync(DEFAULT_LEASE_DURATION);

                return new BlobLock(leaseClient, ct);
            }
            catch (RequestFailedException ex)
            {
                if (ex.ErrorCode == "LeaseAlreadyPresent")
                {
                    throw new InvalidOperationException("Lease on blob already present");
                }
                else
                {
                    throw;
                }
            }
        }

        private BlobLock(BlobLeaseClient leaseClient, CancellationToken ct)
        {
            LeaseClient = leaseClient;
            _backgroundTask = Task.Run(() => BackGroundRenewLockAsync(ct));
        }
        #endregion

        public BlobLeaseClient LeaseClient { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _backgroundCompletedSource.SetResult();
            await _backgroundTask;
        }

        private async Task BackGroundRenewLockAsync(CancellationToken ct)
        {
            while (!_backgroundCompletedSource.Task.IsCompleted)
            {
                await Task.WhenAny(
                    Task.Delay(DEFAULT_LEASE_RENEWAL_PERIOD, ct),
                    _backgroundCompletedSource.Task);
                await LeaseClient.RenewAsync(null, ct);
            }
            await LeaseClient.ReleaseAsync(cancellationToken: ct);
        }
    }
}