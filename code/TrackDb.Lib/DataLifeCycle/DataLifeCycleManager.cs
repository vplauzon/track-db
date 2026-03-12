using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle.Persistance;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class DataLifeCycleManager : IAsyncDisposable
    {
        #region Inner types
        private record LifeCycleItem(
            DataManagementActivity DataManagementActivity,
            TaskCompletionSource? Source)
        {
            public bool IsForced => DataManagementActivity != DataManagementActivity.None
                || Source != null;
        }
        #endregion

        private readonly Database _database;
        //  List of agents responsible for data life cycle
        private readonly IImmutableList<DataLifeCycleAgentBase> _dataLifeCycleAgents;
        //  Channel used to push data lifecycle trigger over threads
        private readonly Channel<LifeCycleItem> _channel = Channel.CreateUnbounded<LifeCycleItem>();
        //  Task running as long as this object is alive
        private readonly Task _dataMaintenanceTask;
        //  TaskCompletionSource signaling the stop of the background task
        private readonly TaskCompletionSource _dataMaintenanceStopSource = new TaskCompletionSource();
        private TaskCompletionSource<bool>? _batchDelayInterrupt;

        public DataLifeCycleManager(Database database)
        {
            var nonMetaTableProvider = new NonMetaTableProvider(database);
            var metaTableProvider = new MetaTableProvider(database);

            _database = database;
            _dataLifeCycleAgents = ImmutableList.Create<DataLifeCycleAgentBase>(
                new TransactionLogMergingAgent(database),
                new RecordPersistanceAgent(
                    database,
                    new RecordCountPersistanceCandidateProvider(database, nonMetaTableProvider)),
                new RecordPersistanceAgent(
                    database,
                    new RecordCountPersistanceCandidateProvider(database, metaTableProvider)),
                new RecordCountHardDeleteAgent(database),
                new TimeHardDeleteAgent(database));
            _dataMaintenanceTask = DataMaintanceAsync();
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _dataMaintenanceStopSource.SetResult();
            await _dataMaintenanceTask;
        }

        public void TriggerDataManagement()
        {
            SendLifeCycleManagementTrigger(new LifeCycleItem(DataManagementActivity.None, null));
        }

        public async Task ForceDataManagementAsync(
            DataManagementActivity forcedDataManagementActivities = DataManagementActivity.None)
        {
            var source = new TaskCompletionSource();

            SendLifeCycleManagementTrigger(
                new LifeCycleItem(forcedDataManagementActivities, source));
            await source.Task;
        }

        public void ObserveBackgroundTask()
        {
            if (_dataMaintenanceTask.Status == TaskStatus.Faulted)
            {
                _dataMaintenanceTask.Wait();
            }
        }

        private void SendLifeCycleManagementTrigger(LifeCycleItem lifeCycleItem)
        {
            ObserveBackgroundTask();
            if (!_channel.Writer.TryWrite(lifeCycleItem))
            {
                throw new InvalidOperationException("Couldn't trigger life cycle management");
            }
            
            // Interrupt the batch delay if this is a forced call arriving during a delay
            if (lifeCycleItem.IsForced)
            {
                _batchDelayInterrupt?.TrySetResult(true);
            }
        }

        private async Task DataMaintanceAsync()
        {
            var lastReleaseBlock = DateTime.Now;

            //  This loop is continuous as long as the object exists
            while (!_dataMaintenanceStopSource.Task.IsCompleted)
            {
                var itemTask = _channel.Reader.WaitToReadAsync().AsTask();

                //  Wait for the first item
                await Task.WhenAny(itemTask, _dataMaintenanceStopSource.Task);

                if (!_dataMaintenanceStopSource.Task.IsCompleted)
                {
                    // Read the first item that triggered this cycle
                    if (!_channel.Reader.TryRead(out var firstItem))
                    {
                        continue; // Shouldn't happen, but be safe
                    }
                    
                    if (!firstItem.IsForced)
                    {
                        // Create new interrupt signal for this batch window
                        _batchDelayInterrupt = new TaskCompletionSource<bool>();
                        
                        //  We wait a little before processing so we can catch multiple transactions at once
                        var completedTask = await Task.WhenAny(
                            Task.Delay(_database.DatabasePolicy.LifeCyclePolicy.MaxWaitPeriod),
                            _batchDelayInterrupt.Task,
                            _dataMaintenanceStopSource.Task);
                        
                        // No exception handling needed - just check which task completed
                    }
                    // If first item was forced, skip delay entirely
                    
                    if (!_dataMaintenanceStopSource.Task.IsCompleted)
                    {
                        // Accumulate the first item we already read
                        var sourceList = ImmutableList<TaskCompletionSource>.Empty.ToBuilder();
                        var dataManagementActivity = firstItem.DataManagementActivity;
                        
                        if (firstItem.Source != null)
                        {
                            sourceList.Add(firstItem.Source);
                        }
                        
                        // Read any additional accumulated items
                        while (_channel.Reader.TryRead(out var item))
                        {
                            dataManagementActivity = dataManagementActivity | item.DataManagementActivity;
                            if (item.Source != null)
                            {
                                sourceList.Add(item.Source);
                            }
                        }

                        RunDataMaintanceAndReleaseSources(dataManagementActivity, sourceList.ToImmutable());
                    }
                }
                ReleaseBlocks(ref lastReleaseBlock);
            }
        }

        private void ReleaseBlocks(ref DateTime lastReleaseBlock)
        {
            if (DateTime.Now >
                lastReleaseBlock.Add(_database.DatabasePolicy.LifeCyclePolicy.BlockReleaseWaitPeriod))
            {
                if (!_database.HasActiveTransaction)
                {
                    using (var tx = _database.CreateTransaction())
                    {
                        var resetBlockIds =
                            _database.AvailabilityBlockManager.ResetNoLongerInUsedBlocks(tx);

                        foreach (var blockId in resetBlockIds)
                        {
                            _database.InvalidateCache(blockId);
                        }
                        lastReleaseBlock = DateTime.Now;

                        tx.Complete();
                    }
                }
            }
        }

        private void RunDataMaintanceAndReleaseSources(
            DataManagementActivity dataManagementActivity,
            IImmutableList<TaskCompletionSource> sourceList)
        {
            try
            {
                RunDataMaintance(dataManagementActivity);
            }
            catch (Exception ex)
            {
                //  Signal the sources
                foreach (var source in sourceList)
                {
                    source.TrySetException(ex);
                }
            }
            finally
            {
                //  Signal the sources
                foreach (var source in sourceList)
                {
                    source.TrySetResult();
                }
            }
        }

        private (DataManagementActivity, IImmutableList<TaskCompletionSource>) ReadAccumulatedItems()
        {
            var sourceList = ImmutableList<TaskCompletionSource>.Empty.ToBuilder();
            var dataManagementActivity = DataManagementActivity.None;

            while (_channel.Reader.TryRead(out var item))
            {
                dataManagementActivity =
                    dataManagementActivity | item.DataManagementActivity;
                if (item.Source != null)
                {
                    sourceList.Add(item.Source);
                }
            }

            return (dataManagementActivity, sourceList.ToImmutable());
        }

        private void RunDataMaintance(DataManagementActivity forcedDataManagementActivity)
        {
            foreach (var agent in _dataLifeCycleAgents)
            {
                if (!_dataMaintenanceStopSource.Task.IsCompleted)
                {
                    agent.Run(forcedDataManagementActivity);
                }
                else
                {   //  We stop running agent
                    return;
                }
            }
        }
    }
}