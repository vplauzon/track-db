using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class DataLifeCycleManager : IAsyncDisposable
    {
        #region Inner types
        private record LifeCycleItem(
            DataManagementActivity DataManagementActivity,
            TaskCompletionSource? Source);
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

        public DataLifeCycleManager(Database database)
        {
            _database = database;
            _dataLifeCycleAgents = ImmutableList.Create<DataLifeCycleAgentBase>(
                new NonMetaRecordPersistanceAgent(database),
                new RecordCountHardDeleteAgent(database),
                new TimeHardDeleteAgent(database),
                new MetaRecordPersistanceAgent(database),
                new TransactionLogMergingAgent(database));
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
                    //  We wait a little before processing so we can catch multiple transactions at once
                    await Task.WhenAny(
                        Task.Delay(_database.DatabasePolicy.LifeCyclePolicy.MaxWaitPeriod),
                        _dataMaintenanceStopSource.Task);
                    if (!_dataMaintenanceStopSource.Task.IsCompleted)
                    {
                        (var dataManagementActivity, var sourceList) = ReadAccumulatedItems();

                        RunDataMaintanceAndReleaseSources(dataManagementActivity, sourceList);
                    }
                }
                ReleaseBlocks(ref lastReleaseBlock);
            }
        }

        private void ReleaseBlocks(ref DateTime lastReleaseBlock)
        {
            if (lastReleaseBlock.Add(_database.DatabasePolicy.LifeCyclePolicy.MaxWaitPeriod)
                > DateTime.Now)
            {
                if (!_database.HasActiveTransaction)
                {
                    using (var tx = _database.CreateTransaction())
                    {
                        _database.ReleaseNoLongerInUsedBlocks(tx);
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
                //  Signal the sources
                foreach (var source in sourceList)
                {
                    source.TrySetResult();
                }
            }
            catch (Exception ex)
            {
                //  Signal the sources
                foreach (var source in sourceList)
                {
                    source.TrySetException(ex);
                }
                throw;
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
            using (var tx = _database.CreateTransaction())
            {
                foreach (var agent in _dataLifeCycleAgents)
                {
                    if (!_dataMaintenanceStopSource.Task.IsCompleted)
                    {
                        agent.Run(forcedDataManagementActivity, tx);
                    }
                    else
                    {   //  We stop running agent
                        return;
                    }
                }

                tx.Complete();
            }
        }
    }
}