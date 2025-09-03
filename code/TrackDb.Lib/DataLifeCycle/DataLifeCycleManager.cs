using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.DbStorage;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class DataLifeCycleManager : IAsyncDisposable
    {
        private readonly Database _database;

        //  Task running as long as this object is alive
        private readonly Task _dataMaintenanceTask;
        //  TaskCompletionSource signaling the stop of the background task
        private readonly TaskCompletionSource _dataMaintenanceStopSource = new TaskCompletionSource();
        //  List of agents responsible for data life cycle
        private readonly IImmutableList<DataLifeCycleAgentBase> _dataLifeCycleAgents;
        private TaskCompletionSource _dataMaintenanceTriggerSource = new TaskCompletionSource();
        private DataManagementActivity _forcedDataManagementActivity = DataManagementActivity.None;
        private TaskCompletionSource? _forceDataManagementSource = null;

        public DataLifeCycleManager(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<StorageManager> storageManager)
        {
            _database = database;
            _dataMaintenanceTask = DataMaintanceAsync();
            _dataLifeCycleAgents = ImmutableList.Create<DataLifeCycleAgentBase>(
                new LogMergingAgent(database, tombstoneTable, storageManager),
                new RecordPersistanceAgent(database, tombstoneTable, storageManager),
                new HardDeleteAgent(database, tombstoneTable, storageManager));
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _dataMaintenanceStopSource.SetResult();
            await _dataMaintenanceTask;
        }

        public void TriggerDataManagement()
        {
            ObserveBackgroundTask();
            _dataMaintenanceTriggerSource.TrySetResult();
        }

        public async Task ForceDataManagementAsync(
            DataManagementActivity forcedDataManagementActivities = DataManagementActivity.None)
        {
            if (forcedDataManagementActivities != DataManagementActivity.None)
            {
                _forceDataManagementSource = new TaskCompletionSource();
                Interlocked.Exchange(ref _forcedDataManagementActivity, forcedDataManagementActivities);
                _dataMaintenanceTriggerSource.TrySetResult();
                await _forceDataManagementSource.Task;
            }
        }

        public void ObserveBackgroundTask()
        {
            if (_dataMaintenanceTask.Status == TaskStatus.Faulted)
            {
                _dataMaintenanceTask.Wait();
            }
        }

        private async Task DataMaintanceAsync()
        {   //  This loop is continuous as long as the object exists
            while (!_dataMaintenanceStopSource.Task.IsCompleted)
            {
                await Task.WhenAny(
                    _dataMaintenanceTriggerSource.Task,
                    _dataMaintenanceStopSource.Task);

                //  Reset the trigger source (before starting the work)
                _dataMaintenanceTriggerSource = new TaskCompletionSource();

                var forcedDataManagementActivity = Interlocked.Exchange(
                    ref _forcedDataManagementActivity,
                    DataManagementActivity.None);

                DataMaintanceIteration(forcedDataManagementActivity);
            }
        }

        private void DataMaintanceIteration(DataManagementActivity forcedDataManagementActivity)
        {
            try
            {
                foreach (var agent in _dataLifeCycleAgents)
                {
                    if (_dataMaintenanceStopSource.Task.IsCompleted)
                    {   //  We stop running agent
                        break;
                    }
                    else if (!agent.Run(forcedDataManagementActivity))
                    {   //  Agent isn't complete, so we rerun everything
                        DataMaintanceIteration(forcedDataManagementActivity);

                        return;
                    }
                }
                //  We're done
                _forceDataManagementSource?.TrySetResult();

                return;
            }
            catch (Exception ex)
            {
                _forceDataManagementSource?.TrySetException(ex);
            }
        }
    }
}