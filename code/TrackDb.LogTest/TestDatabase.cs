using Azure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace TrackDb.LogTest
{
    internal class TestDatabase : IAsyncDisposable
    {
        private static readonly string _runFolder = DateTime.UtcNow.ToString("yyyy-MM-dd_HH-mm-ss");

        #region Entity types
        public enum WorkflowState
        {
            Pending,
            Started,
            Completed
        }

        public enum ActivityState
        {
            Started,
            Completed
        }

        public enum TaskState
        {
            Started,
            Completed
        }

        public record Workflow(
            string WorkflowName,
            int WorkflowId,
            WorkflowState State,
            DateTime StartTime);

        public record Activity(
            string WorkflowName,
            string ActivityName,
            string? ParentActivityName,
            ActivityState State);

        public record ActivityTask(
            string WorkflowName,
            string ActivityName,
            string TaskName,
            TaskState State,
            DateTime StartTime,
            DateTime? EndTime);
        #endregion

        private const string WORKFLOW_TABLE = "Workflow";
        private const string ACTIVITY_TABLE = "Activity";
        private const string TASK_TABLE = "Task";

        #region Constructor
        public static async Task<TestDatabase> CreateAsync(
            string testId,
            Func<DatabasePolicy, DatabasePolicy>? dataPolicyChanger = null)
        {
            var logFolderUriText = TestConfiguration.Instance.GetConfiguration("logFolderUri");
            var logFolderKey = TestConfiguration.Instance.GetConfiguration("logFolderKey");

            if (string.IsNullOrWhiteSpace(logFolderUriText))
            {
                throw new InvalidOperationException("'logFolderUri' is missing from configuration");
            }
            if (string.IsNullOrWhiteSpace(logFolderKey))
            {
                throw new InvalidOperationException("'logFolderKey' is missing from configuration");
            }

            var logFolderUri = new UriBuilder(logFolderUriText);

            logFolderUri.Path += $"/{_runFolder}/{testId}";

            var accountName = logFolderUri.Host.Split('.').First();
            var dataPolicy = DatabasePolicy.Create(LogPolicy: LogPolicy.Create(
                new StorageConfiguration(
                    logFolderUri.Uri,
                    null,
                    new StorageSharedKeyCredential(accountName, logFolderKey))));
            var modifiedDataPolicy = dataPolicyChanger != null
                ? dataPolicyChanger(dataPolicy)
                : dataPolicy;
            var db = await Database.CreateAsync(
                dataPolicy,
                TypedTableSchema<Workflow>.FromConstructor(WORKFLOW_TABLE)
                .AddPrimaryKeyProperty(m => m.WorkflowName),
                TypedTableSchema<Activity>.FromConstructor(ACTIVITY_TABLE)
                .AddPrimaryKeyProperty(m => m.ActivityName),
                TypedTableSchema<ActivityTask>.FromConstructor(TASK_TABLE)
                .AddPrimaryKeyProperty(m => m.ActivityName)
                .AddPrimaryKeyProperty(m => m.TaskName));

            return new TestDatabase(db);
        }

        private TestDatabase(Database database)
        {
            Database = database;
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
        }

        public Database Database { get; }

        public TypedTable<Workflow> WorkflowTable
            => Database.GetTypedTable<Workflow>(WORKFLOW_TABLE);

        public TypedTable<Activity> ActivityTable
            => Database.GetTypedTable<Activity>(ACTIVITY_TABLE);

        public TypedTable<ActivityTask> TaskTable
            => Database.GetTypedTable<ActivityTask>(TASK_TABLE);
    }
}