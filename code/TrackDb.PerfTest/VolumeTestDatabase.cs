using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace TrackDb.PerfTest
{
    internal class VolumeTestDatabase : DatabaseContextBase
    {
        #region Entity types
        public record Employee(string EmployeeId, string Name);

        public enum RequestStatus
        {
            Initiated,
            Treating,
            Completed
        }

        public record Request(
            string EmployeeId,
            string RequestCode,
            RequestStatus RequestStatus);

        public record Document(string RequestCode, string DocumentContent);

        public enum OrderStatus
        {
            Initiated,
            Intreatment,
            Completed
        }

        public record TriggeringOrder(string OrderId, OrderStatus OrderStatus);
        
        public record OrderSummary(OrderStatus OrderStatus, int OrderCount);
        #endregion

        private const string EMPLOYEE_TABLE = "Employee";
        private const string REQUEST_TABLE = "Request";
        private const string DOCUMENT_TABLE = "Document";
        private const string ORDER_TABLE = "TriggeringOrder";
        private const string ORDER_SUMMARY_TABLE = "OrderSummary";

        #region Constructor
        public static async Task<VolumeTestDatabase> CreateAsync(
            Func<DatabasePolicy, DatabasePolicy>? dataPolicyChanger = null)
        {
            var dataPolicy = DatabasePolicy.Create();
            var modifiedDataPolicy = dataPolicyChanger != null
                ? dataPolicyChanger(dataPolicy)
                : dataPolicy;
            var db = await Database.CreateAsync<VolumeTestDatabase>(
                modifiedDataPolicy,
                db => new(db),
                CancellationToken.None,
                TypedTableSchema<Employee>.FromConstructor(EMPLOYEE_TABLE)
                .AddPrimaryKeyProperty(p => p.EmployeeId),
                TypedTableSchema<Request>.FromConstructor(REQUEST_TABLE)
                .AddPrimaryKeyProperty(p => p.RequestCode),
                TypedTableSchema<Document>.FromConstructor(DOCUMENT_TABLE),
                TypedTableSchema<TriggeringOrder>.FromConstructor(ORDER_TABLE)
                .AddPrimaryKeyProperty(o => o.OrderId)
                .AddTrigger((db, tx) => TriggerOrder((VolumeTestDatabase)db, tx)),
                TypedTableSchema<OrderSummary>.FromConstructor(ORDER_SUMMARY_TABLE));

            return db;
        }

        private VolumeTestDatabase(Database database)
            : base(database)
        {
        }
        #endregion

        public TypedTable<Employee> EmployeeTable
            => Database.GetTypedTable<Employee>(EMPLOYEE_TABLE);

        public TypedTable<Request> RequestTable
            => Database.GetTypedTable<Request>(REQUEST_TABLE);

        public TypedTable<Document> DocumentTable
            => Database.GetTypedTable<Document>(DOCUMENT_TABLE);

        public TypedTable<TriggeringOrder> TriggeringOrderTable
            => Database.GetTypedTable<TriggeringOrder>(ORDER_TABLE);

        public TypedTable<OrderSummary> OrderSummaryTable
            => Database.GetTypedTable<OrderSummary>(ORDER_SUMMARY_TABLE);

        private static void TriggerOrder(VolumeTestDatabase db, TransactionContext tx)
        {
            var accumulations = db.TriggeringOrderTable.Query(tx)
            .WithinTransactionOnly()
            .GroupBy(m => m.OrderStatus)
            .Select(g => new OrderSummary(g.Key, g.Count()));
            var decumulations = db.TriggeringOrderTable.TombstonedWithinTransaction(tx)
            .GroupBy(m => m.OrderStatus)
            .Select(g => new OrderSummary(g.Key, -g.Count()));
            var integratedAccumulations = accumulations
            .Concat(decumulations)
            .GroupBy(m => m.OrderStatus)
            .Select(g => new OrderSummary(g.Key, g.Sum(m => m.OrderCount)));

            db.OrderSummaryTable.AppendRecords(integratedAccumulations, tx);
        }
    }
}