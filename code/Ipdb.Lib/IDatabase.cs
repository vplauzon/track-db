using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    public interface IDatabase
    {
        ITable<T> AddTable<T>(string tableName, Schema<T> schema);

        Task LoadDataAsync();

        void ExecuteCommand(DatabaseCommand databaseCommand);
    }
}