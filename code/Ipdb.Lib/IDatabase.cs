using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    internal interface IDatabase
    {
        ITable<T> CreateTable<T>(string tableName);
    }
}