using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2
{
    /// <summary>
    /// Database:  a collection of tables that can share transactions
    /// and are persisted in the same file.
    /// </summary>
    public class Database
    {
        #region Constructors
        public Database(string databasePath, params IEnumerable<TableSchema>)
        {
        }
        #endregion
    }
}