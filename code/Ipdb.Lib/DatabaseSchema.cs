using System;

namespace Ipdb.Lib
{
    public class DatabaseSchema
    {
        #region Constructor
        public DatabaseSchema()
        {
        }
        #endregion

        public DatabaseSchema AddTable<T>(string tableName, TableSchema<T> schema)
        {
            throw new NotImplementedException();
        }
    }
}