using TrackDb.Lib.Cache;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace TrackDb.Lib
{
    public class Table
    {
        internal Table(Database database, TableSchema schema)
        {
            Database = database;
            Schema = schema;
        }

        public Database Database { get; }

        public TableSchema Schema { get; }
    }
}