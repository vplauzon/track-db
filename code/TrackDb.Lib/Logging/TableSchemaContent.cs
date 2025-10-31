using System;
using System.Collections.Immutable;

namespace TrackDb.Lib.Logging
{
    internal record TableSchemaContent(
        string TableName,
        IImmutableList<ColumnSchemaContent> Columns);
}