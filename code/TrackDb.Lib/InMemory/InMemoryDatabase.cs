﻿using TrackDb.Lib.InMemory.Block;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrackDb.Lib.InMemory
{
    internal record InMemoryDatabase(
        IImmutableDictionary<string, ImmutableTableTransactionLogs> TableTransactionLogsMap)
    {
        public InMemoryDatabase()
            : this(ImmutableDictionary<string, ImmutableTableTransactionLogs>.Empty)
        {
        }

        public InMemoryDatabase CommitLog(TransactionLog transactionLog)
        {
            var logs = ImmutableDictionary<string, ImmutableTableTransactionLogs>
                .Empty
                .ToBuilder();

            logs.AddRange(TableTransactionLogsMap);
            foreach (var pair in transactionLog.TableBlockBuilderMap)
            {
                var tableName = pair.Key;
                var blockBuilder = pair.Value;

                if (logs.ContainsKey(tableName))
                {
                    logs[tableName] = new ImmutableTableTransactionLogs(
                        logs[tableName].InMemoryBlocks.Add(blockBuilder));
                }
                else
                {
                    logs[tableName] = new ImmutableTableTransactionLogs(
                        new[] { blockBuilder }.Cast<IBlock>().ToImmutableArray());
                }
            }

            return new InMemoryDatabase(logs.ToImmutableDictionary());
        }

        public InMemoryDatabase RemovePrefixes(InMemoryDatabase other)
        {
            var extraTableNames =
                other.TableTransactionLogsMap.Keys.Except(TableTransactionLogsMap.Keys);

            if (extraTableNames.Any())
            {
                throw new ArgumentException(
                    $"Extra table name:  {extraTableNames.First()}",
                    nameof(other));
            }

            var mapBuilder =
                ImmutableDictionary<string, ImmutableTableTransactionLogs>.Empty.ToBuilder();

            foreach (var pair in TableTransactionLogsMap)
            {
                var tableName = pair.Key;
                var logs = pair.Value;

                if (other.TableTransactionLogsMap.TryGetValue(tableName, out var otherLogs)
                    && logs.InMemoryBlocks.Count > otherLogs.InMemoryBlocks.Count)
                {
                    mapBuilder.Add(
                        tableName,
                        new ImmutableTableTransactionLogs(
                            logs.InMemoryBlocks
                            .Skip(otherLogs.InMemoryBlocks.Count)
                            .ToImmutableArray()));
                }
            }

            return new InMemoryDatabase(mapBuilder.ToImmutableDictionary());
        }

        public InMemoryDatabase Append(InMemoryDatabase other)
        {
            var mapBuilder =
               ImmutableDictionary<string, ImmutableTableTransactionLogs>.Empty.ToBuilder();

            mapBuilder.AddRange(TableTransactionLogsMap);
            foreach (var pair in other.TableTransactionLogsMap)
            {
                var tableName = pair.Key;
                var logs = pair.Value;

                if (mapBuilder.TryGetValue(tableName, out var thisLogs))
                {
                    mapBuilder[tableName] = new ImmutableTableTransactionLogs(
                        thisLogs.InMemoryBlocks
                        .Concat(logs.InMemoryBlocks)
                        .ToImmutableArray());
                }
                else
                {
                    mapBuilder[tableName] = logs;
                }
            }

            return new InMemoryDatabase(mapBuilder.ToImmutableDictionary());
        }
    }
}