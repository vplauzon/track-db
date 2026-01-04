using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib
{
    /// <summary>Schema of a table including data mapping to a .NET type.</summary>
    public class TypedTableSchema<T> : TableSchema
    {
        #region Inner Types
        private record ConstructorMapping(
            ConstructorInfo ConstructorInfo,
            IImmutableList<ConstructorParameterMapping> ConstructorParameterMappings,
            IImmutableList<ColumnSchema> ColumnSchemas)
        {
            public static ConstructorMapping FromConstructor(Type type)
            {
                var maxConstructorParams = type.GetConstructors()
                    .Max(c => c.GetParameters().Count());
                var argMaxConstructor = type.GetConstructors()
                    .First(c => c.GetParameters().Count() == maxConstructorParams);
                var parameterMappings =
                    ConstructorParameterMapping.FromParameters(argMaxConstructor.GetParameters());
                var columnSchemas = parameterMappings
                    .Select(p => p.ConstructorMapping == null
                    ? [new ColumnSchema(p.PropertyInfo.Name, p.PropertyInfo.PropertyType)]
                    : p.ConstructorMapping.ColumnSchemas.Select(s => new ColumnSchema(
                        $"{p.PropertyInfo.Name}.{s.ColumnName}",
                        s.ColumnType)))
                    .SelectMany(m => m)
                    .ToImmutableArray();

                return new ConstructorMapping(argMaxConstructor, parameterMappings, columnSchemas);
            }

            public IImmutableDictionary<Type, ConstructorMapping> GetIndexedMappings()
            {
                return ListIndexedMappings()
                    .Select(p => new
                    {
                        Type = p.Item1,
                        Mapping = p.Item2
                    })
                    //  Take only on mapping per type (in case a type shows up multiple times)
                    .GroupBy(o => o.Type)
                    .Select(g => g.First())
                    .ToImmutableDictionary(o => o.Type, o => o.Mapping);
            }

            private IEnumerable<(Type, ConstructorMapping)> ListIndexedMappings()
            {
                yield return (ConstructorInfo.DeclaringType!, this);

                foreach (var cpm in ConstructorParameterMappings)
                {
                    if (cpm.ConstructorMapping != null)
                    {
                        foreach (var mapping in cpm.ConstructorMapping.ListIndexedMappings())
                        {
                            yield return mapping;
                        }
                    }
                }
            }

            public IImmutableDictionary<string, IImmutableList<int>> GetPropertyPathToColumnIndexesMap()
            {
                var pathIndex = ColumnSchemas
                    .Select(c => c.ColumnName)
                    .Index()
                    .Select(o => new
                    {
                        o.Index,
                        ColumnPath = o.Item,
                        ColumnParts = o.Item.Split('.')
                    })
                    .ToImmutableArray();
                var maxDepth = pathIndex
                    .Select(o => o.ColumnParts.Length)
                    .Max();
                var pairs = new List<KeyValuePair<string, ImmutableList<int>>>();

                for (var depth = 1; depth <= maxDepth; ++depth)
                {
                    var depthPairs = pathIndex
                        .Where(o => o.ColumnParts.Length >= depth)
                        .Select(o => new
                        {
                            Prefix = string.Join('.', o.ColumnParts.Take(depth)),
                            o.Index
                        })
                        .GroupBy(o => o.Prefix)
                        .Select(g => KeyValuePair.Create(g.Key, g.Select(o => o.Index).ToImmutableList()));

                    pairs.AddRange(depthPairs);
                }

                return pairs.ToImmutableDictionary(p => p.Key, p => (IImmutableList<int>)p.Value);
            }

            public ReadOnlySpan<object?> ObjectToColumns(object record)
            {
                var buffer = new object?[ColumnSchemas.Count];

                ObjectToColumns(record!, buffer);

                return buffer;
            }

            public object ColumnsToObject(ReadOnlySpan<object?> columns)
            {
                if (columns.Length != ColumnSchemas.Count)
                {
                    throw new ArgumentOutOfRangeException(nameof(columns));
                }

                var parameters = new object?[ConstructorParameterMappings.Count];
                var remainingColumns = columns;

                for (var i = 0; i != ConstructorParameterMappings.Count; ++i)
                {
                    var parameterMapping = ConstructorParameterMappings[i];

                    if (remainingColumns.Length == 0)
                    {
                        throw new InvalidOperationException("Remaining buffer is empty");
                    }

                    if (parameterMapping.ConstructorMapping != null)
                    {
                        var columnCount = parameterMapping.ConstructorMapping.ColumnSchemas.Count;

                        parameters[i] = parameterMapping.ConstructorMapping.ColumnsToObject(
                            remainingColumns.Slice(0, columnCount));
                        remainingColumns = remainingColumns.Slice(columnCount);
                    }
                    else
                    {
                        parameters[i] = remainingColumns[0];
                        remainingColumns = remainingColumns.Slice(1);
                    }
                }
                if (remainingColumns.Length != 0)
                {
                    throw new InvalidOperationException("Buffer should be empty here");
                }

                return ConstructorInfo.Invoke(parameters);
            }

            private void ObjectToColumns(object record, Span<object?> columns)
            {
                if (columns.Length != ColumnSchemas.Count)
                {
                    throw new ArgumentOutOfRangeException(nameof(columns));
                }

                var remainingColumns = columns;

                for (var i = 0; i != ConstructorParameterMappings.Count; ++i)
                {
                    var parameterMapping = ConstructorParameterMappings[i];
                    var propertyValue = parameterMapping.PropertyInfo.GetValue(record, null);

                    if (remainingColumns.Length == 0)
                    {
                        throw new InvalidOperationException("Remaining buffer is empty");
                    }
                    if (parameterMapping.ConstructorMapping != null)
                    {
                        if (propertyValue == null)
                        {
                            throw new NotSupportedException("Record sub object is null");
                        }
                        else
                        {
                            var columnCount = parameterMapping.ConstructorMapping.ColumnSchemas.Count;

                            parameterMapping.ConstructorMapping.ObjectToColumns(
                                propertyValue,
                                remainingColumns.Slice(0, columnCount));
                            remainingColumns = remainingColumns.Slice(columnCount);
                        }
                    }
                    else
                    {
                        remainingColumns[0] = propertyValue;
                        remainingColumns = remainingColumns.Slice(1);
                    }
                }
                if (remainingColumns.Length != 0)
                {
                    throw new InvalidOperationException("Buffer should be empty here");
                }
            }
        }

        private record ConstructorParameterMapping(
            PropertyInfo PropertyInfo,
            ConstructorMapping? ConstructorMapping)
        {
            public static IImmutableList<ConstructorParameterMapping> FromParameters(
                IEnumerable<ParameterInfo> parameterInfos)
            {
                var columnMappings = parameterInfos
                    .Select(param =>
                    {
                        var matchingProp = GetMatchingProperty(param);

                        if (ReadOnlyBlockBase.IsSupportedDataColumnType(param.ParameterType))
                        {
                            return new ConstructorParameterMapping(matchingProp, null);
                        }
                        else
                        {
                            return new ConstructorParameterMapping(
                                matchingProp,
                                ConstructorMapping.FromConstructor(param.ParameterType));
                        }
                    })
                    .ToImmutableArray();

                return columnMappings;
            }

            private static PropertyInfo GetMatchingProperty(ParameterInfo param)
            {
                var parentType = param.Member.DeclaringType!;

                if (string.IsNullOrWhiteSpace(param.Name))
                {
                    throw new InvalidOperationException(
                        "Record constructor parameter must have a name");
                }

                var matchingProp = parentType.GetProperty(param.Name);

                if (matchingProp == null)
                {
                    throw new InvalidOperationException(
                        $"Constructor parameter '{param.Name}' does not have a matching property");
                }
                if (matchingProp.PropertyType != param.ParameterType)
                {
                    throw new InvalidOperationException(
                        $"Constructor parameter '{param.Name}' is type '{param.ParameterType}' " +
                        $"while matching property is type '{matchingProp.PropertyType}'");
                }
                if (matchingProp.GetGetMethod() == null)
                {
                    throw new InvalidOperationException(
                        $"Constructor parameter '{param.Name}' matching property can't be read");
                }

                return matchingProp;
            }
        }
        #endregion

        private readonly ConstructorMapping _mainConstructorMapping;
        private readonly IImmutableDictionary<Type, ConstructorMapping> _constructorMappingByType;
        private readonly IImmutableDictionary<string, IImmutableList<int>> _propertyPathToColumnIndexesMap;

        #region Constructor
        /// <summary>
        /// Uses <typeparamref name="T"/>'s constructor for table's columns.
        /// Columns are in the order of the parameters in the constructors.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        /// <exception cref="ArgumentException"></exception>
        public static TypedTableSchema<T> FromConstructor(string tableName)
        {
            var constructorMapping = ConstructorMapping.FromConstructor(typeof(T));

            return new TypedTableSchema<T>(
                tableName,
                ImmutableArray<int>.Empty,
                ImmutableArray<int>.Empty,
                ImmutableArray<TableTriggerAction>.Empty,
                constructorMapping,
                constructorMapping.GetIndexedMappings(),
                constructorMapping.GetPropertyPathToColumnIndexesMap());
        }

        private TypedTableSchema(
            string tableName,
            IImmutableList<int> primaryKeyColumnIndexes,
            IImmutableList<int> partitionKeyColumnIndexes,
            IImmutableList<TableTriggerAction> triggerActions,
            ConstructorMapping mainConstructorMapping,
            IImmutableDictionary<Type, ConstructorMapping> constructorMappingByType,
            IImmutableDictionary<string, IImmutableList<int>> propertyPathToColumnIndexesMap)
            : base(
                  tableName,
                  mainConstructorMapping.ColumnSchemas,
                  primaryKeyColumnIndexes,
                  partitionKeyColumnIndexes,
                  triggerActions)
        {
            _mainConstructorMapping = mainConstructorMapping;
            _constructorMappingByType = constructorMappingByType;
            _propertyPathToColumnIndexesMap = propertyPathToColumnIndexesMap;
        }
        #endregion

        /// <summary>Add a primary key column mapped to a property.</summary>
        /// <typeparam name="PT"></typeparam>
        /// <param name="propertyExtractor"></param>
        /// <returns></returns>
        public TypedTableSchema<T> AddPrimaryKeyProperty<PT>(
            Expression<Func<T, PT>> propertyExtractor)
        {
            var columnIndexSubset = GetColumnIndexSubset(propertyExtractor);
            var newPrimaryKeyColumnIndexes = PrimaryKeyColumnIndexes
                .Concat(columnIndexSubset)
                .OrderBy(i => i)
                .ToImmutableArray();

            ValidateNonRepeatingIndexes(newPrimaryKeyColumnIndexes);

            return new TypedTableSchema<T>(
                TableName,
                newPrimaryKeyColumnIndexes,
                PartitionKeyColumnIndexes,
                TriggerActions,
                _mainConstructorMapping,
                _constructorMappingByType,
                _propertyPathToColumnIndexesMap);
        }

        /// <summary>
        /// Add a partition key column mapped to a property.
        /// </summary>
        /// <typeparam name="PT"></typeparam>
        /// <param name="propertyExtractor"></param>
        /// <returns></returns>
        public TypedTableSchema<T> AddPartitionKeyProperty<PT>(
            Expression<Func<T, PT>> propertyExtractor)
        {
            var columnIndexSubset = GetColumnIndexSubset(propertyExtractor);
            var newPartitionKeyColumnIndexes = PartitionKeyColumnIndexes
                .Concat(columnIndexSubset)
                .OrderBy(i => i)
                .ToImmutableArray();

            ValidateNonRepeatingIndexes(newPartitionKeyColumnIndexes);

            return new TypedTableSchema<T>(
                TableName,
                PrimaryKeyColumnIndexes,
                newPartitionKeyColumnIndexes,
                TriggerActions,
                _mainConstructorMapping,
                _constructorMappingByType,
                _propertyPathToColumnIndexesMap);
        }

        public TypedTableSchema<T> AddTrigger(
            Action<DatabaseContextBase, TransactionContext> triggerAction)
        {
            throw new NotImplementedException();
        }

        internal ReadOnlySpan<object?> FromObjectToColumns(T record)
        {
            return _mainConstructorMapping.ObjectToColumns(record!);
        }

        internal T FromColumnsToObject(ReadOnlySpan<object?> columns)
        {
            return (T)_mainConstructorMapping.ColumnsToObject(columns);
        }

        internal ReadOnlySpan<object?> FromPropertyValueToColumns(object propertyValue)
        {
            return _constructorMappingByType[propertyValue.GetType()]
                .ObjectToColumns(propertyValue);
        }

        internal IImmutableList<int> GetColumnIndexSubset<U>(Expression<Func<T, U>> propertySelection)
        {
            return GetColumnIndexSubset(propertySelection.Body);
        }

        internal IImmutableList<int> GetColumnIndexSubset(Expression expression)
        {
            string GetPropertyPath(Expression expression, string? suffix)
            {
                if (expression is LambdaExpression le)
                {
                    expression = le.Body;
                }
                if (expression is ParameterExpression)
                {
                    return suffix ?? string.Empty;
                }
                else if (expression is MemberExpression me)
                {
                    if (me.Member is PropertyInfo pi)
                    {
                        var newSuffix = suffix == null ? pi.Name : $"{pi.Name}.{suffix}";

                        return GetPropertyPath(me.Expression!, newSuffix);
                    }
                    else
                    {
                        throw new NotSupportedException($"MemberInfo '{me.GetType().Name}'");
                    }
                }
                else
                {
                    throw new NotSupportedException($"Expression '{expression}'");
                }
            }

            var path = GetPropertyPath(expression, null);

            if (_propertyPathToColumnIndexesMap.TryGetValue(path, out var subset))
            {
                return subset;
            }
            else
            {
                throw new ArgumentOutOfRangeException(
                    $"Property path '{path}' doesn't map to a subset of columns");
            }
        }

        private void ValidateNonRepeatingIndexes(IEnumerable<int> columnIndexes)
        {
            var hasRepeatedIndex = columnIndexes
                .GroupBy(i => i)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .Any();

            if (hasRepeatedIndex)
            {
                throw new ArgumentException("Repeated indexes", nameof(columnIndexes));
            }
        }
    }
}