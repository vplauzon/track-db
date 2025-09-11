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

            public ReadOnlySpan<object?> ObjectToColumns(T record)
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
                                columns.Slice(0, columnCount));
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

        private readonly Func<T, ReadOnlySpan<object?>> _objectToColumnsFunc;
        private readonly Func<ReadOnlySpan<object?>, object> _columnsToObjectFunc;

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
            var columnSchemas = constructorMapping.ColumnSchemas
                .ToImmutableArray();
            var objectToColumnsFunc = constructorMapping.ObjectToColumns;
            var columnsToObjectFunc = constructorMapping.ColumnsToObject;

            return new TypedTableSchema<T>(
                tableName,
                ImmutableArray<int>.Empty,
                columnSchemas,
                objectToColumnsFunc,
                columnsToObjectFunc);
        }

        private TypedTableSchema(
            string tableName,
            IImmutableList<int> partitionKeyColumnIndexes,
            IImmutableList<ColumnSchema> columnSchemas,
            Func<T, ReadOnlySpan<object?>> objectToColumnsFunc,
            Func<ReadOnlySpan<object?>, object> columnsToObjectFunc)
            : base(tableName, columnSchemas, partitionKeyColumnIndexes)
        {
            _objectToColumnsFunc = objectToColumnsFunc;
            _columnsToObjectFunc = columnsToObjectFunc;
        }
        #endregion

        /// <summary>
        /// Add a partition key column mapped to a property.
        /// </summary>
        /// <typeparam name="PT"></typeparam>
        /// <param name="propertyExtractor"></param>
        /// <returns></returns>
        public TypedTableSchema<T> AddPartitionKeyProperty<PT>(
            Expression<Func<T, PT>> propertyExtractor)
        {
            if (propertyExtractor.Body is MemberExpression me)
            {
                if (me.Member.MemberType == MemberTypes.Property)
                {
                    var propertyName = me.Member.Name;
                    //  Is it from the input object?

                    if (!TryGetColumnIndex(propertyName, out var columnIndex))
                    {
                        throw new ArgumentException(
                            $"Property '{propertyName}' isn't mapped to a column",
                            nameof(propertyExtractor));
                    }

                    return new TypedTableSchema<T>(
                        TableName,
                        PartitionKeyColumnIndexes.Append(columnIndex).ToImmutableArray(),
                        Columns,
                        _objectToColumnsFunc,
                        _columnsToObjectFunc);
                }
                else
                {
                    throw new NotSupportedException(
                        $"Primary key expression only supports properties:  '{propertyExtractor}'");
                }
            }
            else
            {
                throw new NotSupportedException(
                    $"Primary key expression not supported:  '{propertyExtractor}'");
            }
        }

        internal ReadOnlySpan<object?> FromObjectToColumns(T record)
        {
            return _objectToColumnsFunc(record);
        }

        internal T FromColumnsToObject(ReadOnlySpan<object?> columns)
        {
            return (T)_columnsToObjectFunc(columns);
        }
    }
}