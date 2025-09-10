using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
        private record EntityMapping(
            IEnumerable<ColumnSchema> ColumnSchemas,
            Action<T, Span<object?>> ObjectToColumnsAction,
            Func<object?[], object> ColumnsToObjectFunc);

        private record ColumnMapping(
            ColumnSchema ColumnSchema,
            Func<object, object?> ColumnValueFunc);
        #endregion

        private readonly Action<T, Span<object?>> _objectToColumnsAction;
        private readonly Func<object?[], object> _columnsToObjectFunc;
        private readonly object?[] _columnDataBuffer;

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
            var entityMapping = GetMappingFromConstructor(typeof(T));

            return new TypedTableSchema<T>(
                tableName,
                Array.Empty<int>(),
                entityMapping.ColumnSchemas,
                entityMapping.ObjectToColumnsAction,
                entityMapping.ColumnsToObjectFunc);
        }

        private static EntityMapping GetMappingFromConstructor(Type type)
        {
            var maxConstructorParams = type.GetConstructors()
                .Max(c => c.GetParameters().Count());
            var argMaxConstructor = type.GetConstructors()
                .First(c => c.GetParameters().Count() == maxConstructorParams);
            var columnMappings = argMaxConstructor.GetParameters()
                .Select(param =>
                {
                    var matchingProp = ValidateConstructorParameter(type, param);

                    if (ReadOnlyBlockBase.IsSupportedDataColumnType(param.ParameterType))
                    {
                        return new[]
                        {
                            new ColumnMapping(
                                new ColumnSchema(param.Name!, param.ParameterType),
                                (record) => matchingProp.GetGetMethod()!.Invoke(record, null))
                        };
                    }
                    else
                    {
                        var entityMapping = GetMappingFromConstructor(param.ParameterType);

                        throw new NotSupportedException(
                            $"Column type '{param.ParameterType}' on column '{param.Name}'");
                    }
                })
                .SelectMany(a => a)
                .ToImmutableArray();

            return new EntityMapping(
                columnMappings.Select(m => m.ColumnSchema),
                (record, columns) =>
                {
                    if (columns.Length != columnMappings.Length)
                    {
                        throw new ArgumentException(
                            $"'{nameof(columns)}' has length {columns.Length} while the expected" +
                            $" number of columns is {columnMappings.Length}");
                    }
                    for (var i = 0; i != columns.Length; i++)
                    {
                        columns[i] = columnMappings[i].ColumnValueFunc(record!);
                    }
                },
                (input) =>
                {
                    if (input.Length != columnMappings.Length)
                    {
                        throw new ArgumentException(
                            $"'{nameof(input)}' has length {input.Length} while the expected" +
                            $" number of columns is {columnMappings.Length}");
                    }

                    return (T)argMaxConstructor.Invoke(input);
                });
        }

        private static PropertyInfo ValidateConstructorParameter(Type type, ParameterInfo param)
        {
            if (string.IsNullOrWhiteSpace(param.Name))
            {
                throw new InvalidOperationException(
                    "Record constructor parameter must have a name");
            }

            var matchingProp = type.GetProperty(param.Name);

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

        private TypedTableSchema(
            string tableName,
            IEnumerable<int> partitionKeyColumnIndexes,
            IEnumerable<ColumnSchema> columnSchemas,
            Action<T, Span<object?>> objectToColumnsAction,
            Func<object?[], object> columnsToObjectFunc)
            : base(tableName, columnSchemas, partitionKeyColumnIndexes)
        {
            _objectToColumnsAction = objectToColumnsAction;
            _columnsToObjectFunc = columnsToObjectFunc;
            _columnDataBuffer = new object[Columns.Count];
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
                        PartitionKeyColumnIndexes.Append(columnIndex),
                        Columns,
                        _objectToColumnsAction,
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
            _objectToColumnsAction(record, _columnDataBuffer);

            return _columnDataBuffer;
        }

        internal T FromColumnsToObject(object?[] columns)
        {
            return (T)_columnsToObjectFunc(columns);
        }
    }
}
