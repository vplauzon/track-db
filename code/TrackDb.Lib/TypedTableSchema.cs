using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2
{
    /// <summary>Schema of a table including data mapping to a .NET type.</summary>
    public class TypedTableSchema<T> : TableSchema
    {
        private readonly Action<T, Span<object?>> _objectToColumnsAction;
        private readonly Func<object?[], T> _columnsToObjectFunc;
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
            var maxConstructorParams = typeof(T).GetConstructors()
                .Max(c => c.GetParameters().Count());
            var argMaxConstructor = typeof(T).GetConstructors()
                .First(c => c.GetParameters().Count() == maxConstructorParams);
            var parameters = argMaxConstructor.GetParameters().Select(param =>
            {
                if (param.Name == null)
                {
                    throw new InvalidOperationException(
                        "Record constructor parameter must have a name");
                }

                var matchingProp = typeof(T).GetProperty(param.Name);

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

                return new
                {
                    Schema = new ColumnSchema(
                        ColumnName: param.Name,
                        ColumnType: param.ParameterType),
                    Property = matchingProp
                };
            })
                .ToImmutableArray();
            var columns = parameters
                .Select(c => c.Schema);
            Action<T, Span<object?>> objectToColumnsAction = (record, columns) =>
            {
                if (columns.Length != parameters.Length)
                {
                    throw new ArgumentException(
                        $"'{nameof(columns)}' has length {columns.Length} while the expected" +
                        $" number of columns is {parameters.Length}");
                }
                for (var i = 0; i != parameters.Length; i++)
                {
                    columns[i] = parameters[i].Property.GetGetMethod()!.Invoke(record, null);
                }
            };
            Func<object?[], T> columnsToObjectFunc = (columns) =>
            {
                if (columns.Length != parameters.Length)
                {
                    throw new ArgumentException(
                        $"'{nameof(columns)}' has length {columns.Length} while the expected" +
                        $" number of columns is {parameters.Length}");
                }

                return (T)argMaxConstructor.Invoke(columns);
            };

            return new TypedTableSchema<T>(
                tableName,
                columns,
                Array.Empty<int>(),
                objectToColumnsAction,
                columnsToObjectFunc);
        }

        private TypedTableSchema(
            string tableName,
            IEnumerable<ColumnSchema> columns,
            IEnumerable<int> partitionKeyColumnIndexes,
            Action<T, Span<object?>> objectToColumnsAction,
            Func<object?[], T> columnsToObjectFunc)
            : base(tableName, columns, partitionKeyColumnIndexes)
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
                        Columns,
                        PartitionKeyColumnIndexes.Append(columnIndex),
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
            return _columnsToObjectFunc(columns);
        }
    }
}
