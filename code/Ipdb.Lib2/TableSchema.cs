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
    /// <summary>Schema of a table.</summary>
    public abstract class TableSchema
    {
        private static readonly IImmutableSet<Type> SUPPORTED_COLUMN_TYPES =
            [typeof(int)];

        private readonly IImmutableDictionary<string, int> _propertyPathToIndexMap;
        private readonly Action<object, object?[]> _objectToColumnsAction;
        private readonly Func<object?[], object> _columnsToObjectFunc;

        #region Constructor
        protected TableSchema(string tableName, IImmutableList<string> partitionKeyPropertyPaths)
        {
            var maxConstructorParams = RepresentationType.GetConstructors()
                .Max(c => c.GetParameters().Count());
            var argMaxConstructor = RepresentationType.GetConstructors()
                .First(c => c.GetParameters().Count() == maxConstructorParams);
            var columnSchemas = argMaxConstructor.GetParameters().Select(param =>
            {
                if (param.Name == null)
                {
                    throw new InvalidOperationException(
                        "Record constructor parameter must have a name");
                }

                var matchingProp = RepresentationType.GetProperty(param.Name);

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
                        PropertyPath: param.Name,
                        ColumnType: param.ParameterType),
                    Property = matchingProp
                };
            })
                .ToImmutableArray();
            var unsupportedColumns = columnSchemas.Where(
                c => !SUPPORTED_COLUMN_TYPES.Contains(c.Schema.ColumnType));

            if (unsupportedColumns.Any())
            {
                var unsupportedColumn = unsupportedColumns.First();

                throw new NotSupportedException(
                    $"Column '{unsupportedColumn.Schema.PropertyPath}' has unsupported " +
                    $"type '{unsupportedColumn.Schema.ColumnType}'");
            }
            TableName = tableName;
            PartitionKeyPropertyPaths = partitionKeyPropertyPaths;
            Columns = columnSchemas
                .Select(c => c.Schema)
                .ToImmutableArray();
            _propertyPathToIndexMap = Enumerable.Range(0, Columns.Count)
                .ToImmutableDictionary(i => Columns[i].PropertyPath, i => i);
            _objectToColumnsAction = (record, columns) =>
            {
                if (columns.Length != columnSchemas.Length)
                {
                    throw new ArgumentException(
                        $"'{nameof(columns)}' has length {columns.Length} while the expected" +
                        $" number of columns is {columnSchemas.Length}");
                }
                for (var i = 0; i != columnSchemas.Length; i++)
                {
                    columns[i] = columnSchemas[i].Property.GetGetMethod()!.Invoke(record, null);
                }
            };
            _columnsToObjectFunc = (columns) =>
            {
                if (columns.Length != columnSchemas.Length)
                {
                    throw new ArgumentException(
                        $"'{nameof(columns)}' has length {columns.Length} while the expected" +
                        $" number of columns is {columnSchemas.Length}");
                }

                return argMaxConstructor.Invoke(columns);
            };
        }
        #endregion

        public string TableName { get; }

        public abstract Type RepresentationType { get; }

        public IImmutableList<string> PartitionKeyPropertyPaths { get; }

        internal IImmutableList<ColumnSchema> Columns { get; }

        internal bool TryGetColumnIndex(string propertyPath, out int columnIndex)
        {
            return _propertyPathToIndexMap.TryGetValue(propertyPath, out columnIndex);
        }

        internal void FromObjectToColumns(object record, object?[] columns)
        {
            _objectToColumnsAction(record, columns);
        }

        internal object FromColumnsToObject(object?[] columns)
        {
            return _columnsToObjectFunc(columns);
        }
    }

    /// <summary>
    /// Schema of a table:  table name, the .NET type representing it and,
    /// optionally, primary keys.
    /// If primary key extractors are present, they point to one or many
    /// properties within the representation types which values act
    /// as a primary key.
    /// If a record with the same primary exists in the table, the record is updated otherwise
    /// inserted (update semantic).  If no primary key is defined, records are always inserted
    /// (can be explicitly deleted).
    /// Column type supported:  <see cref="int"/>.
    /// </summary>
    /// <typeparam name="T">Representation Type</typeparam>
    public class TableSchema<T> : TableSchema
    {
        #region Constructors
        /// <summary>Create a table schema.</summary>>
        /// <param name="tableName"></param>
        public TableSchema(string tableName)
            : this(tableName, ImmutableArray<string>.Empty)
        {
        }

        private TableSchema(string tableName, IImmutableList<string> primaryKeys)
            : base(tableName, primaryKeys)
        {
        }
        #endregion

        public override Type RepresentationType => typeof(T);

        /// <summary>
        /// Add a partition key column mapped to a property.
        /// </summary>
        /// <typeparam name="PT"></typeparam>
        /// <param name="propertyExtractor"></param>
        /// <returns></returns>
        public TableSchema<T> AddPartitionKeyProperty<PT>(
            Expression<Func<T, PT>> propertyExtractor)
        {
            if (propertyExtractor.Body is MemberExpression me)
            {
                if (me.Member.MemberType == MemberTypes.Property)
                {
                    var propertyName = me.Member.Name;

                    return new TableSchema<T>(
                        TableName,
                        PartitionKeyPropertyPaths.Add(propertyName));
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
    }
}
