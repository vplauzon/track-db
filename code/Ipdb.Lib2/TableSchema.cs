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

        #region Constructor
        protected TableSchema(string tableName, IImmutableList<string> partitionKeyPropertyPaths)
        {
            TableName = tableName;
            PartitionKeyPropertyPaths = partitionKeyPropertyPaths;
            Columns = ColumnSchema.Reflect(RepresentationType);
            _propertyPathToIndexMap = Enumerable.Range(0, Columns.Count)
                .ToImmutableDictionary(i => Columns[i].PropertyPath, i => i);

            var unsupportedColumns = Columns.Where(
                c => !SUPPORTED_COLUMN_TYPES.Contains(c.ColumnType));

            if (unsupportedColumns.Any())
            {
                var unsupportedColumn = Columns.First();

                throw new NotSupportedException(
                    $"Column '{unsupportedColumn.PropertyPath}' has unsupported " +
                    $"type '{unsupportedColumn.ColumnType}'");
            }

            var methodInfos = Columns
                .Select(c => GetPropertyRead(RepresentationType, c.PropertyPath))
                .ToImmutableArray();

            _objectToColumnsAction = (record, columns) =>
            {
                if (columns.Length != methodInfos.Length)
                {
                    throw new ArgumentException(
                        $"'{nameof(columns)}' has length {columns.Length} while the expected" +
                        $" number of columns is {methodInfos.Length}");
                }
                for (var i = 0; i != methodInfos.Length; i++)
                {
                    columns[i] = methodInfos[i].Invoke(record, null);
                }
            };
        }

        private static MethodInfo GetPropertyRead(
            Type representationType,
            string propertyPath)
        {
            var propertyInfo = representationType.GetProperty(propertyPath);

            if (propertyInfo == null)
            {
                throw new InvalidOperationException(
                    $"Type '{representationType.Name}' doesn't have property '{propertyPath}'");
            }

            var methodInfo = propertyInfo.GetGetMethod();

            if (methodInfo == null)
            {
                throw new InvalidOperationException(
                    $"Type '{representationType.Name}' doesn't have get-method on property " +
                    $"'{propertyPath}'");
            }

            return methodInfo;
        }
        #endregion

        public string TableName { get; }

        public abstract Type RepresentationType { get; }

        public IImmutableList<string> PartitionKeyPropertyPaths { get; }

        internal IImmutableList<ColumnSchema> Columns { get; }

        internal bool TryGetColumn(string propertyPath, out ColumnSchema columnSchema)
        {
            if(_propertyPathToIndexMap.TryGetValue(propertyPath, out var columnIndex))
            {
                columnSchema = Columns[columnIndex];

                return true;
            }
            else
            {
                return false;
            }
        }

        internal void FromObjectToColumns(object record, object?[] columns)
        {
            _objectToColumnsAction(record, columns);
        }

        internal void FromColumnsToObject(object?[] columns, object record)
        {
            throw new NotImplementedException();
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