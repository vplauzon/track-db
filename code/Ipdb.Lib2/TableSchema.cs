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
        protected TableSchema(string tableName, IImmutableList<string> primaryKeys)
        {
            TableName = tableName;
            PrimaryKeys = primaryKeys;
            Columns = ColumnSchema.Reflect(RepresentationType);
        }

        public string TableName { get; }

        public abstract Type RepresentationType { get; }

        public IImmutableList<string> PrimaryKeys { get; }

        internal IImmutableList<ColumnSchema> Columns { get; }
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
        /// Add a primary key mapped to a property.
        /// </summary>
        /// <typeparam name="PT"></typeparam>
        /// <param name="primaryKeyExtractor"></param>
        /// <returns></returns>
        public TableSchema<T> AddPrimaryKey<PT>(Expression<Func<T, PT>> primaryKeyExtractor)
        {
            if (primaryKeyExtractor.Body is MemberExpression me)
            {
                if (me.Member.MemberType == MemberTypes.Property)
                {
                    var propertyName = me.Member.Name;

                    return new TableSchema<T>(TableName, PrimaryKeys.Add(propertyName));
                }
                else
                {
                    throw new NotSupportedException(
                        $"Primary key expression only supports properties:  '{primaryKeyExtractor}'");
                }
            }
            else
            {
                throw new NotSupportedException(
                    $"Primary key expression not supported:  '{primaryKeyExtractor}'");
            }
        }
    }
}