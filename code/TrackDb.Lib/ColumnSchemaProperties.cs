using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal record ColumnSchemaProperties(
        ColumnSchema ColumnSchema,
        ColumnSchemaStat ColumnSchemaStat,
        string AncestorZeroColumnName,
        int Generation)
    {
        /// <summary>Construct a generation-zero column.</summary>
        /// <param name="ColumnSchema"></param>
        public static ColumnSchemaProperties CreateGenerationZero(ColumnSchema ColumnSchema)
        {
            return new ColumnSchemaProperties(
                ColumnSchema,
                ColumnSchemaStat.Data,
                ColumnSchema.ColumnName,
                0);
        }

        /// <summary>Construct a generation-one column.</summary>
        /// <param name="Parent"></param>
        /// <param name="ColumnSchemaStat"></param>
        public static ColumnSchemaProperties CreateGenerationOne(
            ColumnSchemaProperties Parent,
            ColumnSchemaStat ColumnSchemaStat)
        {
            return new ColumnSchemaProperties(
                new ColumnSchema(
                    GetColumnName(Parent.AncestorZeroColumnName, Parent.Generation, ColumnSchemaStat),
                    Parent.ColumnSchema.ColumnType),
                ColumnSchemaStat,
                Parent.AncestorZeroColumnName,
                Parent.Generation + 1);
        }

        /// <summary>Construct a generation-two ( or+) column.</summary>
        /// <param name="Parent"></param>
        public static ColumnSchemaProperties CreateGenerationTwo(ColumnSchemaProperties Parent)
        {
            return new ColumnSchemaProperties(
                new ColumnSchema(
                    GetColumnName(
                        Parent.AncestorZeroColumnName,
                        Parent.Generation,
                        Parent.ColumnSchemaStat),
                    Parent.ColumnSchema.ColumnType),
                Parent.ColumnSchemaStat,
                Parent.AncestorZeroColumnName,
                Parent.Generation + 1);
        }

        private static string GetColumnName(
            string ancestorZeroColumnName,
            int parentGeneration,
            ColumnSchemaStat columnSchemaStat)
        {
            switch (columnSchemaStat)
            {
                case ColumnSchemaStat.Min:
                    return $"$min-{ancestorZeroColumnName}-{parentGeneration + 1}";
                case ColumnSchemaStat.Max:
                    return $"$max-{ancestorZeroColumnName}-{parentGeneration + 1}";
                default:
                    throw new NotSupportedException(
                          $"{nameof(ColumnSchemaStat)} value {columnSchemaStat}");
            }
        }
    }
}