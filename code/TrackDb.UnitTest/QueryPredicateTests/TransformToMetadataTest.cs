using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TrackDb.Lib;
using TrackDb.Lib.Predicate;
using Xunit;

namespace TrackDb.UnitTest.QueryPredicateTests
{
    public class TransformToMetadataTest
    {
        #region Inner Types
        private record MyEntity(string Name, int Age);
        #endregion

        [Fact]
        public void Equality()
        {
            var schema = TypedTableSchema<MyEntity>.FromConstructor("MyEntity");
            var metaSchema = schema.CreateMetadataTableSchema();
            var metaMetaSchema = metaSchema.CreateMetadataTableSchema();
            var predicate = new BinaryOperatorPredicate(
                schema.ColumnProperties.Index()
                .Where(c => c.Item.ColumnSchema.ColumnName==nameof(MyEntity.Name))
                .First()
                .Index,
                "John",
                BinaryOperator.Equal);
            var metaPredicate = MetaPredicateHelper.GetCorrespondantPredicate(
                predicate,
                schema,
                metaSchema);
            var metaMetaPredicate = MetaPredicateHelper.GetCorrespondantPredicate(
                metaPredicate,
                metaSchema,
                metaMetaSchema);

            Assert.Equal(2, metaPredicate.ReferencedColumnIndexes.Count());
            Assert.Equal(2, metaMetaPredicate.ReferencedColumnIndexes.Count());
        }

        [Fact]
        public void In()
        {
            var schema = TypedTableSchema<MyEntity>.FromConstructor("MyEntity");
            var metaSchema = schema.CreateMetadataTableSchema();
            var metaMetaSchema = metaSchema.CreateMetadataTableSchema();
            var predicate = new InPredicate(
                schema.ColumnProperties.Index()
                .Where(c => c.Item.ColumnSchema.ColumnName == nameof(MyEntity.Name))
                .First()
                .Index,
                ["John", "Bob", "Peter"]);
            var metaPredicate = MetaPredicateHelper.GetCorrespondantPredicate(
                predicate,
                schema,
                metaSchema);
            var metaMetaPredicate = MetaPredicateHelper.GetCorrespondantPredicate(
                metaPredicate,
                metaSchema,
                metaMetaSchema);

            Assert.Equal(2, metaPredicate.ReferencedColumnIndexes.Count());
            Assert.Equal(2, metaMetaPredicate.ReferencedColumnIndexes.Count());
        }
    }
}
