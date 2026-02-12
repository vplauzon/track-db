using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.UnitTest.QueryPredicateTests
{
    public class SchemaCorrespondanceTest
    {
        #region Inner Types
        private record MyEntity(string Name, int Age);
        #endregion

        [Fact]
        public void Correspondance()
        {
            var schema = TypedTableSchema<MyEntity>.FromConstructor("MyEntity");
            var metaSchema = schema.CreateMetadataTableSchema();
            var metaMetaSchema = metaSchema.CreateMetadataTableSchema();
            var correspondances = metaSchema.GetColumnCorrespondances();
            var metaCorrespondances = metaMetaSchema.GetColumnCorrespondances();
            var nameIndex = schema.ColumnProperties
                .Index()
                .Where(p => p.Item.ColumnSchema.ColumnName == nameof(MyEntity.Name))
                .Select(p => p.Index)
                .First();
            var correspondance = correspondances
                .Where(c => c.ColumnIndex == nameIndex)
                .First();

            Assert.Null(correspondance.MetaColumnIndex);
            Assert.NotNull(correspondance.MetaMinColumnIndex);
            Assert.NotNull(correspondance.MetaMaxColumnIndex);

            var metaCorrespondance = metaCorrespondances
                .Where(c => c.ColumnIndex == correspondance.MetaMinColumnIndex)
                .First();

            Assert.NotNull(metaCorrespondance.MetaColumnIndex);
            Assert.Null(metaCorrespondance.MetaMinColumnIndex);
            Assert.Null(metaCorrespondance.MetaMaxColumnIndex);
        }
    }
}
