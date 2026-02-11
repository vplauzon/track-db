using System;
using System.Collections.Generic;
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
            var schema = TypedTableSchema<MyEntity>.FromConstructor("name");
            var metaSchema = schema.CreateMetadataTableSchema();
            var metaMetaSchema = metaSchema.CreateMetadataTableSchema();
            var correspondances = metaSchema.GetColumnCorrespondances();
            var metaCorrespondances = metaMetaSchema.GetColumnCorrespondances();
        }
    }
}
