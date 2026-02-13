using TrackDb.Lib;
using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.UnitTest.QueryPredicateTests
{
    public class BinaryOperationTest
    {
        private record IntegerOnly(int Value);

        [Fact]
        public void Integer()
        {
            var schema = TypedTableSchema<IntegerOnly>.FromConstructor("MyTable");
            var factory = new QueryPredicateFactory<IntegerOnly>(schema);
            var predicateEqual = factory.Equal(i => i.Value, 5);
            var predicateNotEqual = factory.NotEqual(i => i.Value, 5);
            var predicateLessThan = factory.LessThan(i => i.Value, 5);
            var predicateLessThanEqual = factory.LessThanOrEqual(i => i.Value, 5);
            var predicateGreaterThan = factory.GreaterThan(i => i.Value, 5);
            var predicateGreaterThanEqual = factory.GreaterThanOrEqual(i => i.Value, 5);
            var testingPrimitivePairs = new[]
            {
                (predicateEqual, BinaryOperator.Equal),
                (predicateNotEqual, BinaryOperator.NotEqual),
                (predicateLessThan, BinaryOperator.LessThan),
                (predicateLessThanEqual, BinaryOperator.LessThanOrEqual),
                (predicateGreaterThan, BinaryOperator.GreaterThan),
                (predicateGreaterThanEqual, BinaryOperator.GreaterThanOrEqual),
            };

            foreach (var testingPair in testingPrimitivePairs)
            {
                var typedPredicate = testingPair.Item1;
                var predicate = typedPredicate.QueryPredicate;
                var simplifiedPredicate = predicate.Simplify() ?? predicate;
                var binaryOperator = testingPair.Item2;

                Assert.IsType<TypedQueryPredicate<IntegerOnly>>(typedPredicate);
                Assert.IsType<BinaryOperatorPredicate>(simplifiedPredicate);

                var binaryOperatorPredicate = (BinaryOperatorPredicate)simplifiedPredicate;

                Assert.Equal(0, binaryOperatorPredicate.ColumnIndex);
                Assert.Equal(binaryOperator, binaryOperatorPredicate.BinaryOperator);
                Assert.Equal(5, binaryOperatorPredicate.Value);
            }
        }
    }
}