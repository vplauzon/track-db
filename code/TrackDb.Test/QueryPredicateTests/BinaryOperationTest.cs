using TrackDb.Lib;
using TrackDb.Lib.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.Tests.QueryPredicateTests
{
    public class BinaryOperationTest
    {
        private record IntegerOnly(int Value);

        [Fact]
        public void IntegerConstant()
        {
            var schema = new TableSchema(
                "MyTable",
                [new ColumnSchema(nameof(IntegerOnly.Value), typeof(int))],
                []);
            var predicateEqual =
                QueryPredicateFactory.Create((IntegerOnly i) => i.Value == 5, schema);
            var predicateNotEqual =
                QueryPredicateFactory.Create((IntegerOnly i) => i.Value != 5, schema);
            var predicateLessThan =
                QueryPredicateFactory.Create((IntegerOnly i) => i.Value < 5, schema);
            var predicateLessThanEqual =
                QueryPredicateFactory.Create((IntegerOnly i) => i.Value <= 5, schema);
            var predicateGreaterThan =
                QueryPredicateFactory.Create((IntegerOnly i) => i.Value > 5, schema);
            var predicateGreaterThanEqual =
                QueryPredicateFactory.Create((IntegerOnly i) => i.Value >= 5, schema);
            var testingPairs = new[]
            {
                (predicateEqual, BinaryOperator.Equal),
                (predicateNotEqual, BinaryOperator.NotEqual),
                (predicateLessThan, BinaryOperator.LessThan),
                (predicateLessThanEqual, BinaryOperator.LessThanOrEqual),
                (predicateGreaterThan, BinaryOperator.GreaterThan),
                (predicateGreaterThanEqual, BinaryOperator.GreaterThanOrEqual),
            };

            foreach (var testingPair in testingPairs)
            {
                var predicate = testingPair.Item1;
                var binaryOperator = testingPair.Item2;

                Assert.IsType<BinaryOperatorPredicate>(predicate);

                var binaryOperatorPredicate = (BinaryOperatorPredicate)predicate;

                Assert.Equal(0, binaryOperatorPredicate.ColumnIndex);
                Assert.Equal(binaryOperator, binaryOperatorPredicate.BinaryOperator);
                Assert.Equal(5, binaryOperatorPredicate.Value);
            }
        }

        [Fact]
        public void IntegerVariable()
        {
            var schema = new TableSchema(
                "MyTable",
                [new ColumnSchema(nameof(IntegerOnly.Value), typeof(int))],
                []);

            for (var i = 14; i != 15; ++i)
            {
                var predicateEqual =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value == 5, schema);
                var predicateNotEqual =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value != 5, schema);
                var predicateLessThan =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value < 5, schema);
                var predicateLessThanEqual =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value <= 5, schema);
                var predicateGreaterThan =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value > 5, schema);
                var predicateGreaterThanEqual =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value >= 5, schema);
                var testingPairs = new[]
                {
                    (predicateEqual, BinaryOperator.Equal),
                    (predicateNotEqual, BinaryOperator.NotEqual),
                    (predicateLessThan, BinaryOperator.LessThan),
                    (predicateLessThanEqual, BinaryOperator.LessThanOrEqual),
                    (predicateGreaterThan, BinaryOperator.GreaterThan),
                    (predicateGreaterThanEqual, BinaryOperator.GreaterThanOrEqual),
                };

                foreach (var testingPair in testingPairs)
                {
                    var predicate = testingPair.Item1;
                    var binaryOperator = testingPair.Item2;

                    Assert.IsType<BinaryOperatorPredicate>(predicate);

                    var propertyPredicate = (BinaryOperatorPredicate)predicate;

                    Assert.Equal(0, propertyPredicate.ColumnIndex);
                    Assert.Equal(binaryOperator, propertyPredicate.BinaryOperator);
                    Assert.Equal(5, propertyPredicate.Value);
                }
            }
        }

        [Fact]
        public void IntegerAnd()
        {
            var schema = new TableSchema(
                "MyTable",
                [new ColumnSchema(nameof(IntegerOnly.Value), typeof(int))],
                []);
            var predicate = QueryPredicateFactory.Create(
                (IntegerOnly i) => (i.Value > 5) && (i.Value < 12),
                schema);

            Assert.IsType<BinaryOperatorPredicate>(predicate);

            var binaryOperatorPredicate = (BinaryOperatorPredicate)predicate;

            //Assert.Equal(0, binaryOperatorPredicate.ColumnIndex);
            //Assert.Equal(binaryOperator, binaryOperatorPredicate.BinaryOperator);
            //Assert.Equal(5, binaryOperatorPredicate.Value);
        }
    }
}