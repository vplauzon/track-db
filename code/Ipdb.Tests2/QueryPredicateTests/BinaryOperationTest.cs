using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2.QueryPredicateTests
{
    public class OnePropertyTest
    {
        private record IntegerOnly(int Value);

        [Fact]
        public void IntegerConstant()
        {
            var predicateEqual = QueryPredicateFactory.Create((IntegerOnly i) => i.Value == 5);
            var predicateNotEqual = QueryPredicateFactory.Create((IntegerOnly i) => i.Value != 5);
            var predicateLessThan = QueryPredicateFactory.Create((IntegerOnly i) => i.Value < 5);
            var predicateLessThanEqual =
                QueryPredicateFactory.Create((IntegerOnly i) => i.Value <= 5);
            var predicateGreaterThan = QueryPredicateFactory.Create((IntegerOnly i) => i.Value > 5);
            var predicateGreaterThanEqual =
                QueryPredicateFactory.Create((IntegerOnly i) => i.Value >= 5);
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

                Assert.IsType<PropertyPredicate>(predicate);

                var propertyPredicate = (PropertyPredicate)predicate;

                Assert.Equal(nameof(IntegerOnly.Value), propertyPredicate.PropertyPath);
                Assert.Equal(binaryOperator, propertyPredicate.BinaryOperator);
                Assert.Equal(5, propertyPredicate.Value);
            }
        }

        [Fact]
        public void IntegerVariable()
        {
            for (var i = 14; i != 15; ++i)
            {
                var predicateEqual =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value == 5);
                var predicateNotEqual =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value != 5);
                var predicateLessThan =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value < 5);
                var predicateLessThanEqual =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value <= 5);
                var predicateGreaterThan =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value > 5);
                var predicateGreaterThanEqual =
                    QueryPredicateFactory.Create((IntegerOnly i) => i.Value >= 5);
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

                    Assert.IsType<PropertyPredicate>(predicate);

                    var propertyPredicate = (PropertyPredicate)predicate;

                    Assert.Equal(nameof(IntegerOnly.Value), propertyPredicate.PropertyPath);
                    Assert.Equal(binaryOperator, propertyPredicate.BinaryOperator);
                    Assert.Equal(5, propertyPredicate.Value);
                }
            }
        }
    }
}