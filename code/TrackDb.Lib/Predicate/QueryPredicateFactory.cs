using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public record QueryPredicateFactory<T>(TypedTableSchema<T> Schema)
        where T : notnull
    {
        #region Factory methods
        public TypedQueryPredicate<T> Equal<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            var columnIndexes = Schema.GetColumnIndexSubset(propertySelection);
            var columnValues = columnIndexes.Count > 1
                ? Schema.FromPropertyValueToColumns(value!)
                : new object[] { value! }.AsSpan();
            var predicate = (QueryPredicate?)null;

            for (var i = 0; i != columnIndexes.Count; ++i)
            {
                var newPredicate = new BinaryOperatorPredicate(
                    columnIndexes[i],
                    columnValues[i],
                    BinaryOperator.Equal);

                predicate = predicate == null
                    ? newPredicate
                    : new ConjunctionPredicate(predicate, newPredicate);
            }

            return new TypedQueryPredicate<T>(predicate!, Schema);
        }

        public TypedQueryPredicate<T> NotEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicate<T>(
                new NegationPredicate(Equal(propertySelection, value).QueryPredicate),
                Schema);
        }

        public TypedQueryPredicate<T> In<U>(
            Expression<Func<T, U>> propertySelection,
            IEnumerable<U> values)
        {
            return new TypedQueryPredicate<T>(
                new InPredicate(
                    GetColumnIndexes(propertySelection.Body),
                    values.Cast<object?>()),
                Schema);
        }

        public TypedQueryPredicate<T> NotIn<U>(
            Expression<Func<T, U>> propertySelection,
            IEnumerable<U> values)
        {
            return new TypedQueryPredicate<T>(
                new NegationPredicate(
                    new InPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        values.Cast<object?>())),
                Schema);
        }

        public TypedQueryPredicate<T> LessThan<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicate<T>(
                new BinaryOperatorPredicate(
                    GetColumnIndexes(propertySelection.Body),
                    value,
                    BinaryOperator.LessThan),
                Schema);
        }

        public TypedQueryPredicate<T> LessThanOrEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicate<T>(
                new BinaryOperatorPredicate(
                    GetColumnIndexes(propertySelection.Body),
                    value,
                    BinaryOperator.LessThanOrEqual),
                Schema);
        }

        public TypedQueryPredicate<T> GreaterThan<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicate<T>(
                new NegationPredicate(
                    new BinaryOperatorPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        value,
                        BinaryOperator.LessThanOrEqual)),
                Schema);
        }

        public TypedQueryPredicate<T> GreaterThanOrEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicate<T>(
                new NegationPredicate(
                    new BinaryOperatorPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        value,
                        BinaryOperator.LessThan)),
                Schema);
        }
        #endregion

        private int GetColumnIndexes(Expression expression)
        {
            var columnIndexes = Schema.GetColumnIndexSubset(expression);

            if (columnIndexes.Count == 1)
            {
                return columnIndexes[0];
            }
            else
            {
                throw new ArgumentOutOfRangeException(
                    nameof(expression),
                    $"Expression '{expression}' isn't mapped to a column");
            }
        }
    }
}