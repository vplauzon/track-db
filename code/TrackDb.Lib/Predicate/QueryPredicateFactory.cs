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
            if (Schema.TryGetColumnIndex(propertySelection, out var columnIndex))
            {
                return new TypedQueryPredicate<T>(
                    new BinaryOperatorPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        value,
                        BinaryOperator.Equal),
                    Schema);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public TypedQueryPredicate<T> NotEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicate<T>(
                new NegationPredicate(
                    new BinaryOperatorPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        value,
                        BinaryOperator.Equal)),
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
            if (Schema.TryGetColumnIndex(expression, out var columnIndex))
            {
                return columnIndex;
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