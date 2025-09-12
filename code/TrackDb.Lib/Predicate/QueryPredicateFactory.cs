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
        public ITypedQueryPredicate<T> Equal<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            if (Schema.TryGetColumnIndex(propertySelection, out var columnIndex))
            {
                return new TypedQueryPredicateAdapter<T>(
                    new BinaryOperatorPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        value,
                        BinaryOperator.Equal));
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public ITypedQueryPredicate<T> NotEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicateAdapter<T>(
                new NegationPredicate(
                    new BinaryOperatorPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        value,
                        BinaryOperator.Equal)));
        }

        public ITypedQueryPredicate<T> In<U>(
            Expression<Func<T, U>> propertySelection,
            IEnumerable<U> values)
        {
            return new TypedQueryPredicateAdapter<T>(
                new InPredicate(
                    GetColumnIndexes(propertySelection.Body),
                    values.Cast<object?>()));
        }

        public ITypedQueryPredicate<T> NotIn<U>(
            Expression<Func<T, U>> propertySelection,
            IEnumerable<U> values)
        {
            return new TypedQueryPredicateAdapter<T>(
                new NegationPredicate(
                    new InPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        values.Cast<object?>())));
        }

        public ITypedQueryPredicate<T> LessThan<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicateAdapter<T>(
                new BinaryOperatorPredicate(
                    GetColumnIndexes(propertySelection.Body),
                    value,
                    BinaryOperator.LessThan));
        }

        public ITypedQueryPredicate<T> LessThanOrEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicateAdapter<T>(
                new BinaryOperatorPredicate(
                    GetColumnIndexes(propertySelection.Body),
                    value,
                    BinaryOperator.LessThanOrEqual));
        }

        public ITypedQueryPredicate<T> GreaterThan<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicateAdapter<T>(
                new NegationPredicate(
                    new BinaryOperatorPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        value,
                        BinaryOperator.LessThanOrEqual)));
        }

        public ITypedQueryPredicate<T> GreaterThanOrEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicateAdapter<T>(
                new NegationPredicate(
                    new BinaryOperatorPredicate(
                        GetColumnIndexes(propertySelection.Body),
                        value,
                        BinaryOperator.LessThan)));
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