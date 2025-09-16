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
            return new TypedQueryPredicate<T>(
                new BinaryOperatorPredicate(
                    GetColumnIndexes(propertySelection.Body),
                    value,
                    BinaryOperator.Equal),
                Schema);
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

        #region MatchKeys
        public TypedQueryPredicate<T> MatchKeys<U1>(
            T record,
            Expression<Func<T, U1>> propertySelection1)
        {
            var columnIndex1 = GetColumnIndexes(propertySelection1.Body);
            var columns = Schema.FromObjectToColumns(record);

            return new TypedQueryPredicate<T>(
                new BinaryOperatorPredicate(
                    columnIndex1,
                    columns[columnIndex1],
                    BinaryOperator.Equal),
                Schema);
        }

        public TypedQueryPredicate<T> MatchKeys<U1, U2>(
            T record,
            Expression<Func<T, U1>> propertySelection1,
            Expression<Func<T, U2>> propertySelection2)
        {
            var columnIndex1 = GetColumnIndexes(propertySelection1.Body);
            var columnIndex2 = GetColumnIndexes(propertySelection2.Body);
            var columns = Schema.FromObjectToColumns(record);

            return new TypedQueryPredicate<T>(
                new ConjunctionPredicate(
                    new BinaryOperatorPredicate(
                        columnIndex1,
                        columns[columnIndex1],
                        BinaryOperator.Equal),
                    new BinaryOperatorPredicate(
                        columnIndex2,
                        columns[columnIndex2],
                        BinaryOperator.Equal)),
                Schema);
        }

        public TypedQueryPredicate<T> MatchKeys<U1, U2, U3>(
            T record,
            Expression<Func<T, U1>> propertySelection1,
            Expression<Func<T, U2>> propertySelection2,
            Expression<Func<T, U3>> propertySelection3)
        {
            var columnIndex1 = GetColumnIndexes(propertySelection1.Body);
            var columnIndex2 = GetColumnIndexes(propertySelection2.Body);
            var columnIndex3 = GetColumnIndexes(propertySelection3.Body);
            var columns = Schema.FromObjectToColumns(record);

            return new TypedQueryPredicate<T>(
                new ConjunctionPredicate(
                    new ConjunctionPredicate(
                        new BinaryOperatorPredicate(
                            columnIndex1,
                            columns[columnIndex1],
                            BinaryOperator.Equal),
                        new BinaryOperatorPredicate(
                            columnIndex2,
                            columns[columnIndex2],
                            BinaryOperator.Equal)),
                    new BinaryOperatorPredicate(
                        columnIndex3,
                        columns[columnIndex3],
                        BinaryOperator.Equal)),
                Schema);
        }

        public TypedQueryPredicate<T> MatchKeys<U1, U2, U3, U4>(
            T record,
            Expression<Func<T, U1>> propertySelection1,
            Expression<Func<T, U2>> propertySelection2,
            Expression<Func<T, U3>> propertySelection3,
            Expression<Func<T, U4>> propertySelection4)
        {
            var columnIndex1 = GetColumnIndexes(propertySelection1.Body);
            var columnIndex2 = GetColumnIndexes(propertySelection2.Body);
            var columnIndex3 = GetColumnIndexes(propertySelection3.Body);
            var columnIndex4 = GetColumnIndexes(propertySelection4.Body);
            var columns = Schema.FromObjectToColumns(record);

            return new TypedQueryPredicate<T>(
                new ConjunctionPredicate(
                    new ConjunctionPredicate(
                        new BinaryOperatorPredicate(
                            columnIndex1,
                            columns[columnIndex1],
                            BinaryOperator.Equal),
                        new BinaryOperatorPredicate(
                            columnIndex2,
                            columns[columnIndex2],
                            BinaryOperator.Equal)),
                    new ConjunctionPredicate(
                        new BinaryOperatorPredicate(
                            columnIndex3,
                            columns[columnIndex3],
                            BinaryOperator.Equal),
                        new BinaryOperatorPredicate(
                            columnIndex4,
                            columns[columnIndex4],
                            BinaryOperator.Equal))),
                Schema);
        }
        #endregion
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