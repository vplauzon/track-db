using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
            return new TypedQueryPredicateAdapter<T>(
                new BinaryOperatorPredicate(
                    GetColumnIndex(propertySelection.Body),
                    value,
                    BinaryOperator.Equal));
        }

        public ITypedQueryPredicate<T> NotEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicateAdapter<T>(
                new NegationPredicate(
                    new BinaryOperatorPredicate(
                        GetColumnIndex(propertySelection.Body),
                        value,
                        BinaryOperator.Equal)));
        }

        public ITypedQueryPredicate<T> In<U>(
            Expression<Func<T, U>> propertySelection,
            IEnumerable<U> values)
        {
            return new TypedQueryPredicateAdapter<T>(
                new InPredicate(
                    GetColumnIndex(propertySelection.Body),
                    values.Cast<object?>()));
        }

        public ITypedQueryPredicate<T> LessThan<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicateAdapter<T>(
                new BinaryOperatorPredicate(
                    GetColumnIndex(propertySelection.Body),
                    value,
                    BinaryOperator.LessThan));
        }

        public ITypedQueryPredicate<T> LessThanOrEqual<U>(
            Expression<Func<T, U>> propertySelection,
            U value)
        {
            return new TypedQueryPredicateAdapter<T>(
                new BinaryOperatorPredicate(
                    GetColumnIndex(propertySelection.Body),
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
                        GetColumnIndex(propertySelection.Body),
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
                        GetColumnIndex(propertySelection.Body),
                        value,
                        BinaryOperator.LessThan)));
        }
        #endregion

        #region Propery selection
        private int GetColumnIndex(Expression expression)
        {
            if (expression is MemberExpression me)
            {
                if (me.Member is PropertyInfo pi)
                {
                    return GetColumnIndex(pi);
                }
                else
                {
                    throw new NotSupportedException($"MemberInfo '{me.GetType().Name}'");
                }
            }
            else
            {
                throw new NotSupportedException($"Expression '{expression.GetType().Name}'");
            }
        }

        private int GetColumnIndex(PropertyInfo propertyInfo)
        {
            return GetColumnIndex(GetPropertyPath(propertyInfo));
        }

        private int GetColumnIndex(string columnName)
        {
            if (Schema.TryGetColumnIndex(columnName, out var columnIndex))
            {
                return columnIndex;
            }
            else
            {
                throw new ArgumentOutOfRangeException(
                    nameof(columnName),
                    $"Column '{columnName}' doesn't exist in table '{Schema.TableName}'");
            }
        }

        private string GetPropertyPath(PropertyInfo propertyInfo)
        {
            return propertyInfo.Name;
        }
        #endregion
    }
}