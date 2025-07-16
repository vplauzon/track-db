using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Query
{
    internal class QueryPredicateFactory
    {
        public static IQueryPredicate Create<T>(
            Expression<Func<T, bool>> predicateExpression)
        {
            if (predicateExpression.Body is BinaryExpression binaryExpression)
            {
                var binaryOperator = GetBinaryOperator(binaryExpression.NodeType);

                if (binaryExpression.Left is MemberExpression leftMemberExpression
                    && leftMemberExpression.Member is PropertyInfo leftPropertyInfo)
                {
                    return CreateBinaryExpression(
                        GetPropertyPath(leftPropertyInfo),
                        binaryOperator,
                        binaryExpression.Right);
                }
                else if (binaryExpression.Right is MemberExpression rightMemberExpression
                    && rightMemberExpression.Member is PropertyInfo rightPropertyInfo)
                {
                    return CreateBinaryExpression(
                        GetPropertyPath(rightPropertyInfo),
                        binaryOperator,
                        binaryExpression.Left);
                }
                else
                {
                    throw new NotSupportedException($"Unsupported binary expression");
                }
            }

            throw new NotSupportedException($"Unsupported expression");
        }

        private static BinaryOperator GetBinaryOperator(ExpressionType nodeType)
        {
            switch (nodeType)
            {
                case ExpressionType.Equal:
                    return BinaryOperator.Equal;
                case ExpressionType.NotEqual:
                    return BinaryOperator.NotEqual;
                case ExpressionType.LessThan:
                    return BinaryOperator.LessThan;
                case ExpressionType.LessThanOrEqual:
                    return BinaryOperator.LessThanOrEqual;
                case ExpressionType.GreaterThan:
                    return BinaryOperator.GreaterThan;
                case ExpressionType.GreaterThanOrEqual:
                    return BinaryOperator.GreaterThanOrEqual;

                default:
                    throw new NotSupportedException($"Binary operator:  '{nodeType}'");
            }
            throw new NotImplementedException();
        }

        private static string GetPropertyPath(PropertyInfo propertyInfo)
        {
            return propertyInfo.Name;
        }

        private static IQueryPredicate CreateBinaryExpression(
            string propertyPath,
            BinaryOperator binaryOperator,
            Expression valueExpression)
        {
            if (valueExpression is ConstantExpression constantExpression)
            {
                return new BinaryOperatorPredicate(
                    propertyPath,
                    constantExpression.Value,
                    binaryOperator);
            }
            else if (valueExpression is MemberExpression memberExpression)
            {
                if (memberExpression.Expression is ConstantExpression innerConstantExpression)
                {
                    return new BinaryOperatorPredicate(
                        propertyPath,
                        GetConstantValue(innerConstantExpression.Value, memberExpression.Member),
                        binaryOperator);
                }

                throw new NotSupportedException("Constant expression");
            }
            else
            {
                throw new NotSupportedException("Unsupported expression in binary expression");
            }
        }

        private static object? GetConstantValue(object? container, MemberInfo member)
        {
            if(container == null)
            {
                throw new NotSupportedException(
                    "Can't extract constant value from a null object");
            }

            switch (member)
            {
                case FieldInfo field:
                    return field.GetValue(container);
                case PropertyInfo prop:
                    return prop.GetValue(container);

                default:
                    throw new NotSupportedException($"Member type:  '{member.GetType().Name}'");
            }
        }
    }
}