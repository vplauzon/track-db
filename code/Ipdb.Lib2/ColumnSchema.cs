using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2
{
    internal readonly record struct ColumnSchema(string PropertyName, Type ColumnType)
    {
        public static IImmutableList<ColumnSchema> Reflect(Type representationType)
        {
            var constructor = representationType.GetConstructors().First();
            var parameters = constructor.GetParameters();
            var properties = representationType.GetProperties();

            var schemas = parameters.Select(param =>
            {
                if (param.Name == null)
                {
                    throw new InvalidOperationException(
                        "Record constructor parameter must have a name");
                }

                var matchingProp = properties.FirstOrDefault(p => 
                    p.Name == param.Name && 
                    p.PropertyType == param.ParameterType);

                if (matchingProp == null)
                {
                    throw new InvalidOperationException(
                        $"Parameter {param.Name} does not have a matching property of type " + 
                        $"{param.ParameterType.Name}");
                }

                return new ColumnSchema(
                    PropertyName: param.Name,
                    ColumnType: param.ParameterType);
            });

            return schemas.ToImmutableList();
        }
    }
}