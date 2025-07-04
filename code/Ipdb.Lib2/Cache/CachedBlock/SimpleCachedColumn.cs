using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
    internal class SimpleCachedColumn<T> : ICachedColumn
        where T : IEquatable<T>, IComparable<T>
    {
    }
}