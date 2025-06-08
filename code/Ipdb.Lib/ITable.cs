using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Ipdb.Lib
{
    public interface ITable<T>
    {
        void AppendDocument();

        IEnumerable<T> Query(Expression<Func<T, bool>> predicate);
    }
}
