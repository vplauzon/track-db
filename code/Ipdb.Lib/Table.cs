using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Ipdb.Lib
{
    public class Table<T>
    {
        public IEnumerable<T> Query(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }

        public void AppendDocuments(params IEnumerable<T> documents)
        {
            throw new NotImplementedException();
        }

        public long DeleteDocuments(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }
    }
}
