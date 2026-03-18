using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.Predicate
{
    internal interface IInPredicate
    {
        /// <summary>
        /// This is an ISet<T>.
        /// </summary>
        object Values { get; }

        /// <summary>The values are in or not-in.</summary>
        bool IsIn { get; }
    }
}