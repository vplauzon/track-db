using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    /// <summary>
    /// Commented out operators are supported through composed logical operations.
    /// For example NotEqual = Not(Equal), GreaterThan=Not(LessThanOrEqual)
    /// & GreaterThanOrEqual = Not(LesserThan)
    /// </summary>
    public enum BinaryOperator
    {
        Equal,
        //NotEqual,
        LessThan,
        LessThanOrEqual,
        //GreaterThan,
        //GreaterThanOrEqual
    }
}