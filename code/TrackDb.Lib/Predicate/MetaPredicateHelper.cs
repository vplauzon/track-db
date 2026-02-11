using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.Predicate
{
    internal static class MetaPredicateHelper
    {
        /// <summary>
        /// Transforms <paramref name="predicate"/> for <paramref name="schema"/> into a
        /// predicate for <paramref name="metaSchema"/>, assuming the latter is the
        /// metadata schema of the former.
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="schema"></param>
        /// <param name="metaSchema"></param>
        /// <returns></returns>
        public static QueryPredicate GetCorrespondantPredicate(
            QueryPredicate predicate,
            TableSchema schema,
            MetadataTableSchema metaSchema)
        {
            var referencedColumnIndexes = predicate.ReferencedColumnIndexes.Distinct();
            var correspondances = metaSchema.GetColumnCorrespondances();

            throw new NotImplementedException();
        }
    }
}