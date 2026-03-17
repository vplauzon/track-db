using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal record ReversedLinkedList<T>(T Content, ReversedLinkedList<T>? Next)
    {
        public IEnumerable<T> ToEnumerable()
        {
            return ToArray(1);
        }

        private T[] ToArray(int offset)
        {
            var array = Next == null
                ? new T[offset]
                : Next.ToArray(offset + 1);
            
            array[array.Length - offset] = Content;

            return array;
        }
    }
}