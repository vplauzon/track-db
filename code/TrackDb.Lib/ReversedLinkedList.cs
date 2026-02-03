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
            var stack = new Stack<T>();
            var current = this;

            while (current != null)
            {
                stack.Push(current.Content);
                current = current.Next;
            }

            while (stack.Count > 0)
            {
                yield return stack.Pop();
            }
        }
    }
}