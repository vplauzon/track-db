using System;

namespace TrackDb.Lib.Encoding
{
    internal ref struct PlaceholderArrayWriter<T>
    {
        private readonly Span<byte> _span;
        private readonly int _arrayLength;
        private readonly Action<Span<byte>, int, T> _action;

        public PlaceholderArrayWriter(
            Span<byte> span,
            int arrayLength,
            Action<Span<byte>, int, T> action)
        {
            _span = span;
            _arrayLength = arrayLength;
            _action = action;
        }

        public void SetValue(int index, T value)
        {
            if (index > _arrayLength)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            _action(_span, index, value);
        }
    }
}