using System;

namespace TrackDb.Lib.Encoding
{
    internal ref struct PlaceholderWriter<T>
    {
        private readonly Span<byte> _span;
        private readonly Action<Span<byte>, T> _action;

        public PlaceholderWriter(Span<byte> span, Action<Span<byte>, T> action)
        {
            _span = span;
            _action = action;
        }

        public void SetValue(T value)
        {
            _action(_span, value);
        }
    }
}