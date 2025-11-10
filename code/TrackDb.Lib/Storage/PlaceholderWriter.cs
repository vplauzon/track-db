using System;

namespace TrackDb.Lib.Storage
{
    internal ref struct PlaceholderWriter<T>
    {
        private readonly Span<byte> _span;
        private readonly Action<Span<byte>, T> _action;

        public PlaceholderWriter(Span<byte> span, Action<Span<byte>, T> value)
        {
            _span = span;
            _action = value;
        }

        public void SetValue(T value)
        {
            _action(_span, value);
        }
    }
}