using System;

namespace TrackDb.Lib.Encoding
{
    internal ref struct VirtualByteSpan
    {
        private readonly Span<byte> _span;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="span"></param>
        /// <param name="length">Provided in case the span is empty fack lack of space.</param>
        public VirtualByteSpan(Span<byte> span, int length)
        {
            _span = span;
            Length = length;
        }

        public bool HasData => _span.Length > 0;

        public int Length { get; }

        public byte this[int index]
        {
            get
            {
                return HasData
                    ? _span[index]
                    : (byte)0;
            }
            set
            {
                if (HasData)
                {
                    _span[index] = value;
                }
            }
        }

        public void Fill(byte value)
        {
            if(HasData)
            {
                for (var i = 0; i < Length; i++)
                {
                    _span[i] = value;
                }
            }
        }

        #region Copy
        public void CopyTo(VirtualByteSpan destination)
        {
            if (Length != destination.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(destination));
            }
            if (HasData && destination.HasData)
            {
                _span.CopyTo(destination._span);
            }
        }

        public void CopyFrom(ReadOnlySpan<byte> source)
        {
            if (Length != source.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(source));
            }
            if (HasData)
            {
                source.CopyTo(_span);
            }
        }
        #endregion
    }
}