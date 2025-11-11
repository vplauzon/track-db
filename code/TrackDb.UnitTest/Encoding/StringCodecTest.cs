using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using TrackDb.Lib.Encoding;

namespace TrackDb.UnitTest.Encoding
{
    public class StringCodecTest
    {
        [Fact]
        public void IdenticalNonNull()
        {
            TestScenario(new string?[] { "Bob", "Bob", "Bob" }, false);
        }

        [Fact]
        public void IdenticalWithNull()
        {
            TestScenario(new string?[] { "Bob", null, "Bob" }, true);
        }

        [Fact]
        public void IdenticalWithOneNonNull()
        {
            TestScenario(new string?[] { null, null, "Alice" }, true);
        }

        [Fact]
        public void OnlyNulls()
        {
            TestScenario(new string?[] { null, null, null, null }, false);
        }

        [Fact]
        public void TwoValues()
        {
            TestScenario(new string?[] { "Alice", "Bob", "Bob", "Alice" }, true);
        }

        [Fact]
        public void TwoValuesWithNulls()
        {
            TestScenario(new string?[] { "Alice", null, "Bob", "Bob", null, "Alice" }, true);
        }

        [Fact]
        public void VariedNonNull()
        {
            TestScenario(new string?[] { "Hi", "My", "name", "is", "Sam" }, true);
        }

        [Fact]
        public void VariedWithNull()
        {
            TestScenario(new string?[] { "Hi", null, "name", null, "Sam" }, true);
        }

        [Fact]
        public void LongOne()
        {
            //  Fixed seed for reproductability
            var random = new Random(42);
            var dictionary = new[] { "Alice", "Bob", "Carl" };

            TestScenario(
                Enumerable.Range(0, 25000)
                .Select(i => dictionary[random.Next(0, dictionary.Length)])
                .ToImmutableArray(),
                true);
        }

        [Fact]
        public void EmptySequence()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                var writer = new ByteWriter(new Span<byte>(), false);
                var draftWriter = new ByteWriter(new Span<byte>(), false);

                StringCodec.Compress(null!, ref writer, draftWriter);
            });
            Assert.Throws<ArgumentNullException>(() =>
            {
                var writer = new ByteWriter(new Span<byte>(), false);
                var draftWriter = new ByteWriter(new Span<byte>(), false);

                StringCodec.Compress(Array.Empty<string?>(), ref writer, draftWriter);
            });
        }

        [Fact]
        public void SequenceTooLarge()
        {
            var data = Enumerable.Range(0, UInt16.MaxValue + 1)
                .Select(i => "Bob")
                .ToArray();

            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                var writer = new ByteWriter(new Span<byte>(), false);
                var draftWriter = new ByteWriter(new Span<byte>(), false);

                StringCodec.Compress(data, ref writer, draftWriter);
            });
        }

        [Fact]
        public void SingleValueSequences()
        {
            TestScenario(new string?[] { "Alice" }, false);
            TestScenario(new string?[] { null }, false);
        }

        [Fact]
        public void TwoValueSequences()
        {
            TestScenario(new string?[] { "Alice", "Bob" }, true);
            TestScenario(new string?[] { "Alice", null }, true);
            TestScenario(new string?[] { null, "Bob" }, true);
            TestScenario(new string?[] { "Alice", "Alice" }, false);
        }

        private static void TestScenario(IEnumerable<string?> data, bool doExpectPayload)
        {
            var buffer = new byte[1000];
            var writer = new ByteWriter(buffer, true);
            var draftWriter = new ByteWriter(new byte[1000], true);
            var package = StringCodec.Compress(data, ref writer, draftWriter);
            var decodedArray = StringCodec.Decompress(
                data.Count(),
                buffer.AsSpan(0, writer.Position))
                .ToImmutableArray();

            Assert.False(writer.IsOverflow);
            Assert.Equal(doExpectPayload, writer.Position != 0);
            Assert.True(Enumerable.SequenceEqual(decodedArray, data));
            Assert.Equal(data.Min(), decodedArray.Min());
            Assert.Equal(data.Max(), decodedArray.Max());
        }
    }
}
