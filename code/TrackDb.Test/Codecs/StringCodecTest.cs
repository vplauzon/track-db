using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.Test.Codecs
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
            Assert.Throws<ArgumentNullException>(() => StringCodec.Compress(null!));
            Assert.Throws<ArgumentNullException>(() => StringCodec.Compress(Array.Empty<string?>()));
        }

        [Fact]
        public void SequenceTooLarge()
        {
            var data = Enumerable.Range(0, UInt16.MaxValue + 1)
                .Select(i => "Bob")
                .ToArray();
            Assert.Throws<ArgumentOutOfRangeException>(() => StringCodec.Compress(data));
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
            var column = StringCodec.Compress(data);
            var decodedArray = StringCodec.Decompress(column)
                .ToImmutableArray();

            Assert.Equal(doExpectPayload, column.Payload.Length != 0);
            Assert.True(Enumerable.SequenceEqual(decodedArray, data));
            Assert.Equal(data.Min(), decodedArray.Min());
            Assert.Equal(data.Max(), decodedArray.Max());
        }
    }
}
