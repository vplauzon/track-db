using System;
using System.Collections.Immutable;
using System.IO;
using System.Text.Json;

namespace TrackDb.UnitTest
{
    public class TestConfiguration
    {
        private const string FILE_NAME = "Properties/test.json";

        private readonly IImmutableDictionary<string, string> _map;

        private TestConfiguration()
        {
            if (File.Exists(FILE_NAME))
            {
                using (var stream = File.OpenRead(FILE_NAME))
                {
                    var map =
                        JsonSerializer.Deserialize<IImmutableDictionary<string, string>>(stream);

                    if (map == null)
                    {
                        throw new FileLoadException(FILE_NAME);
                    }
                    else
                    {
                        _map = map;
                    }
                }
            }
            else
            {
                _map = ImmutableDictionary<string, string>.Empty;
            }
        }

        public static TestConfiguration Instance { get; } = new TestConfiguration();

        public string? GetConfiguration(string key)
        {
            if(_map.TryGetValue(key, out var value))
            {
                return value;
            }
            else
            {
                return null;
            }
        }
    }
}