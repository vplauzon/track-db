using System.IO;
using System.Text.Json;

namespace TrackDb.Lib.Logging
{
    internal record ContentBase<T>
    {
        private static readonly JsonSerializerOptions JSON_OPTIONS = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public static T FromJson(string json)
        {
            var content = JsonSerializer.Deserialize<T>(json, JSON_OPTIONS);

            return content
                ?? throw new InvalidDataException($"Can't deserialize '{json}'");
        }

        public string ToJson()
        {
            return JsonSerializer.Serialize((object)this, typeof(T), JSON_OPTIONS);
        }
    }
}