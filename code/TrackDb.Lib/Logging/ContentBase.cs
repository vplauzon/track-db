using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace TrackDb.Lib.Logging
{
    internal record ContentBase<T>
    {
        public static T FromJson(string json)
        {
            var content = JsonSerializer.Deserialize<T>(json, GetTypeInfo());

            return content
                ?? throw new InvalidDataException($"Can't deserialize '{json}'");
        }

        public virtual string ToJson()
        {
            var json = JsonSerializer.Serialize(this, GetTypeInfo());

            if (string.IsNullOrWhiteSpace(json))
            {
                throw new InvalidOperationException($"Can't serialize content of type {typeof(T).Name}");
            }

            return json;
        }

        private static JsonTypeInfo<T> GetTypeInfo()
        {
            return ContentJsonContext.Default.GetTypeInfo(typeof(T)) as JsonTypeInfo<T>
                ?? throw new InvalidOperationException($"Type {typeof(T)} not registered in JsonContext");
        }
    }
}