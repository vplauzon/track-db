using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace TrackDb.Lib.Logging
{
    [JsonSourceGenerationOptions(
        WriteIndented = false,
        PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonSerializable(typeof(CheckpointHeader))]
    [JsonSerializable(typeof(TransactionContent))]
    internal partial class ContentJsonContext : JsonSerializerContext
    {
    }
}