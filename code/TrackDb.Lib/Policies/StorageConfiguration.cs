using Azure.Core;
using System;

namespace TrackDb.Lib.Policies
{
    public record StorageConfiguration(Uri LogFolderUri, TokenCredential TokenCredential);
}