using Azure;
using Azure.Core;
using Azure.Storage;
using System;

namespace TrackDb.Lib.Policies
{
    public record StorageConfiguration(
        Uri LogFolderUri,
        TokenCredential? TokenCredential,
        StorageSharedKeyCredential? KeyCredential)
    {
        public void Validate()
        {
            if (TokenCredential == null && KeyCredential == null)
            {
                throw new ArgumentException(
                    $"{nameof(TokenCredential)} & {nameof(KeyCredential)} can't both be null");
            }
            if (TokenCredential != null && KeyCredential != null)
            {
                throw new ArgumentException(
                    $"{nameof(TokenCredential)} & {nameof(KeyCredential)} can't both be non-null");
            }
        }
    }
}