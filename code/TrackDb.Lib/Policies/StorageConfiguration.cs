using Azure;
using Azure.Core;
using System;

namespace TrackDb.Lib.Policies
{
    public record StorageConfiguration(
        Uri LogFolderUri,
        TokenCredential? TokenCredential,
        AzureSasCredential? SasCredential)
    {
        public void Validate()
        {
            if (TokenCredential == null && SasCredential == null)
            {
                throw new ArgumentException(
                    $"{nameof(TokenCredential)} & {nameof(SasCredential)} can't both be null");
            }
            if (TokenCredential != null && SasCredential != null)
            {
                throw new ArgumentException(
                    $"{nameof(TokenCredential)} & {nameof(SasCredential)} can't both be non-null");
            }
        }
    }
}