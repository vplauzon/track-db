using Azure;
using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
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

        internal BlobClients CreateClients()
        {
            if (TokenCredential != null)
            {
                var dummyBlob = new AppendBlobClient(LogFolderUri, TokenCredential);
                var loggingDirectory = new DataLakeDirectoryClient(LogFolderUri, TokenCredential);
                var loggingContainer = dummyBlob.GetParentBlobContainerClient();

                return new(loggingDirectory, loggingContainer);
            }
            else
            {
                var dummyBlob = new AppendBlobClient(LogFolderUri, KeyCredential);
                var loggingDirectory = new DataLakeDirectoryClient(LogFolderUri, KeyCredential);
                var loggingContainer = dummyBlob.GetParentBlobContainerClient();

                return new(loggingDirectory, loggingContainer);
            }
        }
    }
}