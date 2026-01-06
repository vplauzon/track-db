using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib
{
    internal record BlobClients(DataLakeDirectoryClient Directory, BlobContainerClient Container);
}
