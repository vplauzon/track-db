using System;

namespace TrackDb.Lib.Policies
{
	public record StoragePolicy(ushort BlockSize)
	{
		public static StoragePolicy Create(ushort? BlockSize = null)
		{
			return new(BlockSize ?? 4096);
		}
	}
}