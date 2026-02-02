using System;

namespace TrackDb.Lib.Policies
{
    public record DiagnosticPolicy(
        bool ThrowOnPhantomTombstones)
    {
        public static DiagnosticPolicy Create(
            bool? ThrowOnPhantomTombstones = null)
        {
            return new DiagnosticPolicy(
                ThrowOnPhantomTombstones ?? false);
        }
    }
}