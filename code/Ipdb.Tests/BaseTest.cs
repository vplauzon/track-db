using Ipdb.Lib;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Tests
{
    public class BaseTest
    {
        protected Engine Engine { get; } = new(Path.Combine(
            Environment.GetEnvironmentVariable("EngineRoot")!,
            $"{DateTime.Now:yyyy-MM-dd_HH-mm-ss}"));
    }
}