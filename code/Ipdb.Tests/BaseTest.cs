using Ipdb.Lib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Tests
{
    public class BaseTest
    {
        protected Engine Engine { get; } = new();
    }
}