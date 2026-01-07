using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace TrackDb.LogTest
{
    public class TestBase
    {
        protected string GetTestId(
            [CallerMemberName] string memberName = "",
            [CallerFilePath] string filePath = "")
        {
            var className = Path.GetFileNameWithoutExtension(filePath);
            
            return $"{className}.{memberName}-{Guid.NewGuid()}";
        }
    }
}