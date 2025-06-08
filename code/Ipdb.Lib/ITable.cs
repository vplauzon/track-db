using System;

namespace Ipdb.Lib
{
    public interface ITable<T>
    {
        void AddIndex<PT>(Func<T, PT> propertyExtractor);

        void AppendDocument();
    }
}