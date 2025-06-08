namespace Ipdb.Lib
{
    public interface IEngine
    {
        IDatabase CreateDatabase(string name);
    }
}