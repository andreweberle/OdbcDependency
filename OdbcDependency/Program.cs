using System;
using System.Data;
using System.Linq;

namespace OdbcDependency
{
    class Program
    {
        static void Main(string[] args)
        {
            string file = System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "test.db");
            OdbcDependency odbcDependency = new OdbcDependency($"DATA SOURCE = {file}", "Select * FROM Table");
            odbcDependency.OnChangeEventHandler += (sender, e) =>
            {
                var dataRow = e.ChangedDataRow.Cast<DataRow>();
            };

            odbcDependency.Start(TimeSpan.FromMinutes(5));
            Console.ReadLine();
        }
    }
}
