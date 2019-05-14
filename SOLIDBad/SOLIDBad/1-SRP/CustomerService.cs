using System;
using System.IO;

namespace SOLIDBad._1_SRP
{
    public class CustomerService
    {
        private readonly Database _database;

        public CustomerService(Database database)
        {
            _database = database;
        }

        public void Add()
        {
            try
            {
                _database.Add();
            }
            catch (Exception ex)
            {
                File.WriteAllText(@"C:\Error.txt", ex.ToString());
            }
        }
    }
}