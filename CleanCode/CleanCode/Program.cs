using CleanCode.Services;
using System;

namespace CleanCode
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var serv = new EmployeeService();
            var serv1 = new BalanceService(100);
            serv1.Balance = 1000;

            // Obtener trabajadores
            var es = serv.CreateWorkers();

            // escribir numero de trabajadores
            Console.WriteLine($"Number of employees is {es.Count}");

            // pagarles su sueldo a cada uno
            foreach (var employee in es)
            {
                bool ok = false;
                try
                {
                    ok = serv1.Pay(employee);
                }
                catch (Exception e)
                {
                }

                if (!ok)
                {
                    Console.WriteLine($"Cannot pay to {employee.stringname}");
                }

                if (ok)
                {
                    Console.WriteLine($"{employee.stringname} paid OK");
                }
            }

            // mostrar balance
            Console.WriteLine($"The balance now is {serv1.Balance}");

            Console.ReadLine();
        }
    }
}