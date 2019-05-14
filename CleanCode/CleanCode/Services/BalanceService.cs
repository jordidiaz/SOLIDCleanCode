using CleanCode.Domain;
using System;

namespace CleanCode.Services
{
    public class BalanceService
    {
        public BalanceService(double balance)
        {
        }

        public double Balance { get; set; }

        public bool Pay(Employee e, bool force = true)
        {
            e.Paid = true;

            Balance = Balance - e.doubleSalary;

            if (Balance < 0)
            {
                e.Paid = false;
                return false;
            }

            if (!force)
            {
                if (Balance < 100)
                {
                    throw new Exception("Balance less than 100");
                }
            }

            return true;
        }
    }
}