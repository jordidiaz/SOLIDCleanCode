using System;

namespace SOLID._2
{
    internal class Car : IVehicle
    {
        public void Drive()
        {
            Console.WriteLine("Driving...");
        }

        public void Fly()
        {
            throw new Exception("A car cannot fly!");
        }
    }
}