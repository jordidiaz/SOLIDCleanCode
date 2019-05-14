using System;

namespace SOLID._2
{
    internal class HybridVehicle : IVehicle
    {
        public void Drive()
        {
            Console.WriteLine("Driving...");
        }

        public void Fly()
        {
            Console.WriteLine("Flying...");
        }
    }
}