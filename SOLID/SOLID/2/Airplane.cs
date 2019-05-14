using System;

namespace SOLID._2
{
    internal class Airplane : IVehicle
    {
        public void Drive()
        {
            throw new Exception("An Airplane cannot de driven!");
        }

        public void Fly()
        {
            Console.WriteLine("Flying...");
        }
    }
}