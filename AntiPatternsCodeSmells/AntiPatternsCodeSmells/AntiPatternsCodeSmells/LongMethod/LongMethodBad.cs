using System;

namespace AntiPatternsCodeSmells.LongMethod
{
    public static class LongMethodBad
    {
        public Coordinates GetCoordinates(string location) // location (25.5, 26.7)
        {
            var noParens = location.Replace('(', ' ').Replace(')', ' ');
            var coordinates = noParens.Split(",");

            if (coordinates.Length != 2)
            {
                throw new Exception("That's not a valid coordinate.");
            }

            var latitude = float.Parse(coordinates[0].Trim());
            var longitude = float.Parse(coordinates[1].Trim());

            return new Coordinates
            {
                Latitude = latitude,
                Longitude = longitude
            };
        }

        public class Coordinates
        {
            public float Latitude { get; set; }

            public float Longitude { get; set; }
        }
    }
}