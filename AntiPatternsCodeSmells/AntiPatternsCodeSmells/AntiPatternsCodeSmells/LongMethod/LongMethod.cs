using System;

namespace AntiPatternsCodeSmells.LongMethod
{
    public static class LongMethod
    {
        public BoardCoordinates GetCoordinates(string location) // location (25.5, 26.7)
        {
            var locationCleaned = CleanInput(location);

            var coordinates = GetLatitudeLongitudeOrThrow(locationCleaned);

            var (latitude, longitude) = ParseLatitudeLongitude(coordinates);

            return new BoardCoordinates
            {
                Latitude = latitude,
                Longitude = longitude
            };
        }

        private static (float, float) ParseLatitudeLongitude(string[] coordinates)
        {
            var latitude = float.Parse(coordinates[0].Trim());
            var longitude = float.Parse(coordinates[1].Trim());
            return (latitude, longitude);
        }

        private static string[] GetLatitudeLongitudeOrThrow(string locationCleaned)
        {
            var coordinates = locationCleaned.Split(",");
            if (coordinates.Length != 2)
            {
                throw new Exception("That's not a valid coordinate.");
            }

            return coordinates;
        }

        private static string CleanInput(string location)
        {
            return location.Replace('(', ' ').Replace(')', ' ');
        }

        public class BoardCoordinates
        {
            public float Latitude { get; set; }

            public float Longitude { get; set; }
        }
    }
}