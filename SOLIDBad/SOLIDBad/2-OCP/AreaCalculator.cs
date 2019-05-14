using System;
using System.Collections.Generic;

namespace SOLIDBad._2_OCP
{
    public class AreaCalculator
    {
        public double TotalArea(IEnumerable<object> objects)
        {
            double area = 0;

            foreach (var obj in objects)
            {
                if (obj is Rectangle rectangle)
                {
                    area += rectangle.Width * rectangle.Width;
                }
                else
                {
                    var circle = (Circle)obj;
                    area += circle.Radius * circle.Radius * Math.PI;
                }
            }

            return area;
        }
    }
}