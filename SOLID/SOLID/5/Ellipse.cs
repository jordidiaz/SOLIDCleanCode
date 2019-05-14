using System;

namespace SOLID._5
{
    public class Ellipse
    {
        public double MajorAxis { get; set; }
        public double MinorAxis { get; set; }

        public virtual void SetMajorAxis(double majorAxis)
        {
            MajorAxis = majorAxis;
        }

        public virtual void SetMinorAxis(double minorAxis)
        {
            MinorAxis = minorAxis;
        }

        public virtual double Area()
        {
            return MajorAxis * MinorAxis * Math.PI;
        }
    }
}