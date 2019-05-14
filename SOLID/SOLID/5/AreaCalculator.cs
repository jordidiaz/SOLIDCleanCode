namespace SOLID._5
{
    internal class AreaCalculator
    {
        internal double Calculate()
        {
            var shape = new Ellipse();
            shape.SetMajorAxis(5);
            shape.SetMinorAxis(4);
            return shape.Area();
        }
    }
}