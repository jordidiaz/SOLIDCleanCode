namespace SOLID._5
{
    public class Circle : Ellipse
    {
        public override void SetMajorAxis(double majorAxis)
        {
            base.SetMajorAxis(majorAxis);
            MinorAxis = majorAxis; //En un circulo los dos ejes son identicos
        }
    }
}