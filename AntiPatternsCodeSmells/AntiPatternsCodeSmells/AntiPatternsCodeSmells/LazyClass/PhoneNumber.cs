namespace AntiPatternsCodeSmells.LazyClass
{
    public class PhoneNumber
    {
        public PhoneNumber(string areaCode, string number)
        {
            AreaCode = areaCode;
            Number = number;
        }

        public string AreaCode { get; private set; }

        public string Number { get; private set; }

        public string GetPhoneNumber()
        {
            return $"{AreaCode}{Number}";
        }
    }
}