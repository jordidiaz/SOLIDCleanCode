namespace AntiPatternsCodeSmells.LazyClass
{
    public class Person
    {
        public Person(string name)
        {
            Name = name;
        }

        public string Name { get; private set; }

        public PhoneNumber PhoneNumber { get; set; }
    }
}