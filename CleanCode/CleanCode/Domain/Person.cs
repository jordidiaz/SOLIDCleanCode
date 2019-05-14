using System;

namespace CleanCode.Domain
{
    public class Person
    {
        public string stringname { get; set; }

        public DateTime dbirth { get; set; }

        public Gender g { get; set; }
    }
}