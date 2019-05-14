namespace CleanCode.Domain
{
    public class Employee : Person
    {
        public double doubleSalary { get; set; }

        public Job EmployeeJob { get; set; }

        public bool Paid { get; set; }
    }
}