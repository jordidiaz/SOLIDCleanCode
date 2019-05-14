using CleanCode.Domain;
using System;
using System.Collections.Generic;

namespace CleanCode.Services
{
    public class EmployeeService
    {
        public List<Employee> CreateWorkers()
        {
            var emp1 = new Employee();
            emp1.stringname = "Anna";
            emp1.dbirth = DateTime.Today;
            emp1.g = Gender.Female;
            emp1.doubleSalary = 100;
            emp1.EmployeeJob = Job.Technical;

            var emp2 = new Employee();
            emp2.stringname = "David";
            emp2.dbirth = DateTime.Today;
            emp2.g = Gender.Male;
            emp2.doubleSalary = 10;
            emp2.EmployeeJob = Job.Administrative;

            var emp3 = new Employee();
            emp3.stringname = "Mary";
            emp3.dbirth = DateTime.Today;
            emp3.g = Gender.Female;
            emp3.doubleSalary = 1000;
            emp3.EmployeeJob = Job.Boss;

            var list = new List<Employee>();
            list.Add(emp1);
            list.Add(emp2);
            list.Add(emp3);

            return list;
        }
    }
}