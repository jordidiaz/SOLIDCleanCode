using System;

namespace SOLIDBad._4_ISP
{
    public class Boss : ILead
    {
        public void CreateTask()
        {
        }

        public void AssignTask()
        {
        }

        public void WorkOnTask()
        {
            throw new Exception("The Boss does not work on the tasks!");
        }
    }
}