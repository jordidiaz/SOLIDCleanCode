namespace SOLID._1
{
    internal class UserService
    {
        public bool CanMakeAPayment(User user)
        {
            return true;
        }

        public bool CanMakeAPayment(Admin admin)
        {
            return true;
        }
    }
}