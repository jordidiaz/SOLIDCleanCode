namespace SOLID._3
{
    public class NotificationService
    {
        private Email _email;
        private SMS _sms;

        public NotificationService()
        {
            _email = new Email();
            _sms = new SMS();
        }

        public void Send()
        {
            _email.SendEmail();
            _sms.SendSMS();
        }
    }
}