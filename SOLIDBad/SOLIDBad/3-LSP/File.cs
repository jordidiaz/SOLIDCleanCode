namespace SOLIDBad._3_LSP
{
    public class File
    {
        public string Path { get; set; }

        public string Text { get; set; }

        public string LoadText()
        {
            return string.Empty;
        }

        public virtual void SaveText(string text)
        {
        }
    }
}