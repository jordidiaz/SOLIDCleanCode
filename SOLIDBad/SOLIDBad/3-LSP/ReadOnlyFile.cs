using System;

namespace SOLIDBad._3_LSP
{
    public class ReadOnlyFile : File
    {
        public override void SaveText(string text)
        {
            throw new Exception("Can't Save");
        }
    }
}