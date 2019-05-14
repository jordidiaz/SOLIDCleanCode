using System.Collections.Generic;
using System.Text;

namespace SOLIDBad._3_LSP
{
    public class FileManager2
    {
        public string GetTextFromFiles(IEnumerable<File> files)
        {
            var objStrBuilder = new StringBuilder();
            foreach (var file in files)
            {
                objStrBuilder.Append(file.LoadText());
            }
            return objStrBuilder.ToString();
        }

        public void SaveTextIntoFiles(IEnumerable<File> files, string text)
        {
            foreach (var file in files)
            {
                if (!(file is ReadOnlyFile))
                {
                    file.SaveText(text);
                }
            }
        }
    }
}