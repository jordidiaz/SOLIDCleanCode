using System.Collections.Generic;

namespace AntiPatternsCodeSmells.Poltergeist
{
    public class SuperList<T>
    {
        private readonly List<T> _list;

        public SuperList()
        {
            _list = new List<T>();
        }

        public int Count()
        {
            return _list.Count;
        }

        public void Add(T item)
        {
            _list.Add(item);
        }

        public void Clear()
        {
            _list.Clear();
        }
    }
}