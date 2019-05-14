namespace AntiPatternsCodeSmells.Switch
{
    public class SortService
    {
        public string[] Sort(SortType sortType, string[] list)
        {
            switch (sortType)
            {
                case SortType.BubbleSort:
                    return new BubbleSort().Sort(list);

                case SortType.HeapSort:
                    return new HeapSort().SortWithHeapSort(list);

                case SortType.MergeSort:
                    return new MergeSort().Sort(list);
            }
        }
    }
}