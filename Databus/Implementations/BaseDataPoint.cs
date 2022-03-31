using SINTEF.AutoActive.Databus.Common;
using SINTEF.AutoActive.Databus.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SINTEF.AutoActive.Databus.Implementations
{
    // To simplify generic type matching
    public interface IBaseDataPoint : IDataPoint { }

    public class BaseDataPoint<T> : IBaseDataPoint where T : IConvertible
    {
        public virtual T[] Data { get; protected set; }
        protected Task<T[]> DataLoader;
        public BaseDataPoint(string name, Task<T[]> loader, BaseTimePoint time, string uri, string unit)
        {
            Name = name;
            DataLoader = loader;
            URI = uri;
            DataType = typeof(T);
            Time = time;
            Unit = unit;
        }

        public BaseDataPoint(string name, T[] data, BaseTimePoint time, string uri, string unit)
        {
            Data = data;
            URI = uri;
            DataType = typeof(T);
            Name = name;
            Time = time;
            Unit = unit;
        }
        public string URI { get; set; }

        public Type DataType { get; }

        public string Name { get; set; }

        public BaseTimePoint Time { get; }
        ITimePoint IDataPoint.Time => Time;

        public string Unit { get; set; }

        private List<BaseDataViewer> _viewers = new List<BaseDataViewer>();
        public bool HasViewers() { 
            return _viewers.Count > 0;
        }

        public event EventHandler DataChanged;

        protected virtual async Task<BaseDataViewer> CreateDataViewer()
        {
            return new BaseDataViewer(new BaseTimeViewer(Time), this);
        }

        public async Task<IDataViewer> CreateViewer()
        {
            if (Data == null && DataLoader != null)
            {
                Data = await DataLoader;
            }

            var viewer = await CreateDataViewer();
            _viewers.Add(viewer);
            return viewer;
        }

        public virtual void TriggerDataChanged()
        {
            foreach(var viewer in _viewers)
            {
                viewer.TriggerDataChanged();
            }
            DataChanged?.Invoke(this, EventArgs.Empty);
        }
    }

    public class BaseDataViewer : IDataViewer
    {
        protected ITimeViewer TimeViewer { get; }
        public BaseDataViewer(ITimeViewer timeViewer, IDataPoint dataPoint)
        {
            TimeViewer = timeViewer;
            DataPoint = dataPoint;
        }
        public IDataPoint DataPoint { get; internal set; }

        public long CurrentTimeRangeFrom { get; private set; }

        public long CurrentTimeRangeTo { get; private set; }

        public long PreviewPercentage { get; set; }

        public event EventHandler Changed;

        public virtual void SetTimeRange(long from, long to)
        {
            var diff = to - from;
            var startTime = from - diff * PreviewPercentage / 100;
            var endTime = from + diff;

            CurrentTimeRangeFrom = from;
            CurrentTimeRangeTo = to;

            Changed?.Invoke(this, EventArgs.Empty);
        }

        internal void TriggerDataChanged()
        {
            Changed?.Invoke(this, EventArgs.Empty);
        }
    }

    public abstract class BaseTimeSeriesViewer : BaseDataViewer, ITimeSeriesViewer
    {
        protected new BaseTimeViewer TimeViewer { get; }
        public BaseTimeSeriesViewer(BaseTimeViewer timeViewer, IDataPoint dataPoint) : base(timeViewer, dataPoint)
        {
            TimeViewer = timeViewer;
        }

        public double? MinValueHint => null;

        public double? MaxValueHint => null;

        public int StartIndex { get; private set; }
        public int EndIndex { get; private set; }
        protected int Length => EndIndex - StartIndex + 1;

        public override void SetTimeRange(long from, long to)
        {
            var diff = to - from;
            var startTime = from - diff * PreviewPercentage / 100;
            var endTime = from + diff;

            var start = FindClosestIndex(startTime);
            var end = Math.Min(FindClosestIndex(endTime) + 1, TimeViewer.Data.Length - 1);

            if (start == StartIndex && end == EndIndex) return;

            StartIndex = start;
            EndIndex = end;

            base.SetTimeRange(from, to);
        }

        public abstract SpanPair<T1> GetCurrentData<T1>() where T1 : IConvertible;

        private int FindClosestIndex(long value)
        {
            var index = Array.BinarySearch(TimeViewer.Data, value);

            // BinarySearch returns a 2's complement if the value was not found.
            if (index < 0) index = ~index;

            return index;
        }

        public SpanPair<bool> GetCurrentBools() { throw new NotImplementedException(); }
        public SpanPair<string> GetCurrentStrings() { throw new NotImplementedException(); }
    }


    public class GenericTimeSeriesViewer<T> : BaseTimeSeriesViewer, ITimeSeriesViewer where T : IConvertible
    {
        private readonly BaseDataPoint<T> _dataPoint;
        public GenericTimeSeriesViewer(BaseTimeViewer timeViewer, BaseDataPoint<T> dataPoint) : base(timeViewer, dataPoint)
        {
            _dataPoint = dataPoint;
        }

        public override SpanPair<T1> GetCurrentData<T1>()
        {
            if (typeof(T1) != typeof(T))
                throw new ArgumentException();
            if (_dataPoint.Data.Length <= 0) return new SpanPair<T1>();

            var elements = _dataPoint.Data;

            var startIndex = 0;

            Span<T1> data;
            unsafe
            {
                var mem = elements.AsMemory(StartIndex, Length);
                using (var pin = mem.Pin())
                    data = new Span<T1>(pin.Pointer, Length);
            }
            return new SpanPair<T1>(startIndex, TimeViewer.Data.AsSpan(startIndex, Length), data);
        }
    }
}
