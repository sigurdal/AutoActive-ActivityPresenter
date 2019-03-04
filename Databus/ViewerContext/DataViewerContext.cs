﻿using SINTEF.AutoActive.Databus.Interfaces;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SINTEF.AutoActive.Databus.ViewerContext
{
    // FIXME: Do we need thread-safety in these classes?

    public delegate void DataViewerContextWorldClockChangedHandler(DataViewerContext sender, bool synchronizedToWorldClock);
    public delegate void DataViewerContextAvailableTimeRangeChangedHandler(DataViewerContext sender, long from, long to);

    public abstract class DataViewerContext
    {
        // --- Clock mode ---
        private bool _isSynchronizedToWorldClock;
        protected bool IsSynchronizedToWorldClock
        {
            get => _isSynchronizedToWorldClock;
            set
            {
                _isSynchronizedToWorldClock = value;
                SynchronizedToWorldClockChanged?.Invoke(this, value);
            }
        }

        public event DataViewerContextWorldClockChangedHandler SynchronizedToWorldClockChanged;

        // --- Available time range ---
        public long AvailableTimeFrom { get; private set; }
        public long AvailableTimeTo { get; private set; }
        public event DataViewerContextAvailableTimeRangeChangedHandler AvailableTimeRangeChanged;
        protected bool InternalSetAvailableTimeRange(long from, long to)
        {
            if (AvailableTimeFrom == from && AvailableTimeTo == to) return false;

            AvailableTimeFrom = from;
            AvailableTimeTo = to;
            AvailableTimeRangeChanged?.Invoke(this, from, to);
            return true;
        }
        public long SelectedTimeFrom { get; protected set; }
        public long SelectedTimeTo { get; protected set; }
    }

    public delegate void DataViewerContextSelectedRangeChangedHandler(SingleSetDataViewerContext sender, long from, long to);

    public abstract class SingleSetDataViewerContext : DataViewerContext
    {
        // ---- Data _viewers ----
        private readonly Dictionary<ITimeViewer, List<IDataViewer>> _viewers = new Dictionary<ITimeViewer, List<IDataViewer>>();

        private void SetTimeRangeForViewer(ITimeViewer timeviewer, IDataViewer dataviewer, long from, long to)
        {
            if (IsSynchronizedToWorldClock) dataviewer.SetTimeRange(from, to);
            else dataviewer.SetTimeRange(from + timeviewer.Start, to + timeviewer.Start);
        }

        protected async Task<IDataViewer> InternalGetOrAddViewerFor(IDataPoint datapoint)
        {
            foreach (var timeDataViewers in _viewers)
            {
                var timeviewer = timeDataViewers.Key;
                if (timeviewer.TimePoint != datapoint.Time) continue;

                // We already have a time viewer for this datapoint, check if we also have a dataviewer
                foreach (var dataviewer in timeDataViewers.Value)
                {
                    if (dataviewer.DataPoint == datapoint)
                    {
                        return dataviewer;
                    }
                }
                // If not, create one
                var newViewer = await datapoint.CreateViewer();
                timeDataViewers.Value.Add(newViewer);
                SetTimeRangeForViewer(timeviewer, newViewer, SelectedTimeFrom, SelectedTimeTo);
            }

            // If we don't have a timeviewer, we need to create both
            var newDataviewer = await datapoint.CreateViewer();
            var newTimeviewer = await datapoint.Time.CreateViewer();
            _viewers.Add(newTimeviewer, new List<IDataViewer> { newDataviewer });
            newTimeviewer.TimeChanged += OnTimeViewerAvailableChanged;
            OnTimeViewerAvailableChanged(newTimeviewer, newTimeviewer.Start, newTimeviewer.End);

            return newDataviewer;
        }

        protected abstract void OnTimeViewerAvailableChanged(ITimeViewer sender, long start, long end);

        // --- Selected time range ---
        private int _lastViewers = -1;


        public event DataViewerContextSelectedRangeChangedHandler SelectedTimeRangeChanged;
        protected void InternalSetSelectedTimeRange(long from, long to)
        {
            var nViewers = _viewers.Count;
            if (SelectedTimeFrom != from || SelectedTimeTo != to || _lastViewers != nViewers)
            {
                SelectedTimeFrom = from;
                SelectedTimeTo = to;

                // TODO: Maybe we should set the mapping instead of doing this every time
                foreach (var timeDataViewers in _viewers)
                {
                    var timeviewer = timeDataViewers.Key;
                    foreach (var dataviewer in timeDataViewers.Value)
                        SetTimeRangeForViewer(timeviewer, dataviewer, from, to);
                }

                SelectedTimeRangeChanged?.Invoke(this, from, to);
                _lastViewers = nViewers;
            }
        }

        // --- Available time range based on current time _viewers ---
        protected void GetAvailableTimeMinMax(bool useWorldClock, out ITimeViewer minViewer, out long min, out ITimeViewer maxViewer, out long max)
        {
            if (_viewers.Count == 0)
            {
                minViewer = maxViewer = null;
                min = max = 0;
                return;
            }

            // Calculate the min max of current timeviewers
            minViewer = maxViewer = null;
            min = long.MaxValue;
            max = long.MinValue;

            foreach (var timeDataViewers in _viewers)
            {
                var timeViewer = timeDataViewers.Key;
                var start = useWorldClock ? timeViewer.Start : 0;
                var end = useWorldClock ? timeViewer.End : timeViewer.End - timeViewer.Start;
                if (start < min)
                {
                    min = start;
                    minViewer = timeViewer;
                }
                if (end > max)
                {
                    max = end;
                    maxViewer = timeViewer;
                }
            }
        }

        private void UpdateSetAvailableTimeRange()
        {
            GetAvailableTimeMinMax(IsSynchronizedToWorldClock, out var minViewer, out var min, out var maxViewer, out var max);
            InternalSetAvailableTimeRange(min, max);
        }

        public void Remove(IDataViewer dataViewer)
        {
            var toRemove = new List<ITimeViewer>();
            foreach (var item in _viewers)
            {
                item.Value.Remove(dataViewer);
                if (!item.Value.Any())
                {
                    toRemove.Add(item.Key);
                }
            }

            foreach (var viewer in toRemove)
            {
                _viewers.Remove(viewer);
            }

            UpdateSetAvailableTimeRange();
        }
    }
}
