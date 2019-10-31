﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Resources;
using SINTEF.AutoActive.Databus.Interfaces;
using SINTEF.AutoActive.Databus.ViewerContext;
using SINTEF.AutoActive.UI.Helpers;
using SINTEF.AutoActive.UI.Interfaces;
using SINTEF.AutoActive.UI.Views;
using Xamarin.Forms;

namespace SINTEF.AutoActive.UI.Pages.Synchronization
{
    public partial class PointSynchronizationPage : ContentPage, IFigureContainer
    {
        // If start differ by more than this, assume data sets are not synchronized.
        public double OffsetBeforeZeroing = 36000; // 10 hrs [s]

        private readonly TimeSynchronizedContext _masterContext = new TimeSynchronizedContext();
        private bool _masterSet;
        private bool _slaveSet;
        private ITimePoint _masterTime;
        private ITimePoint _slaveTime;
        private RelativeSlider _slaveSlider;
        private SynchronizationContext _slaveContext;

        private long? _selectedMasterTime;
        private long? _selectedSlaveTime;

        public PointSynchronizationPage()
        {
            InitializeComponent();
        }

        protected override void OnAppearing()
        {
            base.OnAppearing();
            TreeView.DataPointTapped += TreeView_DataPointTapped;
            _masterContext.SetSynchronizedToWorldClock(true);
            _slaveSlider = new RelativeSlider {MinimumHeightRequest = 30};
            _slaveSlider.OffsetChanged += SlaveSliderOnOffsetChanged;

            Playbar.ViewerContext = _masterContext;
            Playbar.DataTrackline.RegisterFigureContainer(this);
        }

        protected override void OnDisappearing()
        {
            base.OnDisappearing();
            TreeView.DataPointTapped -= TreeView_DataPointTapped;
            Playbar.DataTrackline.DeregisterFigureContainer(this);
            _slaveSlider.OffsetChanged -= SlaveSliderOnOffsetChanged;
        }

        private static IEnumerable<FigureView> GetFigureViewChildren(StackLayout masterLayout)
        {
            foreach (var child in masterLayout.Children)
            {
                if (child is FigureView figure)
                {
                    yield return figure;
                }
            }
        }

        private void Reset()
        {
            _masterSet = false;
            _masterTime = null;
            _selectedMasterTime = 0L;
            MasterTimeButton.Text = "Unset";

            foreach (var figure in GetFigureViewChildren(MasterLayout))
            {
                foreach (var datapoint in figure.DataPoints)
                {
                    DatapointRemoved?.Invoke(this, (datapoint, _masterContext));
                }
                MasterLayout.Children.Clear();
            }
        }

        private void ResetSlave()
        {
            _slaveSet = false;
            _slaveTime = null;
            _selectedSlaveTime = 0L;
            SlaveTimeButton.Text = "Unset";

            foreach (var figure in GetFigureViewChildren(SlaveLayout))
            {
                foreach (var datapoint in figure.DataPoints)
                {
                    DatapointRemoved?.Invoke(this, (datapoint, _slaveContext));
                }
                SlaveLayout.Children.Clear();
            }
        }

        private void SlaveTimeButton_OnClicked(object sender, EventArgs e)
        {
            if (_slaveContext == null) return;
            _selectedSlaveTime = _slaveContext.SelectedTimeFrom;
            SlaveTimeButton.Text = TimeFormatter.FormatTime(_selectedSlaveTime.Value, dateSeparator:' ');
        }

        private void MasterTimeButton_OnClicked(object sender, EventArgs e)
        {
            _selectedMasterTime = _masterContext.SelectedTimeFrom;
            MasterTimeButton.Text = TimeFormatter.FormatTime(_selectedMasterTime.Value, dateSeparator: ' ');
        }

        private async void Sync_OnClicked(object sender, EventArgs e)
        {
            if (!_selectedMasterTime.HasValue || !_selectedSlaveTime.HasValue)
            {
                await DisplayAlert("Unset sync time", "A point in both the master time and the slave time must be set.", "OK");
                return;
            }
            _slaveSlider.Offset = TimeFormatter.SecondsFromTime(_selectedSlaveTime.Value - _selectedMasterTime.Value);
        }

        private FigureView _selected;
        public FigureView Selected
        {
            get => _selected;
            set
            {
                if (_selected != null) _selected.Selected = false;
                _selected = value;
                if (_selected != null) _selected.Selected = true;
            }
        }

        public event EventHandler<(IDataPoint, DataViewerContext)> DatapointAdded;
        public event EventHandler<(IDataPoint, DataViewerContext)> DatapointRemoved;

        private async void SetMaster(IDataPoint dataPoint)
        {
            var masterFigure = await FigureView.GetView(dataPoint, _masterContext);
            masterFigure.ContextButtonIsVisible = true;
            MasterLayout.Children.Add(masterFigure);

            _masterTime = dataPoint.Time;
            _masterSet = true;

            DatapointAdded?.Invoke(this, (dataPoint, _masterContext));
        }

        private void SlaveSliderOnOffsetChanged(object sender, ValueChangedEventArgs args)
        {
            _slaveContext.Offset = TimeFormatter.TimeFromSeconds(args.NewValue);
            Playbar.DataTrackline.InvalidateSurface();
        }

        public void InvokeDatapointRemoved(IDataPoint dataPoint, DataViewerContext context)
        {
            DatapointRemoved?.Invoke(this, (dataPoint, context));
        }
        private async void TreeView_DataPointTapped(object sender, IDataPoint datapoint)
        {
            if (!_masterSet)
            {
                SetMaster(datapoint);
                return;
            }

            var isMaster = datapoint.Time == _masterTime;

            if (!isMaster && !_slaveSet)
            {
                _slaveSet = true;
                _slaveTime = datapoint.Time;
                _slaveContext = new SynchronizationContext(_masterContext);
                var offset =
                    TimeFormatter.SecondsFromTime(_masterContext.AvailableTimeFrom - _slaveContext.AvailableTimeFrom);
                if (Math.Abs(offset) > OffsetBeforeZeroing)
                    _slaveSlider.Offset = -offset;
                SlaveLayout.Children.Add(_slaveSlider);
            }

            if (_slaveSet && !isMaster && datapoint.Time != _slaveTime)
            {
                await DisplayAlert("Illegal datapoint selected", "Can only show data sets with common time", "OK");
                return;
            }

            TimeSynchronizedContext context;
            StackLayout layout;
            if (isMaster)
            {
                context = _masterContext;
                layout = MasterLayout;
            } else
            {
                context = _slaveContext;
                layout = SlaveLayout;
            }

            if (Selected != null)
            {
                var result = await Selected.ToggleDataPoint(datapoint, context);
                switch (result)
                {
                    case ToggleResult.Added:
                        DatapointAdded?.Invoke(sender, (datapoint, context));
                        break;
                    case ToggleResult.Removed:
                        DatapointRemoved?.Invoke(sender, (datapoint, context));
                        break;
                    case ToggleResult.Cancelled:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                return;
            }

            var figure = await FigureView.GetView(datapoint, context);
            layout.Children.Add(figure);
            DatapointAdded?.Invoke(sender, (datapoint, context));
        }

        public void RemoveChild(FigureView figureView)
        {

            if (figureView.Parent == MasterLayout)
            {
                MasterLayout.Children.Remove(figureView);
            }
            else if (figureView.Parent == SlaveLayout)
            {
                SlaveLayout.Children.Remove(figureView);
            }
            else
            {
                Debug.WriteLine("Could not remove frame from layout.");
            }
            
            foreach (var dataPoint in figureView.DataPoints)
            {
                DatapointRemoved?.Invoke(this, (dataPoint, figureView.Context));
            }
        }

        private void Save_OnClicked(object sender, EventArgs e)
        {
            var extraOffset = 0L;
#if VIDEO_TIME_COMPENSATION
            if (_masterTime is ArchiveVideoTime videoTime)
            {
                //TODO: Check sign of this
                extraOffset = videoTime.VideoPlaybackOffset;
            }
#endif
            _slaveTime.TransformTime(-(_slaveContext.Offset + extraOffset), _slaveContext.Scale);
            _slaveContext.Offset = 0;
        }

        private static long GetOffsetFromTimeStep(TimeStepEvent timeStep)
        {
            long offset;
            switch (timeStep.Length)
            {
                case TimeStepLength.Step:
                    offset = TimeFormatter.TimeFromSeconds(1d / 30);
                    break;
                case TimeStepLength.Short:
                    offset = TimeFormatter.TimeFromSeconds(1);
                    break;
                case TimeStepLength.Large:
                    offset = TimeFormatter.TimeFromSeconds(10);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            if (timeStep.Direction == TimeStepDirection.Backward)
            {
                offset = -offset;
            }

            return offset;
        }

        private void MasterTimeStepper_OnOnStep(object sender, TimeStepEvent e)
        {
            var context = _masterContext;
            var diff = context.SelectedTimeTo - context.SelectedTimeFrom;

            var offset = GetOffsetFromTimeStep(e);
            var from = context.SelectedTimeFrom + offset;
            var to = from + diff;

            context.SetSelectedTimeRange(from, to);
        }

        private void SlaveTimeStepper_OnOnStep(object sender, TimeStepEvent e)
        {
            var context = _slaveContext;
            var diff = context.SelectedTimeTo - context.SelectedTimeFrom;

            var offset = GetOffsetFromTimeStep(e);
            var from = context.SelectedTimeFrom + offset;
            var to = from + diff;

            context.SetSelectedTimeRange(from, to);
        }

        private void Reset_OnClicked(object sender, EventArgs e)
        {
            Reset();
        }

        private void ResetSlave_OnClicked(object sender, EventArgs e)
        {
            ResetSlave();
        }
    }
}