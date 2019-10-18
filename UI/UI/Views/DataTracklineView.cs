﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using SINTEF.AutoActive.Databus.Interfaces;
using SINTEF.AutoActive.Databus.ViewerContext;
using SINTEF.AutoActive.UI.Interfaces;
using SkiaSharp;
using SkiaSharp.Views.Forms;
using Xamarin.Forms.Xaml;

namespace SINTEF.AutoActive.UI.Views
{
    [DesignTimeVisible(true)]
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public class DataTracklineView : SKCanvasView
    {

        private readonly List<(ITimeViewer, TimeSynchronizedContext, string)> _timeViewers = new List<(ITimeViewer, TimeSynchronizedContext, string)>();
        private readonly List<(IDataPoint, ITimeViewer)> _dataTimeList = new List<(IDataPoint, ITimeViewer)>();

        public void AddTimeViewer(ITimeViewer viewer, TimeSynchronizedContext context, string label)
        {
            _timeViewers.Add((viewer, context, label));
            viewer.TimeChanged += ViewerOnTimeChanged;
            InvalidateSurface();
        }

        private void ViewerOnTimeChanged(ITimeViewer sender, long start, long end)
        {
            InvalidateSurface();
        }

        private void RemoveTimeViewer(ITimeViewer timeViewer)
        {
            _timeViewers.ForEach(el => el.Item1.TimeChanged -= ViewerOnTimeChanged);
            _timeViewers.RemoveAll(el => el.Item1 == timeViewer);
        }

        public DataTracklineView()
        {
            PaintSurface += OnPaintSurface;
            WidthMargins = 10;
        }

        public int WidthMargins { get; set; }

        protected void OnPaintSurface(object sender, SKPaintSurfaceEventArgs e)
        {
            var canvas = e.Surface.Canvas;

            canvas.Clear(SKColors.White);

            var height = e.Info.Height;
            var width = e.Info.Width;

            var drawRect = new SKRect(0, 0, width, height);

            var borderPaint = new SKPaint
            {
                Color = SKColors.Black,
                Style = SKPaintStyle.Stroke,
                StrokeWidth = 1
            };
            drawRect.Right -= 1;
            drawRect.Bottom -= 1;

            canvas.DrawRect(drawRect, borderPaint);

            drawRect.Left = WidthMargins;
            drawRect.Right -= WidthMargins;
            drawRect.Top = 2;
            drawRect.Bottom -= 2;

            var trans = SKMatrix.MakeTranslation(drawRect.Left, drawRect.Top);
            canvas.SetMatrix(trans);

            DrawDataSegments(e.Surface.Canvas, drawRect, _timeViewers);
        }

        private void DrawDataSegments(SKCanvas canvas, SKRect drawRect, IReadOnlyCollection<(ITimeViewer, TimeSynchronizedContext, string)> timeViewers)
        {
            if (timeViewers.Count == 0) return;

            const float labelXMargin = 4f;
            const float boxRoundnessX = 5f;
            const float boxRoundnessY = boxRoundnessX;
            const float minTextSize = 10f;
            const float yMargin = 2f;
            const float maxTrackHeight = 30f;


            var times = new List<(long, long, string)>();
            foreach (var (viewer, context, label) in timeViewers)
            {
                var (start, end) = context.GetAvailableTimeInContext(viewer);
                times.Add((start, end, label));
            }

            var xMin = times.Min(el => el.Item1);
            var xMax = times.Max(el => el.Item2);
            var xDiff = xMax - xMin;
            var xScale = drawRect.Width / xDiff;

            var nLines = _timeViewers.Count;
            var yHeight = (drawRect.Height) / nLines - yMargin;

            if (yHeight > maxTrackHeight) yHeight = maxTrackHeight;


            var boldFont = SKTypeface.FromFamilyName(SKTypeface.Default.FamilyName, SKFontStyle.Bold);

            var paint = new SKPaint
            {
                Color = SKColors.Red,
                StrokeWidth = 1,
                Style = SKPaintStyle.Fill,
                IsAntialias = true
            };

            var textPaint = new SKPaint
            {
                Color = SKColors.Black,
                Style = SKPaintStyle.Fill,
                IsAntialias = true,
                SubpixelText = true,
                Typeface = boldFont
            };

            var fontHeight = textPaint.FontMetrics.Bottom - textPaint.FontMetrics.Top;
            var textSize = textPaint.TextSize;
            if (yHeight < Math.Abs(fontHeight))
            {
                textSize = Math.Abs(yHeight / fontHeight * textPaint.TextSize);
            }

            textPaint.TextSize = Math.Max(textSize, minTextSize);

            var fontBottom =
                Math.Abs(textPaint.FontMetrics.Bottom - textPaint.FontMetrics.Top) < yHeight
                    ? yHeight / 2 - textPaint.FontMetrics.Bottom
                    : 0f;

            var yPos = yMargin;
            foreach (var (start, end, label) in times)
            {
                var xPos = (start - xMin) * xScale;
                var width = Math.Max((end - start) * xScale, 1f);
                canvas.DrawRoundRect(xPos, yPos, width, yHeight, boxRoundnessX, boxRoundnessY, paint);
                canvas.DrawText(label, xPos + labelXMargin, yPos + yHeight - fontBottom, textPaint);

                yPos += yHeight + yMargin;
            }
        }

        public async Task AddDataPoint(IDataPoint dataPoint, TimeSynchronizedContext context)
        {
            if (!_dataTimeList.Any())
            {
                context.AvailableTimeRangeChanged += ContextOnAvailableTimeRangeChanged;
            }

            var dataViewer = await context.GetDataViewerFor(dataPoint);
            var timeViewer = await dataViewer.DataPoint.Time.CreateViewer();
            _dataTimeList.Add((dataPoint, timeViewer));
            AddTimeViewer(timeViewer, context, dataPoint.Name);
            timeViewer.TimeChanged += TimeViewer_TimeChanged;
        }

        private void ContextOnAvailableTimeRangeChanged(DataViewerContext sender, long @from, long to)
        {
            InvalidateSurface();
        }

        private void TimeViewer_TimeChanged(ITimeViewer sender, long start, long end)
        {
            InvalidateSurface();
        }

        public Task RemoveDataPoint(IDataPoint dataPoint, TimeSynchronizedContext context)
        {
            var index = _dataTimeList.FindIndex(el => el.Item1 == dataPoint);

            if (index == -1)
                return Task.CompletedTask;

            var (_, timeViewer) = _dataTimeList[index];
            _dataTimeList.RemoveAt(index);
            if (_dataTimeList.Count == 0)
            {
                context.AvailableTimeRangeChanged -= ContextOnAvailableTimeRangeChanged;
            }

            timeViewer.TimeChanged -= TimeViewer_TimeChanged;
            RemoveTimeViewer(timeViewer);
            InvalidateSurface();
            return Task.CompletedTask;
        }


        public void RegisterFigureContainer(IFigureContainer container)
        {
            container.DatapointAdded += async (_, args) =>
            {
                var (datapoint, context) = args;
                if (context is TimeSynchronizedContext timeContext)
                    await AddDataPoint(datapoint, timeContext);
            };
            container.DatapointRemoved += (_, args) =>
            {
                var (datapoint, context) = args;
                if (context is TimeSynchronizedContext timeContext)
                    RemoveDataPoint(datapoint, timeContext);
            };
        }
    }
}