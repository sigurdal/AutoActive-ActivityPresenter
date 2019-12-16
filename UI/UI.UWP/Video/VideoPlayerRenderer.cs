﻿using System;
using System.IO;
using System.Threading.Tasks;
using Windows.Storage.Streams;
using Windows.UI.Xaml.Controls;
using SINTEF.AutoActive.FileSystem;
using SINTEF.AutoActive.UI;
using SINTEF.AutoActive.UI.UWP.Views;
using Xamarin.Forms.Platform.UWP;
using System.Collections.Generic;
using System.Linq;
using Windows.UI.Xaml.Media;

[assembly: ExportRenderer(typeof(VideoPlayer), typeof(VideoPlayerRenderer))]

namespace SINTEF.AutoActive.UI.UWP.Views
{
    public class VideoPlayerRenderer : ViewRenderer<VideoPlayer, MediaElement>
    {
        public static async Task<IRandomAccessStream> GetVideoStream(IReadSeekStreamFactory factory)
        {
            var stream = await factory.GetReadStream();
            return stream.AsRandomAccessStream();
        }

        private MediaElement _mediaElement;
        private VideoPlayer _videoPlayer;
        private TimeCompensator _timeCompensator;

        protected override async void OnElementChanged(ElementChangedEventArgs<VideoPlayer> args)
        {
            base.OnElementChanged(args);

            if (args.OldElement != null)
            {
                var oldVideoPlayer = args.OldElement;
                oldVideoPlayer.PositionChanged -= VideoPlayerOnPositionChanged;
                oldVideoPlayer.PlayingChanged -= VideoPlayerOnPlayingChanged;
                oldVideoPlayer.PlaybackRateChanged -= PlaybackRateChanged;
            }

            if (args.NewElement == null) return;

            if (Control == null)
            {
                _mediaElement = new MediaElement();
                SetNativeControl(_mediaElement);

                _mediaElement.PlaybackRate = 1d;
                _mediaElement.AutoPlay = false;
                _mediaElement.IsMuted = true;
                _mediaElement.Volume = 0d;
            }

            _videoPlayer = args.NewElement;
            _timeCompensator = new TimeCompensator(_videoPlayer.AllowedOffset);

            _mediaElement.AutoPlay = _videoPlayer.IsPlaying;
            Control.SetSource(await GetVideoStream(_videoPlayer.Source), _videoPlayer.MimeType);


            _videoPlayer.PositionChanged += VideoPlayerOnPositionChanged;
            _videoPlayer.PlayingChanged += VideoPlayerOnPlayingChanged;
            _videoPlayer.PlaybackRateChanged += PlaybackRateChanged;
            PlaybackRateChanged(this, _videoPlayer.PlaybackRate);

            if (_videoPlayer.IsPlaying)
            {
                _mediaElement.Play();
            }

            _currentlyPlaying = _videoPlayer.IsPlaying;
        }

        private void PlaybackRateChanged(object sender, double rate)
        {
            if (rate == 0d) rate = 1d;
            _mediaElement.DefaultPlaybackRate = rate;
            _mediaElement.PlaybackRate = rate;
        }

        private bool _currentlyPlaying;
        private void VideoPlayerOnPlayingChanged(object sender, bool isPlaying)
        {
            _currentlyPlaying = isPlaying;

            if (isPlaying)
                _mediaElement.Play();
            else _mediaElement.Pause();
        }

        private void SetVideoPosition(TimeSpan wantedPosition, double allowedOffset)
        {
            var duration = _mediaElement.NaturalDuration.TimeSpan;
            if (duration == TimeSpan.Zero) return;

            if (wantedPosition > duration)
            {
                _mediaElement.Pause();
                if (_mediaElement.Position < duration)
                {
                    _mediaElement.Position = duration;
                }
                return;
            }

            if (wantedPosition.TotalSeconds < 0)
            {
                _mediaElement.Position = TimeSpan.Zero;
                _mediaElement.Pause();
                return;
            }

            if (!_currentlyPlaying)
            {
                _mediaElement.Position = wantedPosition;
                _videoPlayer.CurrentOffset = 0;
                _timeCompensator.Compensator = 0;
                return;
            }

            if (_timeCompensator.UpdateTimeDiff(_mediaElement.Position, wantedPosition, _mediaElement.DefaultPlaybackRate, out var diff, out var offsetComp))
            {
                _mediaElement.Position = offsetComp;
                _videoPlayer.Compensator = _timeCompensator.Compensator;
            }
            _videoPlayer.CurrentOffset = diff;


            // Only ensure play state if the time is not later than the duration
            if (_currentlyPlaying && wantedPosition < _mediaElement.NaturalDuration)
            {
                if(_mediaElement.CurrentState != MediaElementState.Playing)
                    _mediaElement.Play();
            } else
            {
                _mediaElement.Pause();
            }
        }

        private void VideoPlayerOnPositionChanged(object sender, PositionChangedEventArgs args)
        {
            var allowedOffset = _videoPlayer.AllowedOffset;
            XamarinHelpers.EnsureMainThread(() => SetVideoPosition(args.Time, allowedOffset));
        }
    }

    internal class TimeCompensator
    {
        private double _compensator;

        public double Compensator
        {
            get => _compensator;
            set
            {
                _compensator = value;
                _diffQueue.Clear();
                _lastUpdate = DateTime.Now;
            }
        }

        private const int OffsetQueueElements = 120;
        private readonly Queue<double> _diffQueue = new Queue<double>(OffsetQueueElements);

#if USE_MAX_DIFF_COMP
        // The difference between the first element in the queue and all the others must be less than this before updating
        public static double MaxDiffComp = 0.3d;
#endif

        // The variance between the first element and all the others must be less than this before updating
        public static double MaxVarianceDiff = 0.1d * 0.1d;

        // The difference between the new and old offset must be larger than this before updating the offset
        public static double OffsetEqualComp = 0.01d;

        // The difference between the wanted time and the current time must be larger than this to change the offset
        public static double MinChangingOffset = 0.01d;

        // The offset can't be larger than this
        public static double MaxAllowedOffset;

        private DateTime _lastDiffTime;
        private DateTime _lastUpdate;
        public TimeSpan MinUpdateDelta = new TimeSpan(0, 0, 2);

        public TimeCompensator(double maxAllowedOffset)
        {
            MaxAllowedOffset = maxAllowedOffset;
            _lastUpdate = DateTime.Now;
        }

        private bool GetOffset(double playbackRate, out double offsetCompensator)
        {
            offsetCompensator = 0;
            if (_diffQueue.Count != OffsetQueueElements)
            {
                return false;
            }

            var avg = _diffQueue.Average();
            // The difference is already low, just keep it
            if (Math.Abs(avg) < playbackRate * MinChangingOffset)
            {
                return false;
            }

            var first = _diffQueue.Peek();

#if USE_MAX_DIFF_COMP
            if (!_diffQueue.All(el => Math.Abs(first - el) < MaxDiffComp)) return true;
#endif

            var variance = _diffQueue.Sum(el => (el - first) * (el - first));
            var newOffset = _diffQueue.Average();

            if (variance > MaxVarianceDiff)
                return false;

            offsetCompensator = Compensator - newOffset;
            var absoluteOffset = Math.Abs(offsetCompensator);
            return absoluteOffset > OffsetEqualComp;
        }

        public bool UpdateTimeDiff(TimeSpan position, TimeSpan wantedPosition, double playbackRate, out double diff, out TimeSpan retOffset)
        {
            var now = DateTime.Now;
            var expectedDiff = now - _lastDiffTime;
            _lastDiffTime = now;

            diff = (position - wantedPosition).TotalSeconds - expectedDiff.TotalSeconds;

            var offset = Math.Abs(diff);

            if (offset > MaxAllowedOffset)
            {
                retOffset = wantedPosition;
                return true;
            }

            if (_diffQueue.Count >= OffsetQueueElements)
                _diffQueue.Dequeue();

            _diffQueue.Enqueue(diff);

            var offsetChanged = GetOffset(playbackRate, out var offsetCompensator);

            // A possibility here would be to estimate the expected offset and compensate for it
            if (now - _lastUpdate <= MinUpdateDelta || !offsetChanged) return false;

            if (Math.Abs(offsetCompensator) > MaxAllowedOffset)
            {
                Compensator = 0;
            }
            else
            {
                Compensator = offsetCompensator;
            }

            retOffset = wantedPosition.Add(new TimeSpan((long)(Compensator * 10000000L)));
            return true;
        }
    }
}

