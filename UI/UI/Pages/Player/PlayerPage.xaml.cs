﻿using SINTEF.AutoActive.Databus.Interfaces;
using SINTEF.AutoActive.Databus.ViewerContext;
using SINTEF.AutoActive.UI.Views.DynamicLayout;
using System;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SINTEF.AutoActive.FileSystem;
using SINTEF.AutoActive.UI.Interfaces;
using SINTEF.AutoActive.UI.Views;
using SINTEF.AutoActive.UI.Views.TreeView;
using Xamarin.Forms;

namespace SINTEF.AutoActive.UI.Pages.Player
{
	public partial class PlayerPage : ContentPage, ISerializableView
	{
        private readonly IFileBrowser _browser;
        private const double SplitViewWidthMin = 10000;
	    private const double OverlayModeWidth = 0.9;
	    private const double OverlayModeShadeOpacity = 0.5;
	    public TimeSynchronizedContext ViewerContext { get; } = new TimeSynchronizedContext();

        public PlayerPage()
        {
            InitializeComponent();
            _browser = DependencyService.Get<IFileBrowser>();
            if (_browser == null)
            {
                XamarinHelpers.GetCurrentPage(Navigation).DisplayAlert("Critical error",
                    "Could get file browser. Will not be able to open and save files.", "OK");
            }


            ViewerContext?.SetSynchronizedToWorldClock(true);

            Appearing += OnAppearing;
            Disappearing += OnDisappearing;
        }

        private void OnAppearing(object sender, EventArgs e)
        {
            Playbar.ViewerContext = ViewerContext;

            Splitter.DragStart += Splitter_DragStart;
            Splitter.Dragged += Splitter_Dragged;

            TreeView.DataPointTapped += TreeView_DataPointTapped;
            TreeView.UseInTimelineTapped += TreeView_UseInTimelineTapped;

            Playbar.DataTrackline.RegisterFigureContainer(PlayerContainer);
        }

        private void OnDisappearing(object sender, EventArgs e)
        {
            Splitter.DragStart -= Splitter_DragStart;
            Splitter.Dragged -= Splitter_Dragged;

            TreeView.DataPointTapped -= TreeView_DataPointTapped;
            TreeView.UseInTimelineTapped -= TreeView_UseInTimelineTapped;

            Playbar.DataTrackline.DeregisterFigureContainer(PlayerContainer);
        }

        private void TreeView_DataPointTapped(object sender, IDataPoint dataPoint)
        {
            PlayerContainer.DataPointSelected(dataPoint, ViewerContext);
        }

        private void TreeView_UseInTimelineTapped(object sender, IDataPoint datapoint)
        {
            Playbar.UseDataPointForTimelinePreview(datapoint);
        }

        /* -- Show or hide the tree based on window size -- */
	    private static readonly GridLength GridZeroLength = new GridLength(0, GridUnitType.Absolute);

	    private enum TreeViewState { SplitMode, OverlayModeHidden, OverlayModeShown }
	    private TreeViewState _treeViewState = TreeViewState.SplitMode;

	    private void UpdateTreeView(TreeViewState nextTreeViewState)
        {
            if (nextTreeViewState == _treeViewState) return;
            var prevTreeViewState = _treeViewState;
            _treeViewState = nextTreeViewState;

            // Deal with the tree
            if (nextTreeViewState == TreeViewState.SplitMode)
            {
                // Remove it from the overlay
                OverlayShading.IsVisible = false;
                TreeView.IsVisible = false;
                OverlayLayout.Children.Remove(TreeView);
                // Move the tree into the grid
                var grid = Content as Grid;
                ColumnSplitter.Width = 2d;
                ColumnTree.Width = _treeViewWidth;
                grid?.Children.Add(TreeView, 2, 1);
                TreeView.IsVisible = true;
            }
            else if (nextTreeViewState == TreeViewState.OverlayModeShown || nextTreeViewState == TreeViewState.OverlayModeHidden)
            {
                // We should prepare the overlay
                if (prevTreeViewState == TreeViewState.SplitMode)
                {
                    // Remove it from the split
                    var grid = Content as Grid;
                    ColumnSplitter.Width = GridZeroLength;
                    ColumnTree.Width = GridZeroLength;
                    grid?.Children.Remove(TreeView);
                    // Show it in the overlay
                    OverlayLayout.Children.Add(TreeView, new Rectangle(1, 1, OverlayModeWidth, 1), AbsoluteLayoutFlags.All);

                    // Show or hide the view
                    if (nextTreeViewState == TreeViewState.OverlayModeShown)
                    {
                        TreeView.IsVisible = true;
                        OverlayShading.IsVisible = true;
                    }
                    else
                    {
                        TreeView.IsVisible = false;
                        OverlayShading.IsVisible = false;
                    }
                }
                else
                {
                    // Animate the opening or closing of the overlay
                    if (nextTreeViewState == TreeViewState.OverlayModeShown)
                    {
                        OverlayShading.Opacity = 0;
                        OverlayShading.IsVisible = true;
                        // Animate the TreeView with a fixed position (so it doesn't scale)
                        AbsoluteLayout.SetLayoutFlags(TreeView, AbsoluteLayoutFlags.SizeProportional);
                        AbsoluteLayout.SetLayoutBounds(TreeView, new Rectangle(0, 0, OverlayModeWidth, 1));
                        TreeView.IsVisible = true;
                        var animation = new Animation(v =>
                        {
                            OverlayShading.Opacity = v * OverlayModeShadeOpacity;
                            AbsoluteLayout.SetLayoutBounds(TreeView, new Rectangle(Width-v*TreeView.Width, 0, OverlayModeWidth, 1));
                        });
                        animation.Commit(this, "SlideTreeOverlayIn", rate: 10, length: 100, easing: Easing.SinIn, finished: (v, c) =>
                        {
                            OverlayShading.Opacity = OverlayModeShadeOpacity;
                            AbsoluteLayout.SetLayoutFlags(TreeView, AbsoluteLayoutFlags.All);
                            AbsoluteLayout.SetLayoutBounds(TreeView, new Rectangle(1, 0, OverlayModeWidth, 1));
                        });
                    }
                    else
                    {
                        // Animate the TreeView with a fixed position (so it doesn't scale)
                        AbsoluteLayout.SetLayoutFlags(TreeView, AbsoluteLayoutFlags.SizeProportional);
                        AbsoluteLayout.SetLayoutBounds(TreeView, new Rectangle(Width-OverlayModeWidth*TreeView.Width, 0, OverlayModeWidth, 1));
                        var animation = new Animation(v =>
                        {
                            OverlayShading.Opacity = v * OverlayModeShadeOpacity;
                            AbsoluteLayout.SetLayoutBounds(TreeView, new Rectangle(Width - v * TreeView.Width, 0, OverlayModeWidth, 1));
                        }, start: 1, end: 0);
                        animation.Commit(this, "SlideTreeOverlayOut", rate: 10, length: 100, easing: Easing.SinOut, finished: (v, c) =>
                        {
                            OverlayShading.IsVisible = false;
                            TreeView.IsVisible = false;
                        });
                    }

                }
            }
        }

        protected override void OnSizeAllocated(double width, double height)
        {
            base.OnSizeAllocated(width, height);
            // Disable hiding overlay
            UpdateTreeView(TreeViewState.SplitMode);
            return;
            var shouldShowSplit = width >= SplitViewWidthMin;
            if (shouldShowSplit && _treeViewState != TreeViewState.SplitMode)
            {
                UpdateTreeView(TreeViewState.SplitMode);
            }
            else if (!shouldShowSplit && _treeViewState == TreeViewState.SplitMode)
            {
                UpdateTreeView(TreeViewState.OverlayModeHidden);
            }
        }

        private void NavigationBar_MenuButtonClicked(object sender, EventArgs e)
        {
            if (_treeViewState == TreeViewState.OverlayModeHidden)
            {
                UpdateTreeView(TreeViewState.OverlayModeShown);
            }
            else if (_treeViewState == TreeViewState.OverlayModeShown)
            {
                UpdateTreeView(TreeViewState.OverlayModeHidden);
            }
        }

        /* -- Resizing the tree view -- */
        GridLength _treeViewWidth = PlayerTreeView.DefaultWidth;
        GridLength _splitterStartDragWidth;

        private void Splitter_DragStart(DraggableSeparator sender, double x, double y)
        {
            _splitterStartDragWidth = ColumnTree.Width;
        }

        private void Splitter_Dragged(DraggableSeparator sender, double x, double y, double dx, double dy)
        {
            var newWidth = _splitterStartDragWidth.Value - x;
            if (newWidth >= 0 && newWidth + ColumnSplitter.Width.Value <= Width)
            {
                ColumnTree.Width = _treeViewWidth = new GridLength(newWidth);
            }
        }

        public async void SaveView(string uri = null)
        {
            IReadWriteSeekStreamFactory file = null;
            if (uri == null)
            {
                file = await _browser.BrowseForSave((".aav", "AutoActive View"));
            }
#if Feature_BroadSystemAccess
            else
            {
                file = await _browser.SaveFromUri(uri);
            }
#endif

            if (file == null) return;

            var root = SerializeView();

            var stream = await file.GetReadWriteStream();
            using (var streamWriter = new StreamWriter(stream))
            using (var writer = new JsonTextWriter(streamWriter))
            {
                var serializer = new JsonSerializer
                {
                    Formatting = Formatting.Indented
                };
                serializer.Serialize(writer, root);
            }
        }

        public async void LoadView(IDataStructure archive = null, string uri = null)
        {
            IReadSeekStreamFactory file = null;
            if (uri == null)
            {
                file = await _browser.BrowseForLoad((".aav", "AutoActive View"));
            }
#if Feature_BroadSystemAccess
            else
            {
                file = await _browser.LoadFromUri(uri);
            }
#endif
            if (file == null)
            {
                return;
            }

            JObject root;
            var stream = await file.GetReadStream();
            using (var streamReader = new StreamReader(stream))
            using (var reader = new JsonTextReader(streamReader))
            {
                var serializer = new JsonSerializer();
                root = serializer.Deserialize(reader) as JObject;
            }

            await DeserializeView(root);
        }

        public string ViewType => "PlayerPage";
        public async Task DeserializeView(JObject root, IDataStructure archive = null)
        {
            var figures = XamarinHelpers.GetAllChildElements<FigureView>(PlayerContainer);

            foreach (var figure in figures)
            {
                figure.RemoveThisView();
            }

            PlayerContainer.ViewerContext = ViewerContext;
            FigureView.DeserializationFailedWarned = false;
            await PlayerContainer.DeserializeView(root, archive);
        }

        public JObject SerializeView(JObject root = null)
        {
            if (root == null)
                root = new JObject();

            PlayerContainer.SerializeView(root);

            return root;
        }
    }
}