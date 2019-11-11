﻿using Newtonsoft.Json.Linq;
using SINTEF.AutoActive.Databus.Interfaces;
using SINTEF.AutoActive.Databus.ViewerContext;
using SINTEF.AutoActive.UI.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace SINTEF.AutoActive.UI.Views.DynamicLayout
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class PlaceableContainer : ContentView, IFigureContainer, ISerializableView
    {
        private readonly List<PlaceableItem> _placeableItems = new List<PlaceableItem>();

        public PlaceableContainer()
        {
            InitializeComponent();
            _placeableItems.AddRange(XamarinHelpers.GetAllChildElements<PlaceableItem>(this));
        }

        private bool PlacementLocationVisible
        {
            set
            {
                foreach (var item in _placeableItems)
                    item.PlacementLocationVisible = value;
            }
        }

        private IDataPoint _selectedItem;
        private TimeSynchronizedContext _selectedContext;

        public void DataPointSelected(IDataPoint dataPoint, TimeSynchronizedContext context)
        {
            SelectItem(dataPoint, context);
        }

        public void SelectItem(IDataPoint item, TimeSynchronizedContext context)
        {
            if (_selectedItem == item)
            {
                _selectedItem = null;
                PlacementLocationVisible = false;
                return;
            }

            _selectedContext = context;
            PlacementLocationVisible = true;
            _selectedItem = item;

            if (_placeableItems.Count == 1 && _placeableItems.First().Item == null)
            {
                PlaceableItem_OnLocationSelected(_placeableItems.First(), PlaceableLocation.Center);
            } 
        }

        private async void PlaceableItem_OnLocationSelected(object sender, PlaceableLocation e)
        {
            PlacementLocationVisible = false;

            if (_selectedItem == null)
            {
                Debug.WriteLine("No item selected.");
                return;
            }

            if (!(sender is PlaceableItem placeableSender))
            {
                Debug.WriteLine($"Unknown sender: {sender}");
                return;
            }


            Debug.WriteLine(placeableSender.Item != null
                ? $"Placing: {_selectedItem} @ {e} : {placeableSender.Item}"
                : $"Placing: {_selectedItem} @ {e}");

            var item = _selectedItem;
            var context = _selectedContext;

            _selectedItem = null;
            _selectedContext = null;

            await PlaceItem(placeableSender, item, context, e);
        }

        private async Task<FigureView> CreateFigureView(IDataPoint item, TimeSynchronizedContext context)
        {
            DatapointAdded?.Invoke(this, (item, context));
            return await FigureView.GetView(item, context);
        }

        private async Task<PlaceableItem> PlaceItem(PlaceableItem placeableSender, IDataPoint item, TimeSynchronizedContext context, PlaceableLocation e)
        {
            if (e != PlaceableLocation.Center)
            {
                return await AddItem(placeableSender, item, context, e);
            }

            if (placeableSender.Item != null)
            {
                return await ToggleDataPoint(placeableSender, item, context);
            }

            placeableSender.SetItem(await CreateFigureView(item, context));
            return placeableSender;
        }

        private async Task<PlaceableItem> AddItem(PlaceableItem placeableSender, IDataPoint item, TimeSynchronizedContext context, PlaceableLocation e)
        {
            var visualizer = await CreateFigureView(item, context);
            var placeableItem = new PlaceableItem();
            placeableItem.LocationSelected += PlaceableItem_OnLocationSelected;
            placeableItem.SetItem(visualizer);
            _placeableItems.Add(placeableItem);
            placeableItem.HorizontalOptions = LayoutOptions.FillAndExpand;
            placeableItem.VerticalOptions = LayoutOptions.FillAndExpand;
            placeableSender.PlaceRelative(placeableItem, e);
            return placeableItem;
        }

        private async Task<PlaceableItem> ToggleDataPoint(PlaceableItem container, IDataPoint item, TimeSynchronizedContext context)
        {
            var res = await container.Item.ToggleDataPoint(item, context);
            switch (res)
            {
                case ToggleResult.Added:
                    DatapointAdded?.Invoke(this, (item, context));
                    break;
                case ToggleResult.Removed:
                    DatapointRemoved?.Invoke(this, (item, context));
                    break;
                case ToggleResult.Cancelled:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            if (container.Item != null && container.Item.DataPoints.Any())
                return container;

            return RemoveItem(container, context);
        }

        private PlaceableItem RemoveItem(PlaceableItem container, TimeSynchronizedContext context)
        {
            if (container.Item != null)
            {
                foreach (var itemDataPoint in container.Item.DataPoints)
                {
                    DatapointRemoved?.Invoke(this, (itemDataPoint, context));
                }
            }
            container.SetItem(null);

            _placeableItems.Remove(container);

            var placeableParent = XamarinHelpers.GetTypedElementFromParents<PlaceableItem>(container.Parent);
            placeableParent?.RemoveItem(container);
            var layout = (ResizableStackLayout)container.Parent;

            {
                var index = layout.Children.IndexOf(container);
                layout.Children.RemoveAt(index);

                var verticalIndex = MainVerticalStackLayout.Children.IndexOf(layout);

                var leftIx = 0;
                // Move the children to the removed item's parent
                foreach (var childItemPair in container.PlaceableItems)
                {
                    var (childItem, location) = childItemPair;
                    if (placeableParent != null)
                    {
                        placeableParent.PlaceRelative(childItem, location);
                    }
                    else
                    {
                        switch (location)
                        {
                            case PlaceableLocation.Left:
                                MainHorizontalStackLayout.Children.Insert(index++ - ++leftIx, childItem);
                                break;
                            case PlaceableLocation.Right:
                                MainHorizontalStackLayout.Children.Insert(index, childItem);
                                break;
                            case PlaceableLocation.Up:
                                MainVerticalStackLayout.Children.Insert(verticalIndex++, childItem);
                                break;
                            case PlaceableLocation.Down:
                                MainVerticalStackLayout.Children.Insert(verticalIndex + 1, childItem);
                                break;
                            case PlaceableLocation.Center:
                                throw new ArgumentException("Center should have been handled elsewhere");
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                }
            }

            // Hack to remove empty StackLayouts
            if (!layout.Children.Any() && layout.Parent is ResizableStackLayout stackParent)
            {
                if (layout != MainHorizontalStackLayout && layout != MainVerticalStackLayout)
                {
                    stackParent.Children.Remove(layout);
                }
            }

            if (_placeableItems.Count != 0) return placeableParent;

            // Ensure at least one item
            var view = new PlaceableItem();
            _placeableItems.Add(view);
            view.LocationSelected += PlaceableItem_OnLocationSelected;
            MainHorizontalStackLayout.Children.Add(view);
            return view;
        }

        

        public FigureView Selected { get; set; }

        public void RemoveChild(FigureView figureView)
        {
            var item = XamarinHelpers.GetTypedElementFromParents<PlaceableItem>(figureView);
            RemoveItem(item, figureView.Context);
        }

        public event EventHandler<(IDataPoint, DataViewerContext)> DatapointAdded;
        public event EventHandler<(IDataPoint, DataViewerContext)> DatapointRemoved;
        public void InvokeDatapointRemoved(IDataPoint dataPoint, DataViewerContext context)
        {
            DatapointRemoved?.Invoke(this, (dataPoint, context));
        }

        public void DeserializeView(JObject root)
        {
            throw new NotImplementedException();
        }

        public JObject SerializeView(JObject root = null)
        {
            throw new NotImplementedException();
        }
    }
}