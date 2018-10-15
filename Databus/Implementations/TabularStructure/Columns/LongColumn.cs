﻿using SINTEF.AutoActive.Databus.Common;
using SINTEF.AutoActive.Databus.Interfaces;
using SINTEF.AutoActive.Databus.ViewerContext;
using System;
using System.Threading.Tasks;

namespace SINTEF.AutoActive.Databus.Implementations.TabularStructure.Columns
{
    public class LongColumn : TableColumn
    {
        internal long[] data;
        private Task<long[]> loader;

        public LongColumn(string name, Task<long[]> loader, TableTimeIndex index) : base(typeof(long), name, loader, index)
        {
            this.loader = loader;
        }

        protected override int CheckLoaderResultLength()
        {
            data = loader.Result;
            return data.Length;
        }

        protected override (double? min, double? max) GetDataMinMax()
        {
            if (data.Length == 0) return (null, null);
            var min = data[0];
            var max = data[0];
            for (var i = 1; i < data.Length; i++)
            {
                if (data[i] < min) min = data[i];
                if (data[i] > max) max = data[i];
            }
            return (min, max);
        }

        protected override IDataViewer CreateLongViewer(TableTimeIndex index)
        {
            return new LongColumnViewer(index, this);
        }
    }

    public class LongColumnViewer : TableColumnViewer
    {
        private LongColumn column;

        internal LongColumnViewer(TableTimeIndex index, LongColumn column) : base(index, column)
        {
            this.column = column;
        }

        public override SpanPair<long> GetCurrentLongs()
        {
            return new SpanPair<long>(index.data.AsSpan(startIndex, length), column.data.AsSpan(startIndex, length));
        }
    }
}