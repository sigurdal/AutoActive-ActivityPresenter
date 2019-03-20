﻿using ICSharpCode.SharpZipLib.Zip;
using Newtonsoft.Json.Linq;
using Parquet;
using Parquet.Data;
using SINTEF.AutoActive.Archive;
using SINTEF.AutoActive.Archive.Plugin;
using SINTEF.AutoActive.Databus.Implementations.TabularStructure;
using SINTEF.AutoActive.Databus.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Parquet.Data.Rows;

namespace SINTEF.AutoActive.Plugins.ArchivePlugins.Table
{
    internal class RememberingParquetReader
    {
        private readonly ParquetReader _reader;
        public RememberingParquetReader(ParquetReader reader)
        {
            _reader = reader;
        }

        public Schema Schema => _reader.Schema;

        private readonly Dictionary<DataField, Array> _data = new Dictionary<DataField, Array>();

        public void LoadAll()
        {
            foreach (var column in _reader.Schema.GetDataFields())
            {
                if (_data.TryGetValue(column, out var arr))
                {
                    continue;
                }

                var t = DataType2Type(column.DataType);
                GetType().GetMethod("LoadColumn")?.MakeGenericMethod(t).Invoke(this, new object[] {column});
            }
        }

        public T[] LoadColumn<T>(DataField column)
        {
            if (_data.TryGetValue(column, out var arr))
            {
                return arr as T[];
            }

            //TODO: these should not be needed
            // Find the datafield we want to use
            var dataField = Array.Find(_reader.Schema.GetDataFields(), field => field.Name == column.Name);
            if (dataField == null) throw new ArgumentException($"Couldn't find column {column.Name} in table");

            T[] data = null;
            // Read the data pages
            for (var page = 0; page < _reader.RowGroupCount; page++)
            {
                // TODO: Do this asynchronously?
                var pageReader = _reader.OpenRowGroupReader(page);
                var dataColumn = pageReader.ReadColumn(dataField);
                var prevLength = data?.Length ?? 0;
                Array.Resize(ref data, prevLength + dataColumn.Data.Length);
                Array.Copy(dataColumn.Data, 0, data, prevLength, dataColumn.Data.Length);
            }

            _data[column] = data;

            return data;
        }

        public static Type DataType2Type(DataType type)
        {
            switch (type)
            {
                case DataType.Boolean:
                    return typeof(bool);
                case DataType.Byte:
                    return typeof(byte);
                case DataType.Int32:
                    return typeof(int);
                case DataType.Int64:
                    return typeof(long);
                case DataType.Float:
                    return typeof(float);
                case DataType.Double:
                    return typeof(double);
                case DataType.SignedByte:
                    return typeof(sbyte);
                case DataType.UnsignedByte:
                    return typeof(byte);
                case DataType.Short:
                    return typeof(short);
                case DataType.UnsignedShort:
                    return typeof(ushort);
                case DataType.Int16:
                    return typeof(short);
                case DataType.UnsignedInt16:
                    return typeof(ushort);
                case DataType.Int96:
                    break;
                case DataType.ByteArray:
                    break;
                case DataType.String:
                    break;
                case DataType.Decimal:
                    break;
                case DataType.DateTimeOffset:
                    break;
                case DataType.Interval:
                    break;
                case DataType.Unspecified:
                    break;
            }

            throw new NotImplementedException($"Data type {type} not implemented.");
        }

        public Array GetColumn(DataField field)
        {
            return _data[field];
        }
    }

    public class ArchiveTable : ArchiveStructure, ISaveable
    {
        public override string Type => "no.sintef.table";
        private readonly ZipEntry _zipEntry;
        private readonly RememberingParquetReader _reader;
        private readonly Archive.Archive _archive;

        internal ArchiveTable(JObject json, Archive.Archive archive, ArchiveTableInformation tableInformation) :
            base(json)
        {
            IsSaved = true;
            _archive = archive;
            _zipEntry = tableInformation.ZipEntry;

            if (tableInformation.Time == null) throw new ArgumentException("Table does not have a column named 'Time'");

            var streamTask = archive.OpenFile(_zipEntry);
            streamTask.Wait();
            using (var reader = new ParquetReader(streamTask.Result))
            {
                _reader = new RememberingParquetReader(reader);
            }

            AddColumns(tableInformation);
        }

        public ArchiveTable(JObject json, ParquetReader reader, ArchiveTableInformation tableInformation, string name) :
            base(json)
        {
            Name = name;
            _reader = new RememberingParquetReader(reader);
            IsSaved = false;

            AddColumns(tableInformation);
        }

        void AddColumns(ArchiveTableInformation tableInformation)
        {
            // TODO(sigurdal): Handle nullable data? column.HasNulls
            var timeInfo = tableInformation.Time;
            var time = new TableTimeIndex(timeInfo.Name, GenerateLoader<long>(_reader, timeInfo), false);

            foreach (var column in tableInformation.Columns)
            {
                if (column.HasNulls)
                {
                    throw new NotImplementedException("Nullable columns are not yet implemented.");
                }

                switch (column.DataType)
                {
                    case DataType.Boolean:
                        this.AddColumn(column.Name, GenerateLoader<bool>(_reader, column), time);
                        break;
                    case DataType.Byte:
                        this.AddColumn(column.Name, GenerateLoader<byte>(_reader, column), time);
                        break;
                    case DataType.Int32:
                        this.AddColumn(column.Name, GenerateLoader<int>(_reader, column), time);
                        break;
                    case DataType.Int64:
                        this.AddColumn(column.Name, GenerateLoader<long>(_reader, column), time);
                        break;
                    case DataType.Float:
                        this.AddColumn(column.Name, GenerateLoader<float>(_reader, column), time);
                        break;
                    case DataType.Double:
                        this.AddColumn(column.Name, GenerateLoader<double>(_reader, column), time);
                        break;

                    default:
                        throw new InvalidOperationException($"Cannot read {column.DataType} columns");
                }
            }
        }

        private static Task<T[]> LoadColumn<T>(RememberingParquetReader reader, DataField column)
        {
            return Task.FromResult(reader.LoadColumn<T>(column));
        }

        private static Task<T[]> GenerateLoader<T>(RememberingParquetReader reader, DataField column)
        {
            return new Task<T[]>(() => LoadColumn<T>(reader, column).Result);
        }

        public bool IsSaved { get; set; }
        public async Task<bool> WriteData(JObject root, ISessionWriter writer)
        {
            string tablePath;
            //TODO: Implement?
            if (false && IsSaved)
            {
                var stream = await _archive.OpenFile(_zipEntry);

                tablePath = writer.StoreFile(stream, _zipEntry.Name);
            }
            else
            {
                //TODO: the table name should probably be something else
                var tableName = Name + "/" + "data.parquet";

                //TODO: this stream might be disposed on commit?
                var ms = new MemoryStream();

                _reader.LoadAll();
                using (var tableWriter = new ParquetWriter(_reader.Schema, ms))
                {
                    var rowGroup = tableWriter.CreateRowGroup();
                    foreach (var field in _reader.Schema.GetDataFields())
                    {
                        var column = new DataColumn(field, _reader.GetColumn(field));
                        rowGroup.WriteColumn(column);
                    }
                }

                ms.Position = 0;
                tablePath = writer.StoreFile(ms, tableName);

            }

            if (!root.TryGetValue("user", out var user))
            {
                user = new JObject();
                root["user"] = user;
            }

            if (!root.TryGetValue("meta", out var meta))
            {
                meta = new JObject();
                root["meta"] = meta;
            }
            root["meta"]["type"] = Type;
            root["meta"]["path"] = tablePath;
            // TODO: add units
            // root["meta"]["units"]

            root["name"] = Name;

            return true;
        }
    }

    [ArchivePlugin("no.sintef.table")]
    public class ArchiveTablePlugin : IArchivePlugin
    {
        private async Task<ArchiveTableInformation> ParseTableInformation(JObject json, Archive.Archive archive)
        {
            // Find the properties in the JSON
            ArchiveStructure.GetUserMeta(json, out var meta, out var user);
            var path = meta["path"].ToObject<string>() ?? throw new ArgumentException("Table is missing 'path'");

            // Find the file in the archive
            var zipEntry = archive.FindFile(path) ?? throw new ZipException($"Table file '{path}' not found in archive");

            var tableInformation = new ArchiveTableInformation
            {
                ZipEntry = zipEntry,
                Columns = new List<DataField>(),
            };

            // Open the table file
            using (var stream = await archive.OpenFile(zipEntry))
            using (var reader = new ParquetReader(stream))
            {
                var fields = reader.Schema.GetDataFields();

                // Find all the column information
                foreach (var field in fields)
                {
                    if (field.Name.Equals("time", StringComparison.OrdinalIgnoreCase))
                    {
                        tableInformation.Time = field;
                    }
                    else
                    {
                        tableInformation.Columns.Add(field);
                    }
                }
            }

            // Return the collected info
            return tableInformation;
        }

        public async Task<ArchiveStructure> CreateFromJSON(JObject json, Archive.Archive archive)
        {
            var information = await ParseTableInformation(json, archive);
            return new ArchiveTable(json, archive, information);
        }
    }

    /* -- Helper structs -- */
    public struct ArchiveTableInformation
    {
        public ZipEntry ZipEntry;
        public DataField Time;
        public List<DataField> Columns;
    }
}
