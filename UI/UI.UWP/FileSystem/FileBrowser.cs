﻿using System;
using Xamarin.Forms;
using Windows.Storage.Pickers;

using SINTEF.AutoActive.FileSystem;
using SINTEF.AutoActive.UI.UWP.FileSystem;
using System.Threading.Tasks;
using System.IO;
using Windows.Storage;
using System.Diagnostics;
using SINTEF.AutoActive.Plugins.Import;

[assembly: Dependency(typeof(FileBrowser))]
namespace SINTEF.AutoActive.UI.UWP.FileSystem
{
    class ReadSeekStreamFactory : IReadSeekStreamFactory
    {
        protected StorageFile _file;

        internal ReadSeekStreamFactory(StorageFile file)
        {
            _file = file;
            Name = file.DisplayName;
            Extension = file.FileType;
            Mime = file.ContentType;
            // TODO: Should we open a reader/writer to ensure no-one changes the file while we are potentially doing other stuff?
        }

        public string Name { get; private set; }
        public string Extension { get; private set; }
        public string Mime { get; private set; }

        public async Task<Stream> GetReadStream()
        {
            var stream = await _file.OpenStreamForReadAsync();
            if (!stream.CanRead) throw new IOException("Stream must readable");
            if (!stream.CanSeek) throw new IOException("Stream must be seekable");
            return stream;
        }
    }

    class ReadWriteSeekStreamFactory : ReadSeekStreamFactory, IReadWriteSeekStreamFactory
    {
        internal ReadWriteSeekStreamFactory(StorageFile file) : base(file) { }

        public async Task<Stream> GetReadWriteStream()
        {
            var stream = await _file.OpenStreamForWriteAsync();
            if (!stream.CanRead) throw new IOException("Stream must readable");
            if (!stream.CanWrite) throw new IOException("Stream must be writable");
            if (!stream.CanSeek) throw new IOException("Stream must be seekable");
            return stream;
        }
    }

    public class FileBrowser : IFileBrowser
    {
        public async Task<IReadWriteSeekStreamFactory> BrowseForArchive()
        {
            var picker = new FileOpenPicker();
            picker.FileTypeFilter.Add(".aaz");
            var file = await picker.PickSingleFileAsync();
            return new ReadWriteSeekStreamFactory(file);
        }

        public async Task<IReadSeekStreamFactory> BrowseForImportFile()
        {
            var picker = new FileOpenPicker();
            // Find all extensions we support
            foreach (var extension in ImportPlugins.SupportedExtensions)
            {
                // TODO: Perhaps do some checking here, it's a bit picky about the format
                // FIXME: Also, I don't know how well this handles duplicates
                picker.FileTypeFilter.Add(extension);
            }
            var file = await picker.PickSingleFileAsync();
            return new ReadSeekStreamFactory(file);
        }
    }
}
