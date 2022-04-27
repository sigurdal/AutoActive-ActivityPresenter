using SINTEF.AutoActive.Databus.Interfaces;
using SINTEF.AutoActive.FileSystem;
using SINTEF.AutoActive.UI.Helpers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SINTEF.AutoActive.Plugins.Import.Csv
{
    [ImportPlugin(".txt")]
    public class ImportLoadsol : IImportPlugin
    {
        public async Task<bool> CanParse(IReadSeekStreamFactory readerFactory)
        {
            if (!readerFactory.Name.Contains("loadsol"))
            {
                return false;
            }
            var stream = await readerFactory.GetReadStream();
            using (var reader = new StreamReader(stream))
            {
                var line = reader.ReadLine();
                return line != null && line.Contains("loadsol") && line.EndsWith(".pdo") ;
            }
        }

        public void GetExtraConfigurationParameters(Dictionary<string, (object, string)> parameters)
        {

        }

        public async Task<IDataProvider> Import(IReadSeekStreamFactory readerFactory,
            Dictionary<string, object> parameters)
        {
            var importer = new LoadsolImporter(parameters, readerFactory.Name);
            importer.ParseFile(await readerFactory.GetReadStream());
            return importer;
        }
    }

    public class LoadsolImporter : GenericCsvImporter
    {
        public LoadsolImporter(Dictionary<string, object> parameters, string filename) : base(parameters, filename)
        {
        }

        private static long ParseDateTime(string date, string time)
        {
            var dateTime = DateTime.Parse(date + " " + time, CultureInfo.InvariantCulture);
            return TimeFormatter.TimeFromDateTime(dateTime);
        }

        protected override string TableName => "Loadsol";

        protected override void PreProcessStream(Stream stream)
        {
            var initialStreamPos = stream.Position;
            var buffer = new byte[4096];
            var dataCount = stream.Read(buffer, 0, buffer.Length);

            // Look for four dashes in a row
            var dashIndex = 3;
            for (; dashIndex < dataCount; dashIndex++)
            {
                if (buffer[dashIndex - 3] == '-' && buffer[dashIndex - 2] == '-' && buffer[dashIndex - 1] == '-' && buffer[dashIndex] == '-')
                {
                    break;
                }
            }

            for (; dashIndex < dataCount; dashIndex++)
            {
                if (buffer[dashIndex] != '\n') continue;
                break;
            }

            stream.Seek(initialStreamPos + dashIndex, SeekOrigin.Begin);
        }
        private static long[] DoubleArrayToTime(IEnumerable<double> stringTime)
        {
            return stringTime.Select(TimeFormatter.TimeFromSeconds).ToArray();
        }

        protected override void PostProcessData(List<string> names, List<Type> types, List<Array> data)
        {
            names[0] = "time";
            types[0] = typeof(long);
            data[0] = DoubleArrayToTime((double[]) data[0]);
        }
    }
}
