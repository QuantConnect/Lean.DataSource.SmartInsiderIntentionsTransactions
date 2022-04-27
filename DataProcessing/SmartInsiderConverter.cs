/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using QuantConnect.DataSource;
using QuantConnect.Logging;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.DataProcessing
{
    public class SmartInsiderConverter : IDisposable
    {
        public const string VendorName = "smartinsider";
        
        private readonly DirectoryInfo _sourceDirectory;
        private readonly DirectoryInfo _destinationDirectory;
        private readonly DirectoryInfo _processedDirectory;

        private readonly MapFileResolver _mapFileResolver;

        private Dictionary<string, Dictionary<string, string>> _intentionUniverse = new();
        private Dictionary<string, Dictionary<string, string>> _transactionUniverse = new();

        /// <summary>
        /// Creates an instance of the converter
        /// </summary>
        /// <param name="sourceDataDirectory">Directory of raw data folder to read raw data from</param>
        /// <param name="destinationDataDirectory">Directory of data folder to write processed data to</param>
        /// <param name="processedDataDirectory">Directory of data folder containing processed data to read existing processed data from</param>
        public SmartInsiderConverter(DirectoryInfo sourceDataDirectory, DirectoryInfo destinationDataDirectory, DirectoryInfo processedDataDirectory)
        {
            _sourceDirectory = new DirectoryInfo(Path.Combine(sourceDataDirectory.FullName, "alternative", VendorName));
            _destinationDirectory = Directory.CreateDirectory(Path.Combine(destinationDataDirectory.FullName, "alternative", VendorName));
            _processedDirectory = new DirectoryInfo(Path.Combine(processedDataDirectory.FullName, "alternative", VendorName));

            var mapFileProvider = new LocalZipMapFileProvider();
            mapFileProvider.Initialize(new DefaultDataProvider());
            _mapFileResolver = mapFileProvider.Get(AuxiliaryDataKey.EquityUsa);

            Directory.CreateDirectory(Path.Combine(_destinationDirectory.FullName, "intentions", "universe"));
            Directory.CreateDirectory(Path.Combine(_destinationDirectory.FullName, "transactions", "universe"));
        }

        /// <summary>
        /// Converts raw data from Smart Insider
        /// </summary>
        /// <param name="date">Date to process</param>
        /// <returns>Boolean value indicating success status</returns>
        public bool Convert(DateTime date)
        {
            try
            {
                Log.Trace($"SmartInsiderConverter.Convert(): Begin converting {_sourceDirectory.FullName}");

                var rawIntentionsFile = new FileInfo(Path.Combine(_sourceDirectory.FullName, "intentions", $"{date:yyyyMMdd}.ttx"));
                var rawTransactionsFile = new FileInfo(Path.Combine(_sourceDirectory.FullName, "transactions", $"{date:yyyyMMdd}.ttx"));

                var intentionsDirectory = new DirectoryInfo(Path.Combine(_destinationDirectory.FullName, "intentions"));
                var transactionsDirectory = new DirectoryInfo(Path.Combine(_destinationDirectory.FullName, "transactions"));

                if (rawIntentionsFile.Exists)
                {
                    var data = Process<SmartInsiderIntention>(rawIntentionsFile);
                    if (!data.Any())
                    {
                        Log.Trace("SmartInsiderConverter.Convert(): Intentions file contains no data to write");
                    }

                    // We can call this method with no data available because it will do nothing without any data
                    WriteToFile(intentionsDirectory, data);
                }
                else
                {
                    Log.Error($"SmartInsiderConverter.Convert(): Raw intentions file does not exist: {rawIntentionsFile.FullName}");
                }

                if (rawTransactionsFile.Exists)
                {
                    var data = Process<SmartInsiderTransaction>(rawTransactionsFile);
                    if (!data.Any())
                    {
                        Log.Trace("SmartInsiderConverter.Convert(): Transactions file contains no data to write");
                    }

                    // We can call this method with no data available because it will do nothing without any data
                    WriteToFile(transactionsDirectory, data);
                }
                else
                {
                    Log.Error($"SmartInsiderConverter.Convert(): Raw transactions file does not exist: {rawTransactionsFile.FullName}");
                }

                if (_intentionUniverse.Count > 0)
                {
                    WriteUniverseFile(intentionsDirectory, _intentionUniverse);
                }

                if (_transactionUniverse.Count > 0)
                {
                    WriteUniverseFile(transactionsDirectory, _transactionUniverse);
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "SmartInsiderConverter.Convert(): Failed to parse raw SmartInsider data");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Processes the data
        /// </summary>
        /// <typeparam name="T"><see cref="SmartInsiderEvent"/> inherited instance</typeparam>
        /// <param name="sourceFile">File to read data from</param>
        /// <returns>Dictionary keyed by ticker that contains all the lines that appeared in the file</returns>
        private Dictionary<string, List<T>> Process<T>(FileInfo sourceFile)
            where T : SmartInsiderEvent, new()
        {
            var previousMarket = string.Empty;
            var previousTicker = string.Empty;
            var lines = new Dictionary<string, List<T>>();
            var i = 0;

            Log.Trace($"SmartInsiderConverter.Process(): Processing file: {sourceFile.FullName}");

            Dictionary<string, int> indexes = null;
            foreach (var rawLine in File.ReadLines(sourceFile.FullName))
            {
                i++;

                var line = rawLine.Replace("\"", "");
                // First line is the header row, but make sure we don't encounter it anywhere else in the data
                if (line.StartsWith("TransactionID"))
                {
                    var headers = line.Split('\t').ToList();
                    indexes = headers.ToDictionary(s => s, s => headers.IndexOf(s), StringComparer.OrdinalIgnoreCase);

                    Log.Trace($"SmartInsiderConverter.Process(): Header row on line {i}: [{string.Join(",", indexes.Select(kvp => $"{kvp.Key}@{kvp.Value}"))}]");
                    continue;
                }

                if (indexes == null)
                {
                    throw new ArgumentException("SmartInsiderConverter.Process(): Header row was not found!");
                }

                if(line.All((x) => x == '\t'))
                {
                    //blank line
                    continue;
                }

                var dataInstance = new T();
                dataInstance.FromRawData(line, indexes);

                var ticker = dataInstance.TickerSymbol;

                // For now, only support US markets
                if (dataInstance.TickerCountry != "US")
                {
                    if (dataInstance.TickerCountry != previousMarket && ticker != previousTicker)
                    {
                        Log.Error($"SmartInsiderConverter.Process(): Market {dataInstance.TickerCountry} is not supported at this time for ticker {ticker} on line {i}");
                    }

                    previousMarket = dataInstance.TickerCountry;
                    previousTicker = ticker;

                    continue;
                }

                var mapFile = _mapFileResolver.ResolveMapFile(ticker, dataInstance.LastUpdate);
                if (!mapFile.Any())
                {
                    Log.Error($"SmartInsiderConverter.Process(): Failed to find mapfile for ticker {ticker} on {dataInstance.LastUpdate} on line {i}");

                    previousMarket = dataInstance.TickerCountry;
                    previousTicker = ticker;

                    continue;
                }

                var newTicker = mapFile.GetMappedSymbol(dataInstance.LastUpdate);
                if (string.IsNullOrEmpty(newTicker))
                {
                    Log.Error($"SmartInsiderConverter.Process(): Failed to resolve ticker for old ticker {ticker} on line {i}");

                    previousMarket = dataInstance.TickerCountry;
                    previousTicker = ticker;

                    continue;
                }

                // Log any mapping events since this can be a point of failure
                if (ticker != newTicker)
                {
                    Log.Trace($"SmartInsiderConverter.Process(): Mapped ticker from {ticker} to {newTicker}");
                }

                var sid = SecurityIdentifier.GenerateEquity(mapFile.FirstDate, newTicker, Market.USA);
                ProcessUniverse(sid.ToString(), dataInstance);

                List<T> symbolLines;
                if (!lines.TryGetValue(newTicker, out symbolLines))
                {
                    symbolLines = new List<T>();
                    lines[newTicker] = symbolLines;
                }

                symbolLines.Add(dataInstance);

                previousMarket = dataInstance.TickerCountry;
                previousTicker = ticker;
            }

            return lines;
        }

        /// <summary>
        /// Processes the data to universe
        /// </summary>
        /// <param name="sid">security ID string</param>
        /// <param name="data">Base class data</param>
        private void ProcessUniverse<T>(string sid, T data)
            where T : SmartInsiderEvent
        {
            if (typeof(T) == typeof(SmartInsiderIntention))
            {
                var intention = data as SmartInsiderIntention;
                ProcessUniverse(sid, intention);
            }
            else if (typeof(T) == typeof(SmartInsiderTransaction))
            {
                var transaction = data as SmartInsiderTransaction;
                ProcessUniverse(sid, transaction);
            }
        }

        /// <summary>
        /// Processes the data to universe
        /// </summary>
        /// <param name="sid">security ID string</param>
        /// <param name="data">Base class data</param>
        private void ProcessUniverse(string sid, SmartInsiderIntention data)
        {
            var date = $"{data.AnnouncementDate:yyyyMMdd}";
            var cap = data.USDMarketCap;
            var minPrice = data.MinimumPrice;
            var maxPrice = data.MaximumPrice;
            var amount = data.Amount;
            var amountValue = data.AmountValue;
            var percent = data.Percentage;
            var dataInstance = $@"{cap},{minPrice},{maxPrice},{amount},{amountValue},{percent}";

            Dictionary<string, string> dataDict;
            if (!_intentionUniverse.TryGetValue(date, out dataDict))
            {
                dataDict = new Dictionary<string, string>();
                _intentionUniverse[date] = dataDict;
            }

            if (!dataDict.ContainsKey(sid))
            {
                dataDict.Add(sid, dataInstance);
            }
            else
            {
                // Consolidate same day, same ticker value
                var oldValue = dataDict[sid].Split(",");

                var newMinPrice = minPrice;
                if (!string.IsNullOrEmpty(oldValue[3]))
                {
                    var newMin = decimal.Parse(oldValue[3], NumberStyles.Any, CultureInfo.InvariantCulture);
                    newMinPrice = newMin < minPrice ? newMin : minPrice;
                }

                var newMaxPrice = maxPrice;
                if (!string.IsNullOrEmpty(oldValue[4]))
                {
                    var newMax = decimal.Parse(oldValue[4], NumberStyles.Any, CultureInfo.InvariantCulture);
                    newMaxPrice = newMax > maxPrice ? newMax : maxPrice;
                }

                var newAmount = (string.IsNullOrEmpty(oldValue[0]) ? 0 : long.Parse(oldValue[0], NumberStyles.Any, CultureInfo.InvariantCulture)) + amount;
                var newAmountValue = (string.IsNullOrEmpty(oldValue[1]) ? 0 : long.Parse(oldValue[1], NumberStyles.Any, CultureInfo.InvariantCulture)) + amountValue;
                var newPercent = (string.IsNullOrEmpty(oldValue[2]) ? 0 : decimal.Parse(oldValue[2], NumberStyles.Any, CultureInfo.InvariantCulture)) + percent;

                dataDict[sid] = $"{cap},{newMinPrice},{newMaxPrice},{newAmount},{newAmountValue},{newPercent}";
            }
        }

        /// <summary>
        /// Processes the data to universe
        /// </summary>
        /// <param name="sid">security ID string</param>
        /// <param name="data">Base class data</param>
        private void ProcessUniverse(string sid, SmartInsiderTransaction data)
        {
            var date = $"{data.BuybackDate:yyyyMMdd}";
            var cap = data.USDMarketCap;
            var price = data.ExecutionPrice;
            var amount = data.Amount;
            var usdValue = data.USDValue;
            var buybackPercentage = data.BuybackPercentage;
            var volumePercentage = data.VolumePercentage;
            var dataInstance = $@"{cap},{price},{price},{amount},{usdValue},{buybackPercentage},{volumePercentage}";

            Dictionary<string, string> dataDict;
            if (!_transactionUniverse.TryGetValue(date, out dataDict))
            {
                dataDict = new Dictionary<string, string>();
                _transactionUniverse[date] = dataDict;
            }

            if (!dataDict.ContainsKey(sid))
            {
                dataDict.Add(sid, dataInstance);
            }
            else
            {
                // Consolidate same day, same ticker value
                var oldValue = dataDict[sid].Split(",");

                var newMinPrice = price;
                if (!string.IsNullOrEmpty(oldValue[1]))
                {
                    var newMin = decimal.Parse(oldValue[1], NumberStyles.Any, CultureInfo.InvariantCulture);
                    newMinPrice = newMin < price ? newMin : price;
                }

                var newMaxPrice = price;
                if (!string.IsNullOrEmpty(oldValue[2]))
                {
                    var newMax = decimal.Parse(oldValue[2], NumberStyles.Any, CultureInfo.InvariantCulture);
                    newMaxPrice = newMax > price ? newMax : price;
                }

                var newAmount = (string.IsNullOrEmpty(oldValue[0]) ? 0 : decimal.Parse(oldValue[0], NumberStyles.Any, CultureInfo.InvariantCulture)) + amount;
                var newValue = (string.IsNullOrEmpty(oldValue[3]) ? 0 : decimal.Parse(oldValue[3], NumberStyles.Any, CultureInfo.InvariantCulture)) + usdValue;
                var newBuybackPercentage = (string.IsNullOrEmpty(oldValue[4]) ? 0 : decimal.Parse(oldValue[4], NumberStyles.Any, CultureInfo.InvariantCulture)) + buybackPercentage;
                var newVolumePercentage = (string.IsNullOrEmpty(oldValue[5]) ? 0 : decimal.Parse(oldValue[5], NumberStyles.Any, CultureInfo.InvariantCulture)) + volumePercentage;

                dataDict[sid] = $"{cap},{newMinPrice},{newMaxPrice},{newAmount},{newValue},{newBuybackPercentage},{newVolumePercentage}";
            }
        }

        /// <summary>
        /// Writes to a temp file and moves the content to the final directory
        /// </summary>
        /// <param name="destinationDirectory">Directory to write final file to</param>
        /// <param name="contents">Contents to write to file</param>
        private void WriteToFile<T>(DirectoryInfo destinationDirectory, Dictionary<string, List<T>> contents)
            where T : SmartInsiderEvent
        {
            foreach (var kvp in contents)
            {
                var ticker = kvp.Key.ToLowerInvariant();

                var finalFile = new FileInfo(Path.Combine(destinationDirectory.FullName, $"{ticker}.tsv"));
                var processedFile = new FileInfo(Path.Combine(_processedDirectory.FullName, destinationDirectory.Name, $"{ticker}.tsv"));
                var fileContents = new List<T>();

                if (processedFile.Exists)
                {
                    Log.Trace($"SmartInsiderConverter.WriteToFile(): Writing from existing processed contents to file: {finalFile.FullName}");
                    fileContents = File.ReadAllLines(processedFile.FullName)
                        .Select(x => (T)CreateSmartInsiderInstance<T>(x))
                        .ToList();
                }
                else
                {
                    Log.Trace($"SmartInsiderConverter.WriteToFile(): Writing to new file: {finalFile.FullName}");
                }

                fileContents.AddRange(kvp.Value);

                var tsvContents = fileContents
                    .OrderBy(x => x.TimeProcessedUtc.Value)
                    .Select(x => x.ToLine())
                    .Distinct();

                File.WriteAllLines(finalFile.FullName, tsvContents);
            }
        }

        /// <summary>
        /// Writes to a temp file and moves the content to the final universe directory
        /// </summary>
        /// <param name="destinationDirectory">Directory to write final file to</param>
        /// <param name="contents">Contents to write to file</param>
        private void WriteUniverseFile(DirectoryInfo destinationDirectory, Dictionary<string, Dictionary<string, string>> contents)
        {
            Parallel.ForEach(contents, kvp =>
            {
                var date = kvp.Key;

                var finalPath = Path.Combine(destinationDirectory.FullName, "universe", $"{kvp.Key}.csv");
                var finalFileExists = File.Exists(finalPath);

                var lines = new HashSet<string>(kvp.Value.Select(kv => $"{kv.Key},{kv.Value}"));
                if (finalFileExists)
                {
                    foreach (var line in File.ReadAllLines(finalPath))
                    {
                        lines.Add(line);
                    }
                }
                var finalLines = lines.OrderBy(x => x.Split(',').First()).ToList();

                var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.tmp");
                File.WriteAllLines(tempPath, finalLines);
                var tempFilePath = new FileInfo(tempPath);
                tempFilePath.MoveTo(finalPath, true);
            });
        }

        /// <summary>
        /// Resolves type parameter to corresponding <see cref="SmartInsiderEvent"/> derived class
        /// </summary>
        /// <typeparam name="T"><see cref="SmartInsiderEvent"/> derived class</typeparam>
        /// <param name="line">CSV line</param>
        /// <returns>SmartInsiderEvent derived class</returns>
        private T CreateSmartInsiderInstance<T>(string line)
            where T : SmartInsiderEvent
        {
            if (typeof(T) == typeof(SmartInsiderIntention))
            {
                return (T)(SmartInsiderEvent)new SmartInsiderIntention(line);
            }
            if (typeof(T) == typeof(SmartInsiderTransaction))
            {
                return (T)(SmartInsiderEvent)new SmartInsiderTransaction(line);
            }

            throw new InvalidOperationException($"Smart Insider custom data source '{typeof(T).Name}' is not supported");
        }

        public void Dispose()
        {
        }
    }
}
