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

using Newtonsoft.Json;
using System;
using System.Globalization;
using System.IO;
using QuantConnect.Data;
using QuantConnect.Logging;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Smart Insider Intentions Universe
    /// </summary>
    public class SmartInsiderIntentionUniverse : BaseData
    {
        /// <summary>
        /// Number of shares to be or authorised to be traded
        /// </summary>
        public int? Amount { get; set; }

        /// <summary>
        /// Value of shares to be authorised to be traded
        /// </summary>
        public long? AmountValue { get; set; }

        /// <summary>
        /// Percentage of oustanding shares to be authorised to be traded
        /// </summary>
        public decimal? Percentage { get; set; }

        /// <summary>
        /// Minimum price shares will or may be purchased at
        /// </summary>
        public decimal? MinimumPrice { get; set; }

        /// <summary>
        /// Maximum price shares will or may be purchased at
        /// </summary>
        public decimal? MaximumPrice { get; set; }

        /// <summary>
        /// Market Capitalization in USD
        /// </summary>
        public decimal? USDMarketCap { get; set; }

        /// <summary>
        /// Time passed between the date of the data and the time the data became available to us
        /// </summary>
        public TimeSpan Period { get; set; } = TimeSpan.FromDays(1);

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + Period;

        /// <summary>
        /// Specifies the location of the data and directs LEAN where to load the data from
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="date">Algorithm date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>Subscription data source object pointing LEAN to the data location</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "smartinsider",
                    "intentions",
                    "universe",
                    $"{date:yyyyMMdd}.tsv"
                ),
                SubscriptionTransportMedium.LocalFile,
            );
        }

        /// <summary>
        /// Loads and reads the data to be used in LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">TSV line</param>
        /// <param name="date">Algorithm date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>Instance of the object</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');
            var amountValue = long.Parse(csv[3]);
            
            return new SmartInsiderIntentionUniverse(line)
            {
                Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]),
                Time = date - Period,
                Value = amountValue,

                Amount = int.Parse(csv[2]);
                AmountValue = amountValue,
                Percentage = decimal.Parse(csv[4], NumberStyles.Any, CultureInfo.InvariantCulture),
                MinimumPrice = decimal.Parse(csv[5], NumberStyles.Any, CultureInfo.InvariantCulture),
                MaximumPrice = decimal.Parse(csv[6], NumberStyles.Any, CultureInfo.InvariantCulture),
                USDMarketCap = decimal.Parse(csv[7], NumberStyles.Any, CultureInfo.InvariantCulture)
            };
        }
    }
}
