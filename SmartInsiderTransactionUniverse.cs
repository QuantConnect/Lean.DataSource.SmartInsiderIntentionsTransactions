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
    /// Smart Insider Transaction Universe
    /// </summary>
    public class SmartInsiderTransactionUniverse : BaseData
    {
        private TimeSpan _period = TimeSpan.FromDays(1);
        
        /// <summary>
        /// Number of shares traded
        /// </summary>
        public decimal? Amount { get; set; }

        /// <summary>
        /// Minimum Value of Denominated in Currency of Transaction
        /// </summary>
        public decimal? MinimumExecutionPrice { get; set; }

        /// <summary>
        /// Maximum Value of Denominated in Currency of Transaction
        /// </summary>
        public decimal? MaximumExecutionPrice { get; set; }

        /// <summary>
        /// Currency conversion rates are updated daily and values are calculated at rate prevailing on the trade date
        /// </summary>
        public decimal? USDValue { get; set; }

        /// <summary>
        /// Percentage of value of the trade as part of the issuers total Market Cap
        /// </summary>
        public decimal? BuybackPercentage { get; set; }

        /// <summary>
        /// Percentage of the volume traded on the day of the buyback.
        /// </summary>
        public decimal? VolumePercentage { get; set; }

        /// <summary>
        /// Market Capitalization in USD
        /// </summary>
        public decimal? USDMarketCap { get; set; }

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + _period;

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
                    "transactions",
                    "universe",
                    $"{date:yyyyMMdd}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
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
            var usdValue = csv[6].IfNotNullOrEmpty<decimal?>(x => decimal.Parse(x, NumberStyles.Any, CultureInfo.InvariantCulture));
            
            return new SmartInsiderTransactionUniverse
            {
                Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]),
                Time = date,
                Value = Convert.ToDecimal(usdValue),

                USDMarketCap = csv[2].IfNotNullOrEmpty<decimal?>(x => decimal.Parse(x, NumberStyles.Any, CultureInfo.InvariantCulture)),
                MinimumExecutionPrice = csv[3].IfNotNullOrEmpty<decimal?>(x => decimal.Parse(x, NumberStyles.Any, CultureInfo.InvariantCulture)),
                MaximumExecutionPrice = csv[4].IfNotNullOrEmpty<decimal?>(x => decimal.Parse(x, NumberStyles.Any, CultureInfo.InvariantCulture)),
                Amount = csv[5].IfNotNullOrEmpty<decimal?>(x => decimal.Parse(x, NumberStyles.Any, CultureInfo.InvariantCulture)),
                USDValue = usdValue,
                BuybackPercentage = csv[7].IfNotNullOrEmpty<decimal?>(x => decimal.Parse(x, NumberStyles.Any, CultureInfo.InvariantCulture)),
                VolumePercentage = csv[8].IfNotNullOrEmpty<decimal?>(x => decimal.Parse(x, NumberStyles.Any, CultureInfo.InvariantCulture)),
            };
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol},{Amount},{MinimumExecutionPrice},{MaximumExecutionPrice},{USDValue},{BuybackPercentage},{VolumePercentage},{USDMarketCap}";
        }
    }
}
