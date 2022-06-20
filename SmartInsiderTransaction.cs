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
using System.IO;
using Newtonsoft.Json;
using QuantConnect.Data;
using System.Globalization;
using QuantConnect.Logging;
using System.Collections.Generic;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Smart Insider Transaction - Execution of a stock buyback and details about the event occurred
    /// </summary>
    public class SmartInsiderTransaction : SmartInsiderEvent
    {
        /// <summary>
        /// Data source ID
        /// </summary>
        public static int DataSourceId { get; } = 2019;

        /// <summary>
        /// Date traded through the market
        /// </summary>
        public DateTime? BuybackDate { get; set; }

        /// <summary>
        /// Describes how transaction was executed
        /// </summary>
        public SmartInsiderExecution? Execution { get; set; }

        /// <summary>
        /// Describes which entity carried out the transaction
        /// </summary>
        public SmartInsiderExecutionEntity? ExecutionEntity { get; set; }

        /// <summary>
        /// Describes what will be done with those shares following repurchase
        /// </summary>
        public SmartInsiderExecutionHolding? ExecutionHolding { get; set; }

        /// <summary>
        /// Currency of transation (ISO Code)
        /// </summary>
        public string Currency { get; set; }

        /// <summary>
        /// Denominated in Currency of Transaction
        /// </summary>
        public decimal? ExecutionPrice { get; set; }

        /// <summary>
        /// Number of shares traded
        /// </summary>
        public decimal? Amount { get; set; }

        /// <summary>
        /// Currency conversion rates are updated daily and values are calculated at rate prevailing on the trade date
        /// </summary>
        public decimal? GBPValue { get; set; }

        /// <summary>
        /// Currency conversion rates are updated daily and values are calculated at rate prevailing on the trade date
        /// </summary>
        public decimal? EURValue { get; set; }

        /// <summary>
        /// Currency conversion rates are updated daily and values are calculated at rate prevailing on the trade date
        /// </summary>
        public decimal? USDValue { get; set; }

        /// <summary>
        /// Free text which expains futher details about the trade
        /// </summary>
        public string NoteText { get; set; }

        /// <summary>
        /// Percentage of value of the trade as part of the issuers total Market Cap
        /// </summary>
        public decimal? BuybackPercentage { get; set; }

        /// <summary>
        /// Percentage of the volume traded on the day of the buyback.
        /// </summary>
        public decimal? VolumePercentage { get; set; }

        /// <summary>
        /// Rate used to calculate 'Value (GBP)' from 'Price' multiplied by 'Amount'. Will be 1 where Currency is also 'GBP'
        /// </summary>
        public decimal? ConversionRate { get; set; }

        /// <summary>
        /// Multiplier which can be applied to 'Amount' field to account for subsequent corporate action
        /// </summary>
        public decimal? AmountAdjustedFactor { get; set; }

        /// <summary>
        /// Multiplier which can be applied to 'Price' and 'LastClose' fields to account for subsequent corporate actions
        /// </summary>
        public decimal? PriceAdjustedFactor { get; set; }

        /// <summary>
        /// Post trade holding of the Treasury or Trust in the security traded
        /// </summary>
        public long? TreasuryHolding { get; set; }

        /// <summary>
        /// Empty contsructor required for <see cref="Slice.Get{T}()"/>
        /// </summary>
        public SmartInsiderTransaction()
        {
        }

        /// <summary>
        /// Creates an instance of the object by taking a formatted TSV line
        /// </summary>
        /// <param name="line">Line of formatted TSV</param>
        public SmartInsiderTransaction(string line) : base(line)
        {
            var tsv = line.Split('\t');

            BuybackDate = string.IsNullOrWhiteSpace(tsv[26]) ? (DateTime?)null : DateTime.ParseExact(tsv[26], "yyyyMMdd", CultureInfo.InvariantCulture);
            Execution = string.IsNullOrWhiteSpace(tsv[27]) ? (SmartInsiderExecution?)null : JsonConvert.DeserializeObject<SmartInsiderExecution>($"\"{tsv[27]}\"");
            ExecutionEntity = string.IsNullOrWhiteSpace(tsv[28]) ? (SmartInsiderExecutionEntity?)null : JsonConvert.DeserializeObject<SmartInsiderExecutionEntity>($"\"{tsv[28]}\"");
            ExecutionHolding = string.IsNullOrWhiteSpace(tsv[29]) ? (SmartInsiderExecutionHolding?)null : JsonConvert.DeserializeObject<SmartInsiderExecutionHolding>($"\"{tsv[29]}\"");
            ExecutionHolding = ExecutionHolding == SmartInsiderExecutionHolding.Error ? SmartInsiderExecutionHolding.SatisfyStockVesting : ExecutionHolding;
            Currency = string.IsNullOrWhiteSpace(tsv[30]) ? null : tsv[30];
            ExecutionPrice = string.IsNullOrWhiteSpace(tsv[31]) ? (decimal?)null : Convert.ToDecimal(tsv[31], CultureInfo.InvariantCulture);
            Amount = string.IsNullOrWhiteSpace(tsv[32]) ? (decimal?)null : Convert.ToDecimal(tsv[32], CultureInfo.InvariantCulture);
            GBPValue = string.IsNullOrWhiteSpace(tsv[33]) ? (decimal?)null : Convert.ToDecimal(tsv[33], CultureInfo.InvariantCulture);
            EURValue = string.IsNullOrWhiteSpace(tsv[34]) ? (decimal?)null : Convert.ToDecimal(tsv[34], CultureInfo.InvariantCulture);
            USDValue = string.IsNullOrWhiteSpace(tsv[35]) ? (decimal?)null : Convert.ToDecimal(tsv[35], CultureInfo.InvariantCulture);
            NoteText = string.IsNullOrWhiteSpace(tsv[36]) ? null : tsv[36];
            BuybackPercentage = string.IsNullOrWhiteSpace(tsv[37]) ? (decimal?)null : Convert.ToDecimal(tsv[37], CultureInfo.InvariantCulture);
            VolumePercentage = string.IsNullOrWhiteSpace(tsv[38]) ? (decimal?)null : Convert.ToDecimal(tsv[38], CultureInfo.InvariantCulture);
            ConversionRate = string.IsNullOrWhiteSpace(tsv[39]) ? (decimal?)null : Convert.ToDecimal(tsv[39], CultureInfo.InvariantCulture);
            AmountAdjustedFactor = string.IsNullOrWhiteSpace(tsv[40]) ? (decimal?)null : Convert.ToDecimal(tsv[40], CultureInfo.InvariantCulture);
            PriceAdjustedFactor = string.IsNullOrWhiteSpace(tsv[41]) ? (decimal?)null : Convert.ToDecimal(tsv[41], CultureInfo.InvariantCulture);
            TreasuryHolding = string.IsNullOrWhiteSpace(tsv[42]) ? (int?)null : Convert.ToInt64(tsv[42], CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Creates an instance of the object by taking a formatted TSV line
        /// </summary>
        /// <param name="line">Line of formatted TSV</param>
        /// <param name="indexes">Index per header column</param>
        /// <returns>success of the parsing task</returns>
        public override bool FromRawData(string line, Dictionary<string, int> indexes)
        {
            try
            {
                var tsv = line.Split('\t');
                if (!base.FromRawData(line, indexes))
                {
                    return false;
                }

                BuybackDate = string.IsNullOrWhiteSpace(tsv[indexes[nameof(BuybackDate)]]) ? null : DateTime.ParseExact(tsv[indexes[nameof(BuybackDate)]], "yyyy-MM-dd", CultureInfo.InvariantCulture);

                Execution = null;
                if (!string.IsNullOrWhiteSpace(tsv[indexes["BybackVia"]]))
                {
                    try
                    {
                        Execution = JsonConvert.DeserializeObject<SmartInsiderExecution>($"\"{tsv[indexes["BybackVia"]]}\"");
                    }
                    catch (JsonSerializationException)
                    {
                        Log.Error($"SmartInsiderTransaction.FromRawData(): New unexpected entry found for Execution: {tsv[indexes["BybackVia"]]}. Parsed as Error.");
                        Execution = SmartInsiderExecution.Error;
                    }
                }

                ExecutionEntity = null;
                if (!string.IsNullOrWhiteSpace(tsv[indexes["BybackBy"]]))
                {
                    try
                    {
                        ExecutionEntity = JsonConvert.DeserializeObject<SmartInsiderExecutionEntity>($"\"{tsv[indexes["BybackBy"]]}\"");
                    }
                    catch (JsonSerializationException)
                    {
                        Log.Error($"SmartInsiderTransaction.FromRawData(): New unexpected entry found for ExecutionEntity: {tsv[indexes["BybackBy"]]}. Parsed as Error.");
                        ExecutionEntity = SmartInsiderExecutionEntity.Error;
                    }
                }

                ExecutionHolding = null;
                if (!string.IsNullOrWhiteSpace(tsv[indexes["HoldingType"]]))
                {
                    try
                    {
                        ExecutionHolding = JsonConvert.DeserializeObject<SmartInsiderExecutionHolding>($"\"{tsv[indexes["HoldingType"]]}\"");
                        if (ExecutionHolding == SmartInsiderExecutionHolding.Error)
                        {
                            // This error in particular represents a SatisfyStockVesting field.
                            ExecutionHolding = SmartInsiderExecutionHolding.SatisfyStockVesting;
                        }
                    }
                    catch (JsonSerializationException)
                    {
                        Log.Error($"SmartInsiderTransaction.FromRawData(): New unexpected entry found for ExecutionHolding: {tsv[indexes["HoldingType"]]}. Parsed as Error.");
                        ExecutionHolding = SmartInsiderExecutionHolding.Error;
                    }
                }

                Currency = string.IsNullOrWhiteSpace(tsv[indexes[nameof(Currency)]]) ? null : tsv[indexes[nameof(Currency)]];
                ExecutionPrice = string.IsNullOrWhiteSpace(tsv[indexes["Price"]]) ? null : Convert.ToDecimal(tsv[indexes["Price"]], CultureInfo.InvariantCulture);
                Amount = string.IsNullOrWhiteSpace(tsv[indexes["TransactionAmount"]]) ? null : Convert.ToDecimal(tsv[indexes["TransactionAmount"]], CultureInfo.InvariantCulture);
                GBPValue = string.IsNullOrWhiteSpace(tsv[indexes[nameof(GBPValue)]]) ? null : Convert.ToDecimal(tsv[indexes[nameof(GBPValue)]], CultureInfo.InvariantCulture);
                EURValue = string.IsNullOrWhiteSpace(tsv[indexes[nameof(EURValue)]]) ? null : Convert.ToDecimal(tsv[indexes[nameof(EURValue)]], CultureInfo.InvariantCulture);
                USDValue = string.IsNullOrWhiteSpace(tsv[indexes[nameof(USDValue)]]) ? null : Convert.ToDecimal(tsv[indexes[nameof(USDValue)]], CultureInfo.InvariantCulture);
                NoteText = string.IsNullOrWhiteSpace(tsv[indexes[nameof(NoteText)]]) ? null : tsv[indexes[nameof(NoteText)]];
                BuybackPercentage = string.IsNullOrWhiteSpace(tsv[indexes[nameof(BuybackPercentage)]]) ? null : Convert.ToDecimal(tsv[indexes[nameof(BuybackPercentage)]], CultureInfo.InvariantCulture);
                VolumePercentage = string.IsNullOrWhiteSpace(tsv[indexes[nameof(VolumePercentage)]]) ? null : Convert.ToDecimal(tsv[indexes[nameof(VolumePercentage)]], CultureInfo.InvariantCulture);
                ConversionRate = string.IsNullOrWhiteSpace(tsv[indexes["ConvRate"]]) ? null : Convert.ToDecimal(tsv[indexes["ConvRate"]], CultureInfo.InvariantCulture);
                AmountAdjustedFactor = string.IsNullOrWhiteSpace(tsv[indexes["AmountAdjFactor"]]) ? null : Convert.ToDecimal(tsv[indexes["AmountAdjFactor"]], CultureInfo.InvariantCulture);
                PriceAdjustedFactor = string.IsNullOrWhiteSpace(tsv[indexes["PriceAdjFactor"]]) ? null : Convert.ToDecimal(tsv[indexes["PriceAdjFactor"]], CultureInfo.InvariantCulture);
                TreasuryHolding = string.IsNullOrWhiteSpace(tsv[indexes[nameof(TreasuryHolding)]]) ? null : Convert.ToInt64(tsv[indexes[nameof(TreasuryHolding)]], CultureInfo.InvariantCulture);

                return true;
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }
        }

        /// <summary>
        /// Specifies the location of the data and directs LEAN where to load the data from
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="date">Date</param>
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
                    $"{config.Symbol.Value.ToLowerInvariant()}.tsv"
                ),
                SubscriptionTransportMedium.LocalFile,
                FileFormat.Csv
            );
        }

        /// <summary>
        /// Reads the data into LEAN for use in algorithms
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of TSV</param>
        /// <param name="date">Algorithm date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>Instance of the object</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            return new SmartInsiderTransaction(line)
            {
                Symbol = config.Symbol
            };
        }

        /// <summary>
        /// Clones the object to a new instance. This method
        /// is required for custom data sources that make use
        /// of properties with more complex types since otherwise
        /// the values will default to null using the default clone method
        /// </summary>
        /// <returns>A new cloned instance of this object</returns>
        public override BaseData Clone()
        {
            return new SmartInsiderTransaction
            {
                TransactionID = TransactionID,
                EventType = EventType,
                LastUpdate = LastUpdate,
                LastIDsUpdate = LastIDsUpdate,
                ISIN = ISIN,
                USDMarketCap = USDMarketCap,
                CompanyID = CompanyID,
                ICBIndustry = ICBIndustry,
                ICBSuperSector = ICBSuperSector,
                ICBSector = ICBSector,
                ICBSubSector = ICBSubSector,
                ICBCode = ICBCode,
                CompanyName = CompanyName,
                PreviousResultsAnnouncementDate = PreviousResultsAnnouncementDate,
                NextResultsAnnouncementsDate = NextResultsAnnouncementsDate,
                NextCloseBegin = NextCloseBegin,
                LastCloseEnded = LastCloseEnded,
                SecurityDescription = SecurityDescription,
                TickerCountry = TickerCountry,
                TickerSymbol = TickerSymbol,
                AnnouncementDate = AnnouncementDate,
                TimeReleased = TimeReleased,
                TimeProcessed = TimeProcessed,
                TimeReleasedUtc = TimeReleasedUtc,
                TimeProcessedUtc = TimeProcessedUtc,
                AnnouncedIn = AnnouncedIn,

                BuybackDate = BuybackDate,
                Execution = Execution,
                ExecutionEntity = ExecutionEntity,
                ExecutionHolding = ExecutionHolding,
                Currency = Currency,
                ExecutionPrice = ExecutionPrice,
                Amount = Amount,
                GBPValue = GBPValue,
                EURValue = EURValue,
                USDValue = USDValue,
                NoteText = NoteText,
                BuybackPercentage = BuybackPercentage,
                VolumePercentage = VolumePercentage,
                ConversionRate = ConversionRate,
                AmountAdjustedFactor = AmountAdjustedFactor,
                PriceAdjustedFactor = PriceAdjustedFactor,
                TreasuryHolding = TreasuryHolding,

                Symbol = Symbol,
                Value = Value,
                Time = Time
            };
        }

        /// <summary>
        /// Converts the data to TSV
        /// </summary>
        /// <returns>String of TSV</returns>
        /// <remarks>Parsable by the constructor should you need to recreate the object from TSV</remarks>
        public override string ToLine()
        {
            return string.Join("\t",
                TimeProcessedUtc?.ToStringInvariant("yyyyMMdd HH:mm:ss"),
                TransactionID,
                EventType == null ? null : JsonConvert.SerializeObject(EventType).Replace("\"", ""),
                LastUpdate.ToStringInvariant("yyyyMMdd"),
                LastIDsUpdate?.ToStringInvariant("yyyyMMdd"),
                ISIN,
                USDMarketCap,
                CompanyID,
                ICBIndustry,
                ICBSuperSector,
                ICBSector,
                ICBSubSector,
                ICBCode,
                CompanyName,
                PreviousResultsAnnouncementDate?.ToStringInvariant("yyyyMMdd"),
                NextResultsAnnouncementsDate?.ToStringInvariant("yyyyMMdd"),
                NextCloseBegin?.ToStringInvariant("yyyyMMdd"),
                LastCloseEnded?.ToStringInvariant("yyyyMMdd"),
                SecurityDescription,
                TickerCountry,
                TickerSymbol,
                AnnouncementDate?.ToStringInvariant("yyyyMMdd"),
                TimeReleased?.ToStringInvariant("yyyyMMdd HH:mm:ss"),
                TimeProcessed?.ToStringInvariant("yyyyMMdd HH:mm:ss"),
                TimeReleasedUtc?.ToStringInvariant("yyyyMMdd HH:mm:ss"),
                AnnouncedIn,
                BuybackDate?.ToStringInvariant("yyyyMMdd"),
                Execution == null ? null : JsonConvert.SerializeObject(Execution).Replace("\"", ""),
                ExecutionEntity == null ? null : JsonConvert.SerializeObject(ExecutionEntity).Replace("\"", ""),
                ExecutionHolding == null ? null : JsonConvert.SerializeObject(ExecutionHolding).Replace("\"", ""),
                Currency,
                ExecutionPrice,
                Amount,
                GBPValue,
                EURValue,
                USDValue,
                NoteText,
                BuybackPercentage,
                VolumePercentage,
                ConversionRate,
                AmountAdjustedFactor,
                PriceAdjustedFactor,
                TreasuryHolding);
        }
    }
}
