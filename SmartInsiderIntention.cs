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
    /// Smart Insider Intentions - Intention to execute a stock buyback and details about the future event
    /// </summary>
    public class SmartInsiderIntention : SmartInsiderEvent
    {
        /// <summary>
        /// Data source ID
        /// </summary>
        public static int DataSourceId { get; } = 2018;

        /// <summary>
        /// Describes how the transaction was executed
        /// </summary>
        public SmartInsiderExecution? Execution { get; set; }

        /// <summary>
        /// Describes which entity intends to execute the transaction
        /// </summary>
        public SmartInsiderExecutionEntity? ExecutionEntity { get; set; }

        /// <summary>
        /// Describes what will be done with those shares following repurchase
        /// </summary>
        public SmartInsiderExecutionHolding? ExecutionHolding { get; set; }

        /// <summary>
        /// Number of shares to be or authorised to be traded
        /// </summary>
        public long? Amount { get; set; }

        /// <summary>
        /// Currency of the value of shares to be/Authorised to be traded (ISO Code)
        /// </summary>
        public string ValueCurrency { get; set; }

        /// <summary>
        /// Value of shares to be authorised to be traded
        /// </summary>
        public long? AmountValue { get; set; }

        /// <summary>
        /// Percentage of oustanding shares to be authorised to be traded
        /// </summary>
        public decimal? Percentage { get; set; }

        /// <summary>
        /// start of the period the intention/authorisation applies to
        /// </summary>
        public DateTime? AuthorizationStartDate { get; set; }

        /// <summary>
        /// End of the period the intention/authorisation applies to
        /// </summary>
        public DateTime? AuthorizationEndDate { get; set; }

        /// <summary>
        /// Currency of min/max prices (ISO Code)
        /// </summary>
        public string PriceCurrency { get; set; }

        /// <summary>
        /// Minimum price shares will or may be purchased at
        /// </summary>
        public decimal? MinimumPrice { get; set; }

        /// <summary>
        /// Maximum price shares will or may be purchased at
        /// </summary>
        public decimal? MaximumPrice { get; set; }

        /// <summary>
        /// Free text which explains further details about the trade
        /// </summary>
        public string NoteText { get; set; }

        /// <summary>
        /// Empty constructor required for <see cref="Slice.Get{T}()"/>
        /// </summary>
        public SmartInsiderIntention()
        {
        }

        /// <summary>
        /// Constructs instance of this via a *formatted* TSV line (tab delimited)
        /// </summary>
        /// <param name="line">Line of formatted TSV data</param>
        public SmartInsiderIntention(string line) : base(line)
        {
            var tsv = line.Split('\t');
            Execution = string.IsNullOrWhiteSpace(tsv[26]) ? (SmartInsiderExecution?)null : JsonConvert.DeserializeObject<SmartInsiderExecution>($"\"{tsv[26]}\"");
            ExecutionEntity = string.IsNullOrWhiteSpace(tsv[27]) ? (SmartInsiderExecutionEntity?)null : JsonConvert.DeserializeObject<SmartInsiderExecutionEntity>($"\"{tsv[27]}\"");
            ExecutionHolding = string.IsNullOrWhiteSpace(tsv[28]) ? (SmartInsiderExecutionHolding?)null : JsonConvert.DeserializeObject<SmartInsiderExecutionHolding>($"\"{tsv[28]}\"");
            ExecutionHolding = ExecutionHolding == SmartInsiderExecutionHolding.Error ? SmartInsiderExecutionHolding.SatisfyStockVesting : ExecutionHolding;
            Amount = string.IsNullOrWhiteSpace(tsv[29]) ? (int?)null : Convert.ToInt32(tsv[29], CultureInfo.InvariantCulture);
            ValueCurrency = string.IsNullOrWhiteSpace(tsv[30]) ? null : tsv[30];
            AmountValue = string.IsNullOrWhiteSpace(tsv[31]) ? (long?)null : Convert.ToInt64(tsv[31], CultureInfo.InvariantCulture);
            Percentage = string.IsNullOrWhiteSpace(tsv[32]) ? (decimal?)null : Convert.ToDecimal(tsv[32], CultureInfo.InvariantCulture);
            AuthorizationStartDate = string.IsNullOrWhiteSpace(tsv[33]) ? (DateTime?)null : DateTime.ParseExact(tsv[33], "yyyyMMdd", CultureInfo.InvariantCulture);
            AuthorizationEndDate = string.IsNullOrWhiteSpace(tsv[34]) ? (DateTime?)null : DateTime.ParseExact(tsv[34], "yyyyMMdd", CultureInfo.InvariantCulture);
            PriceCurrency = string.IsNullOrWhiteSpace(tsv[35]) ? null : tsv[35];
            MinimumPrice = string.IsNullOrWhiteSpace(tsv[36]) ? (decimal?)null : Convert.ToDecimal(tsv[36], CultureInfo.InvariantCulture);
            MaximumPrice = string.IsNullOrWhiteSpace(tsv[37]) ? (decimal?)null : Convert.ToDecimal(tsv[37], CultureInfo.InvariantCulture);
            NoteText = tsv.Length == 39? (string.IsNullOrWhiteSpace(tsv[38]) ? null : tsv[38]) : null;
        }

        /// <summary>
        /// Constructs a new instance from unformatted TSV data
        /// </summary>
        /// <param name="line">Line of raw TSV (raw with fields 46, 36, 14, 7 removed in descending order)</param>
        /// <param name="indexes">Index per header column</param>
        /// <returns>Instance of the object</returns>
        public override void FromRawData(string line, Dictionary<string, int> indexes)
        {
            var tsv = line.Split('\t');
            base.FromRawData(line, indexes);

            Execution = null;
            if (!string.IsNullOrWhiteSpace(tsv[indexes["IntentionVia"]]))
            {
                try
                {
                    Execution = JsonConvert.DeserializeObject<SmartInsiderExecution>($"\"{tsv[indexes["IntentionVia"]]}\"");
                }
                catch (JsonSerializationException)
                {
                    Log.Error($"SmartInsiderIntention.FromRawData(): New unexpected entry found for Execution: {tsv[indexes["IntentionVia"]]}. Parsed as Error.");
                    Execution = SmartInsiderExecution.Error;
                }
            }

            ExecutionEntity = null;
            if (!string.IsNullOrWhiteSpace(tsv[indexes["IntentionBy"]]))
            {
                try
                {
                    ExecutionEntity = JsonConvert.DeserializeObject<SmartInsiderExecutionEntity>($"\"{tsv[indexes["IntentionBy"]]}\"");
                }
                catch (JsonSerializationException)
                {
                    Log.Error($"SmartInsiderIntention.FromRawData(): New unexpected entry found for ExecutionEntity: {tsv[indexes["IntentionBy"]]}. Parsed as Error.");
                    ExecutionEntity = SmartInsiderExecutionEntity.Error;
                }
            }

            ExecutionHolding = null;
            if (!string.IsNullOrWhiteSpace(tsv[indexes["BuybackIntentionHoldingType"]]))
            {
                try
                {
                    ExecutionHolding = JsonConvert.DeserializeObject<SmartInsiderExecutionHolding>($"\"{tsv[indexes["BuybackIntentionHoldingType"]]}\"");
                    if (ExecutionHolding == SmartInsiderExecutionHolding.Error)
                    {
                        // This error in particular represents a SatisfyStockVesting field.
                        ExecutionHolding = SmartInsiderExecutionHolding.SatisfyStockVesting;
                    }
                }
                catch (JsonSerializationException)
                {
                    Log.Error($"SmartInsiderIntention.FromRawData(): New unexpected entry found for ExecutionHolding: {tsv[indexes["BuybackIntentionHoldingType"]]}. Parsed as Error.");
                    ExecutionHolding = SmartInsiderExecutionHolding.Error;

                }
            }

            Amount = string.IsNullOrWhiteSpace(tsv[indexes["IntentionAmount"]]) ? null : Convert.ToInt32(tsv[indexes["IntentionAmount"]], CultureInfo.InvariantCulture);
            ValueCurrency = string.IsNullOrWhiteSpace(tsv[indexes[nameof(ValueCurrency)]]) ? null : tsv[indexes[nameof(ValueCurrency)]];
            AmountValue = string.IsNullOrWhiteSpace(tsv[indexes["IntentionValue"]]) ? null : Convert.ToInt64(tsv[indexes["IntentionValue"]], CultureInfo.InvariantCulture);
            Percentage = string.IsNullOrWhiteSpace(tsv[indexes["IntentionPercentage"]]) ? null : Convert.ToDecimal(tsv[indexes["IntentionPercentage"]], CultureInfo.InvariantCulture);
            AuthorizationStartDate = string.IsNullOrWhiteSpace(tsv[indexes["StartDate"]]) ? null : DateTime.ParseExact(tsv[indexes["StartDate"]], "yyyy-MM-dd", CultureInfo.InvariantCulture);
            AuthorizationEndDate = string.IsNullOrWhiteSpace(tsv[indexes["EndDate"]]) ? null : DateTime.ParseExact(tsv[indexes["EndDate"]], "yyyy-MM-dd", CultureInfo.InvariantCulture);
            PriceCurrency = string.IsNullOrWhiteSpace(tsv[indexes[nameof(PriceCurrency)]]) ? null : tsv[indexes[nameof(PriceCurrency)]];
            MinimumPrice = string.IsNullOrWhiteSpace(tsv[indexes[nameof(MinimumPrice)]]) ? null : Convert.ToDecimal(tsv[indexes[nameof(MinimumPrice)]], CultureInfo.InvariantCulture);
            MaximumPrice = string.IsNullOrWhiteSpace(tsv[indexes[nameof(MaximumPrice)]]) ? null : Convert.ToDecimal(tsv[indexes[nameof(MaximumPrice)]], CultureInfo.InvariantCulture);
            NoteText = string.IsNullOrWhiteSpace(tsv[indexes["BuybackIntentionNoteText"]]) ? null : tsv[indexes["BuybackIntentionNoteText"]];
        }

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
                    $"{config.Symbol.Value.ToLowerInvariant()}.tsv"
                ),
                SubscriptionTransportMedium.LocalFile,
                FileFormat.Csv
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
            return new SmartInsiderIntention(line)
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
            return new SmartInsiderIntention()
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

                Execution = Execution,
                ExecutionEntity = ExecutionEntity,
                ExecutionHolding = ExecutionHolding,
                Amount = Amount,
                ValueCurrency = ValueCurrency,
                AmountValue = AmountValue,
                Percentage = Percentage,
                AuthorizationStartDate = AuthorizationStartDate,
                AuthorizationEndDate = AuthorizationEndDate,
                PriceCurrency = PriceCurrency,
                MinimumPrice = MinimumPrice,
                MaximumPrice = MaximumPrice,
                NoteText = NoteText,

                Symbol = Symbol,
                Value = Value,
                Time = Time,
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
                Execution == null ? null : JsonConvert.SerializeObject(Execution).Replace("\"", ""),
                ExecutionEntity == null ? null : JsonConvert.SerializeObject(ExecutionEntity).Replace("\"", ""),
                ExecutionHolding == null ? null : JsonConvert.SerializeObject(ExecutionHolding).Replace("\"", ""),
                Amount,
                ValueCurrency,
                AmountValue,
                Percentage,
                AuthorizationStartDate?.ToStringInvariant("yyyyMMdd"),
                AuthorizationEndDate?.ToStringInvariant("yyyyMMdd"),
                PriceCurrency,
                MinimumPrice,
                MaximumPrice,
                NoteText);
        }
    }
}
