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

using NUnit.Framework;
using QuantConnect.DataSource;
using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Interfaces;
using QuantConnect.Util;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class SmartInsiderTests
    {
        private static Symbol SymbolsAAPL = Symbol.Create("AAPL", SecurityType.Equity, Market.USA);

        [OneTimeSetUp]
        public void Setup()
        {
            Composer.Instance.GetExportedValueByTypeName<IMapFileProvider>(Configuration.Config.Get("map-file-provider", typeof(LocalDiskMapFileProvider).Name));
        }

        [Test]
        public void ErrorGetsMappedToSatisfyStockVesting()
        {
            var intentionLine = "20200101 01:02:03	BIXYZ	Downwards Revision	20190101	20190101	USXYZ		1	Some Random Industry																	US	Off Market Agreement	Issuer	Missing Lookup Formula for BuybackHoldingTypeId 10.00										";
            var transactionLine = "20200101 01:02:03	BIXYZ	Downwards Revision	20190101	20190101	USXYZ		1	Some Random Industry																			Off Market Agreement	Issuer	Missing Lookup Formula for BuybackHoldingTypeId 10.00																														";

            var intention = new SmartInsiderIntention(intentionLine);
            var transaction = new SmartInsiderTransaction(transactionLine);

            Assert.AreEqual(new DateTime(2020, 1, 1, 1, 2, 3), intention.Time);
            Assert.AreEqual(new DateTime(2020, 1, 1, 1, 2, 3), transaction.Time);

            Assert.IsTrue(intention.ExecutionHolding.HasValue);
            Assert.IsTrue(transaction.ExecutionHolding.HasValue);
            Assert.AreEqual(SmartInsiderExecutionHolding.SatisfyStockVesting, intention.ExecutionHolding);
            Assert.AreEqual(SmartInsiderExecutionHolding.SatisfyStockVesting, transaction.ExecutionHolding);
        }

        [TestCase("2019-01-01  23:59:59")]
        [TestCase("01/01/2019  23:59:59")]
        public void ParsesOldAndNewTransactionDateTimeValues(string date)
        {
            var expected = new DateTime(2019, 1, 1, 23, 59, 59);
            var actual = SmartInsiderEvent.ParseDate(date);

            Assert.AreEqual(expected, actual);
        }

        [Test]
        public void ParseDateThrowsOnInvalidDateTimeValue()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                SmartInsiderEvent.ParseDate("05/21/2019 00:00:00");
            });
        }

        [Test]
        public void SerializeRoundTripSmartInsiderIntention()
        {
            var settings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All };

            var time = new DateTime(2020, 3, 19, 10, 0, 0);
            var underlyingSymbol = SymbolsAAPL;
            var symbol = Symbol.CreateBase(typeof(SmartInsiderIntention), underlyingSymbol, QuantConnect.Market.USA);

            var item = new SmartInsiderIntention
            {
                Symbol = symbol,
                LastUpdate = time,
                Time = time,
                TransactionID = "123",
                EventType = SmartInsiderEventType.Intention,
                Execution = SmartInsiderExecution.Market,
                ExecutionEntity = SmartInsiderExecutionEntity.Issuer,
                ExecutionHolding = SmartInsiderExecutionHolding.NotReported,
                Amount = null
            };

            var serialized = JsonConvert.SerializeObject(item, settings);
            var deserialized = JsonConvert.DeserializeObject<SmartInsiderIntention>(serialized, settings);

            Assert.AreEqual(symbol, deserialized.Symbol);
            Assert.AreEqual("123", deserialized.TransactionID);
            Assert.AreEqual(SmartInsiderEventType.Intention, deserialized.EventType);
            Assert.AreEqual(SmartInsiderExecution.Market, deserialized.Execution);
            Assert.AreEqual(SmartInsiderExecutionEntity.Issuer, deserialized.ExecutionEntity);
            Assert.AreEqual(SmartInsiderExecutionHolding.NotReported, deserialized.ExecutionHolding);
            Assert.AreEqual(null, deserialized.Amount);
            Assert.AreEqual(time, deserialized.LastUpdate);
            Assert.AreEqual(time, deserialized.Time);
            Assert.AreEqual(time, deserialized.EndTime);
        }

        [Test]
        public void SerializeRoundTripSmartInsiderTransaction()
        {
            var settings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All };

            var time = new DateTime(2020, 3, 19, 10, 0, 0);
            var underlyingSymbol = SymbolsAAPL;
            var symbol = Symbol.CreateBase(typeof(SmartInsiderTransaction), underlyingSymbol, QuantConnect.Market.USA);

            var item = new SmartInsiderTransaction
            {
                Symbol = symbol,
                LastUpdate = time,
                Time = time,
                TransactionID = "123",
                EventType = SmartInsiderEventType.Transaction,
                Execution = SmartInsiderExecution.Market,
                ExecutionEntity = SmartInsiderExecutionEntity.Issuer,
                ExecutionHolding = SmartInsiderExecutionHolding.SatisfyEmployeeTax,
                Amount = 1234
            };

            var serialized = JsonConvert.SerializeObject(item, settings);
            var deserialized = JsonConvert.DeserializeObject<SmartInsiderTransaction>(serialized, settings);

            Assert.AreEqual(symbol, deserialized.Symbol);
            Assert.AreEqual("123", deserialized.TransactionID);
            Assert.AreEqual(SmartInsiderEventType.Transaction, deserialized.EventType);
            Assert.AreEqual(SmartInsiderExecution.Market, deserialized.Execution);
            Assert.AreEqual(SmartInsiderExecutionEntity.Issuer, deserialized.ExecutionEntity);
            Assert.AreEqual(SmartInsiderExecutionHolding.SatisfyEmployeeTax, deserialized.ExecutionHolding);
            Assert.AreEqual(1234, deserialized.Amount);
            Assert.AreEqual(time, deserialized.LastUpdate);
            Assert.AreEqual(time, deserialized.Time);
            Assert.AreEqual(time, deserialized.EndTime);
        }

        [Test]
        public void ToLineDoesNotOutputRawNullValues()
        {
            var intentionLine = "20200101 01:02:03	BIXYZ		20190101	20190101	USXYZ		1	Some Random Industry																	US													";
            var transactionLine = "20200101 01:02:03	BIXYZ		20190101	20190101	USXYZ		1	Some Random Industry																																																			";

            var intention = new SmartInsiderIntention(intentionLine);
            var transaction = new SmartInsiderTransaction(transactionLine);

            Assert.IsNull(intention.EventType);
            Assert.IsNull(intention.Execution);
            Assert.IsNull(intention.ExecutionEntity);
            Assert.IsNull(intention.ExecutionHolding);
            Assert.IsNull(transaction.EventType);
            Assert.IsNull(transaction.Execution);
            Assert.IsNull(transaction.ExecutionEntity);
            Assert.IsNull(transaction.ExecutionHolding);

            var intentionLineSerialized = intention.ToLine().Split('\t');
            var transactionLineSerialized = transaction.ToLine().Split('\t');

            Assert.AreNotEqual(intentionLineSerialized[2], "null");
            Assert.AreNotEqual(intentionLineSerialized[26], "null");
            Assert.AreNotEqual(intentionLineSerialized[27], "null");
            Assert.AreNotEqual(intentionLineSerialized[28], "null");
            Assert.AreNotEqual(transactionLineSerialized[2], "null");
            Assert.AreNotEqual(transactionLineSerialized[27], "null");
            Assert.AreNotEqual(transactionLineSerialized[28], "null");
            Assert.AreNotEqual(transactionLineSerialized[29], "null");

            Assert.IsTrue(string.IsNullOrWhiteSpace(intentionLineSerialized[2]));
            Assert.IsTrue(string.IsNullOrWhiteSpace(intentionLineSerialized[26]));
            Assert.IsTrue(string.IsNullOrWhiteSpace(intentionLineSerialized[27]));
            Assert.IsTrue(string.IsNullOrWhiteSpace(intentionLineSerialized[28]));
            Assert.IsTrue(string.IsNullOrWhiteSpace(transactionLineSerialized[2]));
            Assert.IsTrue(string.IsNullOrWhiteSpace(transactionLineSerialized[27]));
            Assert.IsTrue(string.IsNullOrWhiteSpace(transactionLineSerialized[28]));
            Assert.IsTrue(string.IsNullOrWhiteSpace(transactionLineSerialized[29]));
        }

        [Test]
        public void ParseFromRawDataUnexpectedEventTypes()
        {
            var realRawIntentionLine = "\"BI12345\"\t\"Some new event\"\t2020-07-27\t2009-11-11\t\"US1234567890\"\t\"\"\t12345\t\"https://smartinsidercompanypage.com\"\t\"Consumer Staples\"\t" +
                                       "\"Personal Care, Drug and Grocery Stores\"\t\"Personal Care, Drug and Grocery Stores\"\t\"Personal Products\"\t12345678\t\"Some Company Corp\"\t\"Some-Comapny C\"\t" +
                                       "\"\"\t\"\"\t\"\"\t\"\"\t\"Com\"\t\"US\"\t\"SCC\"\t\"\"\t\"\"\t\"\"\t\"\"\t\"\"\t\"\"\t\"\"\t\t\t-999\t\"Some unexpected event.\"\t\"\"\t\"\"\t\"\"\t\"\"\t\"\"\t\"\"\t\"" +
                                       "\"\t2020-07-27\t\"\"\t\"\"\t\"\"\t2020-07-27  13:57:37\t\"US\"\t\"https://smartinsiderdatapage.com\"\t\"UnexpectedEvent\"\t\"UnexpectedIssuer\"\t\"UnexpectedReported\"\t\"\"\t\"\"\t\t" +
                                       "\"\"\t\t\t\"\"\t\t\t\"\"";
            var line = realRawIntentionLine.Replace("\"", "");

            var intention = new SmartInsiderIntention();
            var indexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                { "TransactionID", 0 },{ "BuybackType", 1 },{ "LastUpdate", 2 },{ "LastIDsUpdate", 3 },{ "ISIN", 4 },{ "USDMarketCap", 5 },{ "CompanyID", 6 },{ "companyPageURL", 7 },
                { "ICBIndustry", 8 },{ "ICBSuperSector", 9 },{ "ICBSector", 10 },{ "ICBSubSector", 11 },{ "ICBCode", 12 },{ "CompanyName", 13 },{ "ShortCompanyName", 14 },
                { "previousResultsAnnsDate", 15 },{ "nextResultsAnnsDate", 16 },{ "nextCloseBegin", 17 },{ "LastCloseEnded", 18 },{ "SecurityDescription", 19 },{ "TickerCountry", 20 },
                { "TickerSymbol", 21 },{ "BuybackDate", 22 },{ "BybackVia", 23 },{ "BybackBy", 24 },{ "HoldingType", 25 },{ "Currency", 26 },{ "Price", 27 },{ "TransactionAmount", 28 },
                { "GBPValue", 29 },{ "EURValue", 30 },{ "USDValue", 31 },{ "NoteText", 32 },{ "BuybackPercentage", 33 },{ "VolumePercentage", 34 },{ "ConvRate", 35 },
                { "previousClosePrice", 36 },{ "AmountAdjFactor", 37 },{ "PriceAdjFactor", 38 },{ "TreasuryHolding", 39 },{ "AnnouncementDate", 40 },{ "TimeReleased", 41 },
                { "TimeProcessed", 42 },{ "TimeReleasedGMT", 43 },{ "TimeProcessedGMT", 44 },{ "AnnouncedIn", 45 },{ "showOriginal", 46 },{ "IntentionVia", 47 },{ "IntentionBy", 48 },
                { "BuybackIntentionHoldingType", 49 },{ "IntentionAmount", 50 },{ "ValueCurrency", 51 },{ "IntentionValue", 52 },{ "IntentionPercentage", 53 },{ "StartDate", 54 },
                { "EndDate", 55 },{ "PriceCurrency", 56 },{ "MinimumPrice", 57 },{ "MaximumPrice", 58 },{ "BuybackIntentionNoteText", 59 }
            };
            Assert.DoesNotThrow(() => intention.FromRawData(line, indexes));

            Assert.IsTrue(intention.EventType.HasValue);
            Assert.AreEqual(SmartInsiderEventType.NotSpecified, intention.EventType);
            Assert.AreEqual(SmartInsiderExecution.Error, intention.Execution);
            Assert.AreEqual(SmartInsiderExecutionEntity.Error, intention.ExecutionEntity);
            Assert.AreEqual(SmartInsiderExecutionHolding.Error, intention.ExecutionHolding);
            Assert.AreEqual("US", intention.TickerCountry);
        }
    }
}
