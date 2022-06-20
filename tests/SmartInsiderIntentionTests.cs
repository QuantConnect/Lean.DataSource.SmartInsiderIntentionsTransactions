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
 *
*/

using System;
using System.Linq;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class SmartInsiderIntentionTests
    {
        [TestCase("\"TransactionID\"	\"BuybackType\"	\"LastUpdate\"	\"LastIDsUpdate\"	\"ISIN\"	\"USDMarketCap\"	\"CompanyID\"	\"companyPageURL\"	\"ICBIndustry\"	\"ICBSuperSector\"	\"ICBSector\"	\"ICBSubSector\"	\"ICBCode\"	\"CompanyName\"	\"ShortCompanyName\"	\"previousResultsAnnsDate\"	\"nextResultsAnnsDate\"	\"nextCloseBegin\"	\"LastCloseEnded\"	\"SecurityDescription\"	\"TickerCountry\"	\"TickerSymbol\"	\"BuybackDate\"	\"BybackVia\"	\"BybackBy\"	\"HoldingType\"	\"Currency\"	\"Price\"	\"TransactionAmount\"	\"GBPValue\"	\"EURValue\"	\"USDValue\"	\"NoteText\"	\"BuybackPercentage\"	\"VolumePercentage\"	\"ConvRate\"	\"previousClosePrice\"	\"AmountAdjFactor\"	\"PriceAdjFactor\"	\"TreasuryHolding\"	\"AnnouncementDate\"	\"TimeReleased\"	\"TimeProcessed\"	\"TimeReleasedGMT\"	\"TimeProcessedGMT\"	\"AnnouncedIn\"	\"showOriginal\"	\"IntentionVia\"	\"IntentionBy\"	\"BuybackIntentionHoldingType\"	\"IntentionAmount\"	\"ValueCurrency\"	\"IntentionValue\"	\"IntentionPercentage\"	\"StartDate\"	\"EndDate\"	\"PriceCurrency\"	\"MinimumPrice\"	\"MaximumPrice\"	\"BuybackIntentionNoteText\"	\"UserVarChar1\"	\"UserVarChar2\"	\"UserVarChar3\"	\"UserVarChar4\"	\"UserVarChar5\"	\"SystemVarChar1\"	\"SystemVarChar2\"	\"SystemVarChar3\"	\"SystemVarChar4\"	\"SystemVarChar5\"	\"UserNumber1\"	\"UserNumber2\"	\"UserNumber3\"	\"UserNumber4\"	\"UserNumber5\"	\"SystemNumber1\"	\"SystemNumber2\"	\"SystemNumber3\"	\"SystemNumber4\"	\"SystemNumber5\"	\"UserInteger1\"	\"UserInteger2\"	\"UserInteger3\"	\"UserInteger4\"	\"UserInteger5\"	\"SystemInteger1\"	\"SystemInteger2\"	\"SystemInteger3\"	\"SystemInteger4\"	\"SystemInteger5\"	\"UserDateTime1\"	\"UserDateTime2\"	\"UserDateTime3\"	\"UserDateTime4\"	\"UserDateTime5\"	\"SystemDateTime1\"	\"SystemDateTime2\"	\"SystemDateTime3\"	\"SystemDateTime4\"	\"SystemDateTime5\"",
                  "\"BI15149\"	\"Authorisation\"	2021-08-01	2021-08-02	\"IT0001044996\"	\"\"	88056	\"https://data.smartinsider.com/members/company?c=15149\"	\"Financials\"	\"Financial Services\"	\"Investment Banking and Brokerage Services\"	\"Diversified Financial Services\"	30202000	\"Dovalue S.P.A\"	\"Dovalues S.P.A\"	\"\"	\"\"	\"\"	\"\"	\"Ord NPV\"	\"IT\"	\"DOV\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	4180028	4865000	5775549	\" \"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	2021-06-18	\"\"	\"\"	\"\"	2021-06-23  12:49:11	\"IT\"	\"https://data.smartinsider.com/members/linkbi?bid=15149\"	\"On Market\"	\"Broker\"	\"In Treasury\"	\"500000\"	\"\"		\"\"	2021-07-01	2021-12-31	\"\"			\"\"",
                  ExpectedResult=true)]
        [TestCase("\"TransactionID\"	\"BuybackType\"	\"LastUpdate\"	\"LastIDsUpdate\"	\"ISIN\"	\"USDMarketCap\"	\"CompanyID\"	\"companyPageURL\"	\"ICBIndustry\"	\"ICBSuperSector\"	\"ICBSector\"	\"ICBSubSector\"	\"ICBCode\"	\"CompanyName\"	\"ShortCompanyName\"	\"previousResultsAnnsDate\"	\"nextResultsAnnsDate\"	\"nextCloseBegin\"	\"LastCloseEnded\"	\"SecurityDescription\"	\"TickerCountry\"	\"TickerSymbol\"	\"BuybackDate\"	\"BybackVia\"	\"BybackBy\"	\"HoldingType\"	\"Currency\"	\"Price\"	\"TransactionAmount\"	\"GBPValue\"	\"EURValue\"	\"USDValue\"	\"NoteText\"	\"BuybackPercentage\"	\"VolumePercentage\"	\"ConvRate\"	\"previousClosePrice\"	\"AmountAdjFactor\"	\"PriceAdjFactor\"	\"TreasuryHolding\"	\"AnnouncementDate\"	\"TimeReleased\"	\"TimeProcessed\"	\"TimeReleasedGMT\"	\"TimeProcessedGMT\"	\"AnnouncedIn\"	\"showOriginal\"	\"IntentionVia\"	\"IntentionBy\"	\"BuybackIntentionHoldingType\"	\"IntentionAmount\"	\"ValueCurrency\"	\"IntentionValue\"	\"IntentionPercentage\"	\"StartDate\"	\"EndDate\"	\"PriceCurrency\"	\"MinimumPrice\"	\"MaximumPrice\"	\"BuybackIntentionNoteText\"	\"UserVarChar1\"	\"UserVarChar2\"	\"UserVarChar3\"	\"UserVarChar4\"	\"UserVarChar5\"	\"SystemVarChar1\"	\"SystemVarChar2\"	\"SystemVarChar3\"	\"SystemVarChar4\"	\"SystemVarChar5\"	\"UserNumber1\"	\"UserNumber2\"	\"UserNumber3\"	\"UserNumber4\"	\"UserNumber5\"	\"SystemNumber1\"	\"SystemNumber2\"	\"SystemNumber3\"	\"SystemNumber4\"	\"SystemNumber5\"	\"UserInteger1\"	\"UserInteger2\"	\"UserInteger3\"	\"UserInteger4\"	\"UserInteger5\"	\"SystemInteger1\"	\"SystemInteger2\"	\"SystemInteger3\"	\"SystemInteger4\"	\"SystemInteger5\"	\"UserDateTime1\"	\"UserDateTime2\"	\"UserDateTime3\"	\"UserDateTime4\"	\"UserDateTime5\"	\"SystemDateTime1\"	\"SystemDateTime2\"	\"SystemDateTime3\"	\"SystemDateTime4\"	\"SystemDateTime5\"",
                  "\"BI15149\"	\"Authorisation\"	2021-08-AA	2021-08-02	\"IT0001044996\"	\"\"	88056	\"https://data.smartinsider.com/members/company?c=15149\"	\"Financials\"	\"Financial Services\"	\"Investment Banking and Brokerage Services\"	\"Diversified Financial Services\"	30202000	\"Dovalue S.P.A\"	\"Dovalues S.P.A\"	\"\"	\"\"	\"\"	\"\"	\"Ord NPV\"	\"IT\"	\"DOV\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	4180028	4865000	57755496510654064065406065005640651406534065410654608540686350130438504	\" \"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	\"\"	2021-06-18	\"\"	\"\"	\"\"	2021-06-23  12:49:11	\"IT\"	\"https://data.smartinsider.com/members/linkbi?bid=15149\"	\"On Market\"	\"Broker\"	\"In Treasury\"	\"500000\"	\"\"		\"\"	2021-07-01	2021-12-31	\"\"			\"\"",
                  ExpectedResult=false)]
        public bool FromRawFileParseTest(string header, string line)
        {
            var headers = header.Replace("\"", "").Split('\t').ToList();
            var indexes = headers.ToDictionary(s => s, s => headers.IndexOf(s), StringComparer.OrdinalIgnoreCase);
            
            var instance = new SmartInsiderIntention();
            return instance.FromRawData(line.Replace("\"", ""), indexes);
        }
        
        [Test]
        public void JsonRoundTrip()
        {
            var expected = CreateNewInstance();
            var type = expected.GetType();
            var serialized = JsonConvert.SerializeObject(expected);
            var result = JsonConvert.DeserializeObject(serialized, type);

            AssertAreEqual(expected, result);
        }

        [Test]
        public void Clone()
        {
            var expected = CreateNewInstance();
            var result = expected.Clone();

            AssertAreEqual(expected, result);
        }

        private void AssertAreEqual(object expected, object result, bool filterByCustomAttributes = false)
        {
            foreach (var propertyInfo in expected.GetType().GetProperties())
            {
                // we skip Symbol which isn't protobuffed
                if (filterByCustomAttributes && propertyInfo.CustomAttributes.Count() != 0)
                {
                    Assert.AreEqual(propertyInfo.GetValue(expected), propertyInfo.GetValue(result));
                }
            }
            foreach (var fieldInfo in expected.GetType().GetFields())
            {
                Assert.AreEqual(fieldInfo.GetValue(expected), fieldInfo.GetValue(result));
            }
        }

        private BaseData CreateNewInstance()
        {
            return new SmartInsiderIntention
            {
                Symbol = Symbol.Empty,
                Time = DateTime.Today,
                DataType = MarketDataType.Base,

                TransactionID = "12345",
                EventType = SmartInsiderEventType.Intention,
                LastUpdate = new DateTime(2021, 6, 29),
                LastIDsUpdate = null,
                ISIN = "10923409823409",
                USDMarketCap = null,
                CompanyID = 1000,
                ICBIndustry = "Rare Metals",
                ICBSuperSector = "Materials",
                ICBSector = "Mining",
                ICBSubSector = "Gold",
                ICBCode = null,
                CompanyName = "ABCDEFGHIJKLMNOP",
                PreviousResultsAnnouncementDate = null,
                NextResultsAnnouncementsDate = null,
                NextCloseBegin = null,
                LastCloseEnded = null,
                SecurityDescription = "unknown",
                TickerCountry = "US",
                TickerSymbol = "ABCDEFGHIJKLMNOP",
                AnnouncementDate = new DateTime(2021, 6, 29),
                TimeReleased = new DateTime(2021, 6, 29, 8, 0, 0),
                TimeProcessed = new DateTime(2021, 6, 29, 4, 0, 0),
                TimeReleasedUtc = new DateTime(2021, 6, 29, 8, 0, 0),
                TimeProcessedUtc = new DateTime(2021, 6, 29, 4, 0, 0),
                AnnouncedIn = "US",

                Execution = SmartInsiderExecution.Market,
                ExecutionEntity = SmartInsiderExecutionEntity.Broker,
                ExecutionHolding = SmartInsiderExecutionHolding.SatisfyStockVesting,
                Amount = null,
                ValueCurrency = "USD",
                AmountValue = null,
                Percentage = null,
                AuthorizationStartDate = null,
                AuthorizationEndDate = null,
                PriceCurrency = "USD",
                MinimumPrice = 12m,
                MaximumPrice = 13m,
                NoteText = "This is a test"
            };
        }
    }
}
