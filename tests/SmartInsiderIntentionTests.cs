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
using ProtoBuf;
using System.IO;
using System.Linq;
using ProtoBuf.Meta;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class SmartInsiderIntentionTests
    {
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
	[Ignore("ProtoBuf not implemented for this data type yet")]
        public void ProtobufRoundTrip()
        {
            var expected = CreateNewInstance();
            var type = expected.GetType();

            RuntimeTypeModel.Default[typeof(BaseData)].AddSubType(2000, type);

            using (var stream = new MemoryStream())
            {
                Serializer.Serialize(stream, expected);

                stream.Position = 0;

                var result = Serializer.Deserialize(type, stream);

                AssertAreEqual(expected, result, filterByCustomAttributes: true);
            }
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
