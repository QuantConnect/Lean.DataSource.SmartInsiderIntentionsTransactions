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
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Auxiliary;
using QuantConnect.DataProcessing;
using QuantConnect.DataSource;
using QuantConnect.Interfaces;
using QuantConnect.Util;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class SmartInsiderTransactionUniverseTests
    {
        [OneTimeSetUp]
        public void Setup()
        {
            Composer.Instance.GetExportedValueByTypeName<IMapFileProvider>(Configuration.Config.Get("map-file-provider", typeof(LocalDiskMapFileProvider).Name));
        }

        [TestCase(
            new string[]{"20220309 11:53:36	BT812996	Transaction	20220309	20191004	US00846U1016	38843345345	27276	Health Care	Health Care	Medical Equipment and Services	Medical Equipment	20102010	Agilent Technologies Inc	20211217	20220222	20220131	20211217	Com	US	A	20220309	20220303 17:02:17		20220303 22:02:17	US	20211231	On Market	Issuer	For Cancellation	USD	154.4500	2038115	232558230	276699052	314786862		0.0081	6.0456	1.350000			"}, 
            "20220309",
            ExpectedResult = "SID,ticker,38843345345,154.4500,154.4500,2038115,314786862,0.0081,6.0456")]
        [TestCase(
            new string[]{"20220309 11:52:47	BT812992	Transaction	20220309	20191004	US00846U1016	38843345345	27276	Health Care	Health Care	Medical Equipment and Services	Medical Equipment	20102010	Agilent Technologies Inc	20210901	20211217	20220131	20210901	Com	US	A	20220309	20220303 17:02:17		20220303 22:02:17	US	20211130	On Market	Issuer	For Cancellation	USD	154.9300	675100	78949162	92660500	104593271		0.0027	1.9706	1.320000			",
                         "20220309 11:53:36	BT812996	Transaction	20220309	20191004	US00846U1016	38843345345	27276	Health Care	Health Care	Medical Equipment and Services	Medical Equipment	20102010	Agilent Technologies Inc	20211217	20220222	20220131	20211217	Com	US	A	20220309	20220303 17:02:17		20220303 22:02:17	US	20211231	On Market	Issuer	For Cancellation	USD	154.4500	2038115	232558230	276699052	314786862		0.0081	6.0456	1.350000			"},
            "20220309",
            ExpectedResult = "SID,ticker,38843345345,154.4500,154.9300,2713215,419380133,0.0108,8.0162")]
        public string ProcessUniverseTest(string[] tickerData, string date)
        {
            var instance = new TestSmartInsiderConverter();

            foreach (var line in tickerData)
            {
                var smartInsiderTransaction = new SmartInsiderTransaction(line);
                instance.TestProcessUniverse("SID,ticker", smartInsiderTransaction);
            }

            var transactionUniverse = instance.GetTransactionUniverse();
            var result = transactionUniverse[date].First();

            return $"{result.Key},{result.Value}";
        }

        [Test]
        public void Selection()
        {
            var datum = CreateNewSelection();

            var expected = from d in datum
                            where d.USDValue > 1 && d.USDMarketCap >= 300m
                            select d.Symbol;
            var result = new List<Symbol> {Symbol.Create("AA", SecurityType.Equity, Market.USA)};

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
            return new SmartInsiderTransactionUniverse
            {
                Symbol = Symbol.Create("A", SecurityType.Equity, Market.USA),
                Time = DateTime.Today,

                Amount = 10,
                MinimumExecutionPrice = 1m,
                MaximumExecutionPrice = 2m,
                USDValue = 200,
                BuybackPercentage = 0.1m,
                VolumePercentage = 0.2m,
                USDMarketCap = 60m
            };
        }

        private IEnumerable<SmartInsiderTransactionUniverse> CreateNewSelection()
        {
            return new []
            {
                new SmartInsiderTransactionUniverse
                {
                    Symbol = Symbol.Create("A", SecurityType.Equity, Market.USA),
                    Time = DateTime.Today,

                    Amount = 10,
                    MinimumExecutionPrice = 1m,
                    MaximumExecutionPrice = 2m,
                    USDValue = 200,
                    BuybackPercentage = 0.1m,
                    VolumePercentage = 0.2m,
                    USDMarketCap = 60m
                },
                new SmartInsiderTransactionUniverse
                {
                    Symbol = Symbol.Create("AA", SecurityType.Equity, Market.USA),
                    Time = DateTime.Today,

                    Amount = 50,
                    MinimumExecutionPrice = 4m,
                    MaximumExecutionPrice = 5m,
                    USDValue = 600,
                    BuybackPercentage = 0.3m,
                    VolumePercentage = 0.4m,
                    USDMarketCap = 600m
                }
            };
        }
    }
}