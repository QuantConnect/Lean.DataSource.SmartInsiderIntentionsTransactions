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
using System.Linq;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;
using QuantConnect.Securities;

namespace QuantConnect.Algorithm.CSharp
{
    public class SmartInsiderTransactionUniverseTestAlgorithm : QCAlgorithm
    {
        private readonly Symbol _symbol = QuantConnect.Symbol.Create("AAPL", SecurityType.Equity, Market.USA);
        private Security _smartInsiderTransaction;
        private SmartInsiderTransactionUniverse _datum;
        private List<SmartInsiderTransaction> _collection = new();
        private DateTime _day;

        public override void Initialize()
        {
            // Data ADDED via universe selection is added with Daily resolution.
            UniverseSettings.Resolution = Resolution.Daily;

	        SetStartDate(2021, 10, 29);
	        SetEndDate(2021, 11, 1);
            SetCash(100000);

            // Add data for a single security
            _smartInsiderTransaction = AddData<SmartInsiderTransaction>(_symbol);

            // add a custom universe data source
            AddUniverse<SmartInsiderTransactionUniverse>("SmartInsiderTransactionUniverse", Resolution.Daily, UniverseSelectionMethod);
        }

        private IEnumerable<Symbol> UniverseSelectionMethod(IEnumerable<SmartInsiderTransactionUniverse> data)
        {
            _datum = data.FirstOrDefault(datum => datum.Symbol.Value == _symbol.Value);

            if (_datum != null)
            {
                Debug(_datum.ToString());
                _day = _datum.EndTime;
            }

            return Universe.Unchanged;
        }

        public override void OnData(Slice slice)
        {
            // Sanity check for Universe Selection. The Value (Followers) should be the same.
            // and if-condition should not be true
            var data = slice.Get<SmartInsiderTransaction>().Values;

            if (Time.Day == _day.Day)
            {
                _collection.AddRange(data);
            }
        }

        public override void OnEndOfAlgorithm()
        {
            if (_collection?.First().EndTime.Day == _datum?.EndTime.Day && _collection?.Sum(x => x.Amount) != _datum?.Amount)
            {
                var message = $"Data mismatch: Single: ({_collection?.First().EndTime} > {_collection?.Sum(x => x.Amount)}) vs Universe ({_datum?.EndTime} > {_datum?.Amount})";
                throw new Exception(message: message);
            }
        }
    }
}