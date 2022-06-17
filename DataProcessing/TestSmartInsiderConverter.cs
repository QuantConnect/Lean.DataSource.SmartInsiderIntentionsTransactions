using System.Collections.Generic;
using QuantConnect.DataSource;

namespace QuantConnect.DataProcessing
{
    public class TestSmartInsiderConverter : SmartInsiderConverter
    {
        /// <summary>
        /// Creates an instance of the converter for testing only
        /// </summary>
        public TestSmartInsiderConverter()
            : base()
        {
        }

        /// <summary>
        /// Get method of IntentionUniverse dictionary
        /// </summary>
        /// <return>IntentionUniverse dictionary</return>
        public Dictionary<string, Dictionary<string, string>> GetIntentionUniverse()
        {
            return IntentionUniverse;
        }

        /// <summary>
        /// Get method of TransactionUniverse dictionary
        /// </summary>
        /// <return>TransactionUniverse dictionary</return>
        public Dictionary<string, Dictionary<string, string>> GetTransactionUniverse()
        {
            return TransactionUniverse;
        }

        /// <summary>
        /// Test ProcessUniverse method of base class
        /// </summary>
        public void TestProcessUniverse<T>(string tickerInfo, T data)
            where T : SmartInsiderEvent
        {
            base.ProcessUniverse(tickerInfo, data);
        }
    }
}