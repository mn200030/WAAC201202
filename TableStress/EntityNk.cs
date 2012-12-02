using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStress
{

    public class EntityNk : TableEntity
    {
        const int MAX_PROPERTY = 8; //  
        private static List<byte[]> dataCache;
        private static int dataSize = 1;

        static EntityNk()
        {
            Clear();
        }

        public EntityNk(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Data0 = dataCache[0];
            this.Data1 = dataCache[1];
            this.Data2 = dataCache[2];
            this.Data3 = dataCache[3];
            this.Data4 = dataCache[4];
            this.Data5 = dataCache[5];
            this.Data6 = dataCache[6];
            this.Data7 = dataCache[7];
        }

        public EntityNk() { }

        public byte[] Data0 { get; set; }
        public byte[] Data1 { get; set; }
        public byte[] Data2 { get; set; }
        public byte[] Data3 { get; set; }
        public byte[] Data4 { get; set; }
        public byte[] Data5 { get; set; }
        public byte[] Data6 { get; set; }
        public byte[] Data7 { get; set; }

        public static int DataSize
        {
            set
            {
                if (value != dataSize)
                {
                    Clear();

                    dataSize = value;
                    var x = dataSize / MAX_PROPERTY;
                    var y = dataSize % MAX_PROPERTY;

                    for (var i = 0; i < dataCache.Count(); i++)
                    {
                        dataCache[i] = GetRandomByte(x);
                    }

                    if (y != 0)
                        dataCache[x] = GetRandomByte(y);
                }
            }
        }


        #region Implimentation

        private static void Clear()
        {
            dataCache = new List<byte[]>(MAX_PROPERTY);
            for (var i = 0; i < MAX_PROPERTY; i++)
            {
                dataCache.Add(null);
            }

        }

        private static char[] PrintableAsciiChars()
        {
            var result = new List<char>();
            for (var i = 0x21; i < 0x7d; i++)
            {
                result.Add((char)i);
            }
            return result.ToArray();
        }

        private static string GetRandomString(int length)
        {
            var baseChar = PrintableAsciiChars();

            var randomChars = GetRandomByte(length).Select(c => baseChar[c % baseChar.Length]).ToArray();

            return new string(randomChars);
        }

        private static byte[] GetRandomByte(int length)
        {
            using (var rng = new RNGCryptoServiceProvider())
            {
                var randomByte = new byte[length];
                rng.GetBytes(randomByte);

                return randomByte;
            }
        }
        #endregion

    }
}
