using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using TableStress.Command;

namespace TableStress
{

    class Program
    {
        static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 12 * 4 * 10;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = false;

            var storageAccount = args.Length > 0 ? CloudStorageAccount.Parse(args[0]) : CloudStorageAccount.DevelopmentStorageAccount;
            var numberOfProcess = args.Length > 1 ? int.Parse(args[1]) : 1000;

            var tableClient = storageAccount.CreateCloudTableClient().GetTableReference("TestTable"+Guid.NewGuid().ToString("N"));

            tableClient.CreateIfNotExists();

            for (var i = 1; i < 64; i *= 2)
            {
                // GC & make stable
                GC.Collect(0, GCCollectionMode.Forced, true);
                Thread.Sleep(1000);

                {
                    EntityNk.DataSize = i;
                    var cmd = new Insert();

                    var result = cmd.Run(tableClient, numberOfProcess);

                    WriteReport(i, result);
                }
            }
        }

        private static void WriteReport(int i, IList<Tuple<double, double>> result)
        {

            Console.WriteLine("# Data Size {0}K", i);

            // 処理時間でソート
            var sorted = result.OrderBy(t => t.Item1).ToArray();

            // 最小、最大、平均、標準偏差、70, 80, 90th %, 成功件数、失敗件数
            var min = sorted.First().Item2;
            var max = sorted.Last().Item2;
            var count = sorted.Count();

            var sum = sorted.Aggregate(new Tuple<double, double>(0, 0), (f, s) =>
                new Tuple<double, double>(f.Item1 + s.Item2, f.Item2 + s.Item2 * s.Item2));

            var avg = sum.Item1 / count;
            var variance = (sum.Item2 / count) - (avg * avg);
            var stddev = Math.Sqrt(variance);

            var p70 = sorted[(int)(count * 70.0 / 100.0)];
            var p80 = sorted[(int)(count * 80.0 / 100.0)];
            var p90 = sorted[(int)(count * 90.0 / 100.0)];

            Console.WriteLine("# min max avg stddev 70th% 80th% 90th% sucsess error");
            Console.WriteLine("## {0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}");

            // 全件 dump
            Console.WriteLine("# EntitySize(KB) ElapsedTime(ms) ExecutionTime(ms)");
            foreach (var t in sorted)
                Console.WriteLine("{0} {1} {2}", i, t.Item1, t.Item2);

            Console.Write("\n\n");
#if HISTOGRAM
            // ms で四捨五入して集計
            Console.WriteLine("# EntitySize(KB) ExecutionTime(ms) NumberOfCounts");
            foreach (var t in result.GroupBy(t => Math.Round(t.Item2)).OrderBy(t => t.Key))
                Console.WriteLine("{0} {1} {2}", i, t.Key, t.Count());

            Console.Write("\n\n");
#endif
        }
    }

    public class EntityNk : TableEntity
    {
        public const string DATA_1K = "01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdef";
        public static string DataCache = DATA_1K;
        private static int dataSize = 1;


        public static int DataSize
        {
            set
            {
                if (value != dataSize)
                {
                    var sb = new StringBuilder(value);
                    for (var i = 0; i < value; i++)
                    {
                        sb.Append(DATA_1K);
                    }
                    DataCache = sb.ToString();
                    dataSize = value;
                }
            }
        }


        public EntityNk(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Data = DataCache; // save memory
        }

        public EntityNk() { }

        public string Data { get; set; }

    }

    public static class CloudTableExtensions
    {
        public static Task<TableResult> ExecuteAsync(this CloudTable cloudTable, TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null, object state = null)
        {
            return Task.Factory.FromAsync<TableOperation, TableRequestOptions, OperationContext, TableResult>(
                cloudTable.BeginExecute, cloudTable.EndExecute, operation, requestOptions, operationContext, state);
        }
    }
}
