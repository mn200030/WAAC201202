using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using TableStress.Command;

namespace TableStress
{

    class Program
    {
        static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 12 * 4 * 10;
            ServicePointManager.UseNagleAlgorithm = false;

            var connectionString = ConfigurationManager.AppSettings["storageaccount"];
            var storageAccount = CloudStorageAccount.Parse(connectionString); 
            
            var numberOfProcess = args.Length > 0 ? int.Parse(args[0]) : 10;
            var numberOfThread = args.Length > 1 ? int.Parse(args[1]) : 1;
            var tableName = args.Length > 2 ? args[2] : "TestTable" + Guid.NewGuid().ToString("N");

            var tableClient = storageAccount.CreateCloudTableClient();
            tableClient.RetryPolicy = new NoRetry(); // 時間計測したいのでRetryはエラーにする

            var cloudTable = tableClient.GetTableReference(tableName);

            Console.Error.WriteLine(cloudTable.Name);

            cloudTable.CreateIfNotExists();

//            var size = new[] { 512, 1024, 2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024};
            var size = new[] { 1024 };

#if INSERT_BATCH
            var cmd = new InsertBatch();
#else
            var cmd = new Insert();
#endif

            foreach (var s in size) 
            {
                // GC & make stable
                GC.Collect(0, GCCollectionMode.Forced, true);
                Thread.Sleep(1000);
                {
                    var sw = Stopwatch.StartNew();
                    Console.Error.Write("{0} ", s);

                    EntityNk.DataSize = s;

                    var result = cmd.Run(cloudTable, numberOfProcess, numberOfThread);

                    sw.Stop();
                    WriteReport(s, result);

                    Console.Error.WriteLine(". {0} s", sw.ElapsedMilliseconds / 1000.0);
                }
            }
        }

        private static void WriteReport(int i, IEnumerable<CommandResult> result)
        {
#if SINGLE_PARTITION
            Console.WriteLine("# Data Size {0}K SINGLE PARTITION", i);
#else
            Console.WriteLine("# Data Size {0}K MULTI PARTITION", i);
#endif

            // 開始処理時間でソート
            var sorted = result.OrderBy(t => t.Start).ToArray();

            // 全件 dump
            Console.WriteLine("# EntitySize(KB) ElapsedTime(ms) ExecutionTime(ms)");
            foreach (var t in sorted)
                Console.WriteLine("{0} {1} {2}", i, (double)t.Start / TimeSpan.TicksPerMillisecond, (double)(t.Elapsed) / TimeSpan.TicksPerMillisecond);

            Console.Write("\n\n");
        }
    }

    public static class CloudTableExtensions
    {
        public static Task<TableResult> ExecuteAsync(this CloudTable cloudTable, TableOperation operation, TableRequestOptions requestOptions = null, OperationContext operationContext = null, object state = null)
        {
            return Task.Factory.FromAsync<TableOperation, TableRequestOptions, OperationContext, TableResult>(
                cloudTable.BeginExecute, cloudTable.EndExecute, operation, requestOptions, operationContext, state);
        }

        public static Task<IList<TableResult>> ExecuteBatchAsync(this CloudTable cloudTable, TableBatchOperation batch, TableRequestOptions requestOptions = null, OperationContext operationContext = null, object state = null)
        {
            return Task.Factory.FromAsync<TableBatchOperation, TableRequestOptions, OperationContext, IList<TableResult>>(
                cloudTable.BeginExecuteBatch, cloudTable.EndExecuteBatch, batch, requestOptions, operationContext, state);
        }
    }
}
