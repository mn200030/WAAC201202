using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Command
{
    public class Insert : CommandBase, ICommand
    {
        async Task<Tuple<long, long>> DoInsert(CloudTable table, long n)
        {
            var partitionKey = string.Format("{0:D12}", n % 100);
            var e = new EntityNk(partitionKey, Guid.NewGuid().ToString() + "-" + n.ToString("D12"));
            var tableOperation = TableOperation.Insert(e);

            var sw = Stopwatch.StartNew();
            var result = await table.ExecuteAsync(tableOperation);

            return new Tuple<long, long>(DateTime.UtcNow.Ticks, sw.ElapsedTicks);
        }

        public IList<Tuple<double, double>> Run(CloudTable table, int numberOfProcess)
        {
            var start = DateTime.UtcNow.Ticks;

            var tasks = Enumerable.Range(0, numberOfProcess).Select(n =>
            {
                var e = DoInsert(table, n);
                return e;

            }).ToArray();

            Task.WaitAll(tasks.ToArray());

            var result = tasks.Select(t => new Tuple<double, double>(
                (double)(t.Result.Item1 - start) / TimeSpan.TicksPerMillisecond,
                (double)t.Result.Item2 / TimeSpan.TicksPerMillisecond)).ToArray();

            return result;
        }


    }
}
