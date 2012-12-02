using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStress.Command
{
    public class InsertBatch : CommandBase, ICommand
    {
        private static EntityNk[] EntityGenerator(long n)
        {
#if SINGLE_PARTITION
            var partitionKey = "000000000000";
#else
            var partitionKey =  string.Format("{0:D12}", n % 10);
#endif
            var result = new EntityNk[100];

            for (var i = 0; i < 100; i++)
            {
                var e = new EntityNk(partitionKey, Guid.NewGuid().ToString() + "-" + n.ToString("D12"));
                result[i] = e;
            }
            return result;
        }

        async Task<CommandResult> DoInsert(CloudTable table, long n, Func<long, EntityNk[]> entityFactory)
        {
            
            var batchOperation = new TableBatchOperation();

            foreach (var e in entityFactory(n))
            {
                batchOperation.Insert(e);
            }

            var cresult = new CommandResult { Start = DateTime.UtcNow.Ticks };
            var cbt = 0L;
            var context = GetOperationContext((t) => cbt = t);
            try
            {
                var results = await table.ExecuteBatchAsync(batchOperation, operationContext: context);
                cresult.Elapsed = cbt;
            }
            catch (Exception ex)
            {
                cresult.Elapsed = -1;
                Console.Error.WriteLine("Error DoInsert {0} {1}", n, ex.ToString());
            }
            return cresult;
        }

        private IList<CommandResult> Run(CloudTable table, Tuple<int, int> range)
        {
            Console.Error.WriteLine("start {0}-{1}", range.Item1, range.Item2);
            var tasks = Enumerable.Range(range.Item1, range.Item2 - range.Item1).Select(n =>
            {
                var e = DoInsert(table, n, EntityGenerator);
                return e;

            }).ToArray();

            Task.WaitAll(tasks);

            var result = tasks.Select(t => t.Result).ToArray();

            return result;
        }

        public IEnumerable<CommandResult> Run(CloudTable table, int numberOfProcess, int parallelism = 0)
        {
            if (parallelism == 0) parallelism = System.Environment.ProcessorCount * 3;

            if(parallelism == 1)
                return Run(table, new Tuple<int, int>(0, numberOfProcess));

            var sizeOfWorkload = numberOfProcess / parallelism + (numberOfProcess % parallelism == 0 ? 0 : 1);

            var chunker = Partitioner.Create(0, numberOfProcess, sizeOfWorkload);

            var results = new ConcurrentQueue<IList<CommandResult>>();

            // Loop over the workload partitions in parallel.
            Parallel.ForEach(chunker,
                new ParallelOptions { MaxDegreeOfParallelism = parallelism },
                (range) => results.Enqueue(Run(table, range))
                );

            var ret = new List<CommandResult>();

            foreach (var l in results) ret.AddRange(l);

            return ret;
#if BUG
            return results.Aggregate<IEnumerable<CommandResult>>((f, s) =>
            {
                return f.Concat(s).AsEnumerable();
            });
#endif
        }
    }
}
