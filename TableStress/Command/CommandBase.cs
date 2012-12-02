using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;

namespace TableStress.Command
{
    public abstract class CommandBase
    {
        
        protected static OperationContext GetOperationContext(Action<long> action)
        {
            var context = new OperationContext { };
            var sw = new Stopwatch();

            context.SendingRequest += (sender, args) =>
            {
                var ctx = sender as OperationContext;
                sw.Start();
            };

            context.ResponseReceived += (sender, args) =>
            {
                var ctx = sender as OperationContext;
                sw.Stop();
                action(sw.ElapsedTicks);
            };

            return context;
        }
    }
}
