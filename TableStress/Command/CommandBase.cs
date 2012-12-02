using System;
using System.Diagnostics;
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
