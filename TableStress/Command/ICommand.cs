using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStress.Command
{
    interface ICommand
    {
        IEnumerable<CommandResult> Run(CloudTable table, int numberOfProcess, int parallelism);
    }
}
