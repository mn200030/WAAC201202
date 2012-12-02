using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStress.Command
{
    interface ICommand
    {
        IEnumerable<CommandResult> Run(CloudTable table, int numberOfProcess, int parallelism);
    }
}
