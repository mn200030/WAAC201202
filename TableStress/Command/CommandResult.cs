using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TableStress.Command
{
    public class CommandResult
    {
        public long Start {get;set;}
        public long Elapsed { get; set; }
        public object Data {get;set;}
    }
}
