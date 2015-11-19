using Rachis.Storage;

namespace Rachis.Commands
{
    public class TopologyChangeCommand : Command
    {
        public Topology Requested { get; set; }
        public Topology Previous { get; set; }
        public override string ToString()
        {
            return $"TopologyChange Command. Requested: {Requested} \n, Previous: {Previous}";
        }
    }
}
