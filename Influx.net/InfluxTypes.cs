using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Influx.net
{
    public class InfluxDB
    {
        public string Name { get; set; }
    }

    public class InfluxPoint
    {
        public DateTime Time { get; set; }
        public Dictionary<string, object> Values { get; set; }

        public InfluxPoint()
        {
            this.Time = DateTime.UtcNow;
            this.Values = new Dictionary<string, object>();
        }

    }
}
