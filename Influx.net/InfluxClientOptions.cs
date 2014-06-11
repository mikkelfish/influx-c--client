using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Influx.net
{
    public class InfluxClientOptions
    {
        internal ConcurrentBag<InfluxHost> Hosts { get; set; }

        public void AddHost(InfluxHost host)
        {
            this.Hosts.Add(host);
        }


        public string UserName { get; set; }
        public string Password { get; set; }
        public int FailoverTimeout { get; set; }
        public int RequestTimeout { get; set; }
        public int MaxRetries { get; set; }
        public string DefaultDatabase { get; set; }

        public InfluxClientOptions(IEnumerable<InfluxHost> hosts = null)
        {
            if (hosts == null)
                hosts = new []{new InfluxHost()};

            this.Hosts = new ConcurrentBag<InfluxHost>(hosts);
            this.FailoverTimeout = 60000;
            this.RequestTimeout = int.MaxValue;
            this.MaxRetries = 2;
        }
    }
}
