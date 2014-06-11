using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Influx.net
{
    public class InfluxHost
    {
        public string Host { get; private set; }
        public int Port { get; private set; }
        public bool IsDisabled { get; set; }

        public InfluxHost(string host = "localhost", int port = 8086)
        {
            this.Host = host;
            this.Port = port;
        }
    }
}
