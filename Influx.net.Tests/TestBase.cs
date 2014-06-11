using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Influx.net.Tests
{
    public abstract class TestBase : IDisposable
    {
        public InfluxClient Client { get; private set; }

        protected TestBase()
        {
            this.Client = new InfluxClient(new InfluxClientOptions(new []
            {
                new InfluxHost("SERVER_IP", 8086)
            })
            {
                UserName = "USERNAME",
                Password = "PASSWORD"
            
            });

            this.Client.DeleteDatabaseAsync("test").Wait();
        }

        public void Dispose()
        {
            this.Client.DeleteDatabaseAsync("test").Wait();
        }
    }
}
