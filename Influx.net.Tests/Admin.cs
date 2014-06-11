using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Influx.net.Tests
{
    public class Admin : TestBase
    {

        [Fact]
        public void TestDBCreationAndDeletion()
        {
            this.Client.CreateDatabaseAsync("test").Wait();
            var dblist = this.Client.GetDatabaseListAsync().Result;
            Assert.True(dblist.Any(d => d.Name == "test"));
            this.Client.DeleteDatabaseAsync("test").Wait();
            dblist = this.Client.GetDatabaseListAsync().Result;
            Assert.False(dblist.Any(d => d.Name == "test"));
        }

        [Fact]
        public void TestDbExistsException()
        {
            this.Client.CreateDatabaseAsync("test").Wait();
            Assert.DoesNotThrow(() => this.Client.CreateDatabaseAsync("test").Wait());
            Assert.Throws(typeof(InvalidOperationException), () =>
            {
                try
                {
                    this.Client.CreateDatabaseAsync("test", true).Wait();
                }
                catch (AggregateException ex)
                {
                    throw ex.InnerExceptions[0];
                }
            });
           
        }

        [Fact]
        public void TestDbEmptyDeletion()
        {
            Assert.DoesNotThrow(() => this.Client.DeleteDatabaseAsync("test").Wait());
            Assert.Throws(typeof(InvalidOperationException), () =>
            {
                try
                {
                    this.Client.DeleteDatabaseAsync("test", true).Wait();
                }
                catch(AggregateException ex)
                {
                    throw ex.InnerExceptions[0];
                }
            });
        }
    }
}
