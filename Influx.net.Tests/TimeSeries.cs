using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Influx.net.Tests
{
    public class TimeSeries : TestBase
    {
        [Fact]
        public void TestTimeSeriesUpload()
        {
            var datetime1 = DateTime.UtcNow;
            Thread.Sleep(100);
            var datetime2 = DateTime.UtcNow;
            Thread.Sleep(100);
            var datetime3 = DateTime.UtcNow;
            var points = new[]
            {
                new InfluxPoint
                {
                    Time = datetime1,
                    Values = new Dictionary<string, object> {{"t1", 5}, {"t2", "test"}, {"t3", 9.2}}
                },
                new InfluxPoint {Time = datetime2, Values = new Dictionary<string, object> {{"t1", 2}, {"t3", 1.2}}},
                new InfluxPoint {Time = datetime3, Values = new Dictionary<string, object> {{"t1", 2}, {"t4", 8L}}},
            };

            this.Client.CreateDatabaseAsync("test").Wait();
            this.Client.WriteTimeSeries("testSeries", points, "test").Wait();
            var schema = new Schema();
            schema.Types.Add("t1", typeof (int));
            schema.Types.Add("t2", typeof (string));
            schema.Types.Add("t3", typeof (double));
            schema.Types.Add("t4", typeof (long));
            Thread.Sleep(5000);

            var timeseries = this.Client.GetTimeSeriesList("test").Result;
            Assert.Contains("testSeries", timeseries);

            var result = this.Client.RunQueryAsync("SELECT * from testSeries order ASC", schema, "test").Result;
            Assert.Equal(datetime1.ToString(), result[0][0].Time.ToString());
            Assert.Equal(5, (int) result[0][0].Values["t1"]);
            Assert.Equal("test", (string) result[0][0].Values["t2"]);
            Assert.Equal(9.2, (double) result[0][0].Values["t3"]);
            Assert.False(result[0][0].Values.ContainsKey("t4"));

            Assert.Equal(datetime2.ToString(), result[0][1].Time.ToString());
            Assert.Equal(2, (int) result[0][1].Values["t1"]);
            Assert.False(result[0][1].Values.ContainsKey("t2"));
            Assert.Equal(1.2, (double) result[0][1].Values["t3"]);
            Assert.False(result[0][1].Values.ContainsKey("t4"));


            Assert.Equal(datetime3.ToString(), result[0][2].Time.ToString());
            Assert.Equal(2, (int) result[0][2].Values["t1"]);
            Assert.False(result[0][2].Values.ContainsKey("t2"));
            Assert.False(result[0][2].Values.ContainsKey("t3"));
            Assert.Equal(8L, (long) result[0][2].Values["t4"]);

            this.Client.DeleteSeriesAsync("testSeries", "test").Wait();
            timeseries = this.Client.GetTimeSeriesList("test").Result;
            Assert.Equal(0, timeseries.Count);
        }
    }
}
