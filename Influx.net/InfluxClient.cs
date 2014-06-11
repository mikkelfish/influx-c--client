using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Text;

namespace Influx.net
{
    public class InfluxClient
    {
        public InfluxClientOptions Options { get; private set; }

        public InfluxClient(InfluxClientOptions options = null)
        {
            ServiceStack.Text.JsConfig.EmitLowercaseUnderscoreNames = true;
            if (options == null)
                options = new InfluxClientOptions();
            this.Options = options;
        }

        private class influxApiRequest
        {
            public int FailCount { get; private set; }
            public InfluxClientOptions Options { get; private set; }

            public influxApiRequest(InfluxClientOptions options)
            {
                this.Options = options;
            }

            public async Task<T> Execute<T>(string method, string url, string body, string queryAddition = null) where T:class
            {
                InfluxHost host;
                if (!this.Options.Hosts.TryPeek(out host))
                    throw new InvalidOperationException("No hosts are active");


                var c = (HttpWebRequest)HttpWebRequest.Create(
                    new UriBuilder
                    {
                        Host = host.Host,
                        Port = host.Port,
                        Path = "/" + url,
                        Query = String.Format("u={0}&p={1}{2}", this.Options.UserName, this.Options.Password, queryAddition)
                    }.Uri);

                c.Method = method;
                c.Timeout = this.Options.RequestTimeout;
                c.ContentType = "application/json";
                c.Accept = "application/json";
                
                if (body != null)
                {
                    var requestStream = c.GetRequestStream();
                    var bodyBytes = Encoding.UTF8.GetBytes(body);
                    requestStream.Write(bodyBytes, 0, bodyBytes.Length);
                    requestStream.Flush();
                }

                var fullResponse = new StringBuilder();

                var response = await c.GetResponseAsync();
                var buffer = new byte[2056];
                var count = 0;
                while ( (count = await response.GetResponseStream().ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    var thispart = Encoding.UTF8.GetString(buffer, 0, count);
                    fullResponse.Append(thispart);
                }

                if (typeof (T) == typeof (string))
                    return fullResponse.ToString() as T;

               
                return JsonSerializer.DeserializeFromString<T>(fullResponse.ToString());
            }
        }

        public async Task<IEnumerable<InfluxDB>> GetDatabaseListAsync()
        {
            var request = new influxApiRequest(this.Options);
            return JsonSerializer.DeserializeFromString<InfluxDB[]>(await request.Execute<string>("GET", "db", null));
        }

        public async Task CreateDatabaseAsync(string databaseName, bool throwOnExisting = false)
        {
            if (databaseName == null)
                throw new ArgumentNullException("databaseName");

            var request = new influxApiRequest(this.Options);

            try
            {
                await request.Execute<string>("POST", "db", JsonSerializer.SerializeToString(new InfluxDB {Name = databaseName}));
            }
            catch (WebException ex)
            {
                if (((HttpWebResponse) ex.Response).StatusCode == HttpStatusCode.Conflict && throwOnExisting)
                        throw new InvalidOperationException("The database already exists");
            }
        }

        public async Task DeleteDatabaseAsync(string databaseName, bool throwOnMissing = false)
        {
            if (databaseName == null)
                throw new ArgumentNullException("databaseName");

            var request = new influxApiRequest(this.Options);
            try
            {
                await request.Execute<string>("DELETE", String.Format("db/{0}", databaseName), null);
            }
            catch (WebException ex)
            {
                if (((HttpWebResponse)ex.Response).StatusCode == HttpStatusCode.BadRequest && throwOnMissing)
                    throw new InvalidOperationException("The database does not exist");
            }
        }

        private readonly DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        internal class apiSeries
        {
            public string Name { get; set; }
            public List<string> Columns { get; set; }
            public List<List<object>> Points { get; set; }
        }

        public async Task WriteTimeSeries(string seriesName, IEnumerable<InfluxPoint> points, string database = null)
        {
            if (seriesName == null)
                throw new ArgumentNullException("seriesName");

            if (database == null)
                database = this.Options.DefaultDatabase;
            if(database == null)
                throw new ArgumentNullException("database", "Database is null and default is not set in options");

            var request = new influxApiRequest(this.Options);
            var cols = new List<string>();

            cols.Add("time");
            var columnLookup = new Dictionary<string, int>();
            foreach (var point in points)
            {
                foreach (var val in point.Values)
                {
                    if (!columnLookup.ContainsKey(val.Key))
                    {
                        columnLookup.Add(val.Key, cols.Count);
                        cols.Add(val.Key);
                    }
                }
            }

            var vals = new List<List<object>>();
            foreach (var point in points)
            {
                var row = new List<object>();
                row.Add((long)(point.Time - unixEpoch).TotalMilliseconds);
                for (int i = 1; i < cols.Count; i++)
                {
                    object val;
                    if (!point.Values.TryGetValue(cols[i], out val))
                        row.Add(null);
                    else row.Add(val);
                }
                vals.Add(row);
            }

            var data = new[]{ new apiSeries
            {
                Name = seriesName,
                Columns = cols,
                Points = vals
            }};

            await request.Execute<string>("POST", String.Format("db/{0}/series", database), JsonSerializer.SerializeToString(data), "&time_precision=m"); //, 
        }

        public async Task WritePointAsync(string seriesName, InfluxPoint point, string database = null)
        {
            await this.WriteTimeSeries(seriesName, new[] {point}, database);
        }

        public async Task DeleteSeriesAsync(string seriesName, string database = null, bool throwOnMissing = false)
        {
            if (seriesName == null)
                throw new ArgumentNullException("seriesName");

            if (database == null)
                database = this.Options.DefaultDatabase;
            if (database == null)
                throw new ArgumentNullException("database", "Database is null and default is not set in options");

            var request = new influxApiRequest(this.Options);
            try
            {
                await request.Execute<string>("DELETE", String.Format("db/{0}/series/{1}", database, seriesName), null);
            }
            catch (WebException ex)
            {
                if (((HttpWebResponse)ex.Response).StatusCode == HttpStatusCode.BadRequest && throwOnMissing)
                    throw new InvalidOperationException("The time series or database does not exist");
            }
        }

        public async Task<List<List<InfluxPoint>>> RunQueryAsync(string query, Schema schema, string database = null)
        {
            if (database == null)
                database = this.Options.DefaultDatabase;
            if (database == null)
                throw new ArgumentNullException("database", "Database is null and default is not set in options");

            var request = new influxApiRequest(this.Options);
            var result = await request.Execute<string>("GET", String.Format("db/{0}/series", database), null, String.Format("&q={0}", query));
            var seriesAll = JsonSerializer.DeserializeFromString<apiSeries[]>(result);

            var toRet = new List<List<InfluxPoint>>();
            foreach (var series in seriesAll)
            {
                var thisSeries = new List<InfluxPoint>();
                foreach (var row in series.Points)
                {          
                    var newPoint = new InfluxPoint
                    {
                        Time = schema.Parse<DateTime>((string)row[0])
                    };

                    for (int i = 2; i < row.Count; i++)
                    {
                        if (row[i] != null)
                            newPoint.Values.Add(series.Columns[i], schema.Parse(series.Columns[i], (string)row[i]));
                    }
                    thisSeries.Add(newPoint);
                }
                toRet.Add(thisSeries);
            }
            return toRet;
        }

        public async Task<List<string>> GetTimeSeriesList(string database = null)
        {
            if (database == null)
                database = this.Options.DefaultDatabase;
            if (database == null)
                throw new ArgumentNullException("database", "Database is null and default is not set in options");

            var request = new influxApiRequest(this.Options);
            var result = await request.Execute<string>("GET", String.Format("db/{0}/series", database), null, String.Format("&q={0}", "list series"));
            var seriesAll = JsonSerializer.DeserializeFromString<apiSeries[]>(result);
            return seriesAll.Select(s => s.Name).ToList();
        }

    }
}
