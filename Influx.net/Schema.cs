using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Influx.net
{
    public class Schema
    {
        private readonly DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public Dictionary<string,Type> Types { get; private set; }

        public Schema()
        {
            this.Types = new Dictionary<string, Type>();
        }

        public T Parse<T>(string val)
        {
            return (T)this.Parse(typeof (T), val);
        }

        public object Parse(Type type, string val)
        {
            if (type == typeof(string))
                return val;

            if (type == typeof(long))
                return long.Parse(val);
            if (type == typeof(DateTime))
                return unixEpoch.AddMilliseconds(long.Parse(val));
            if (type == typeof(double))
                return double.Parse(val);
            if (type == typeof(float))
                return float.Parse(val);
            if (type == typeof(int))
                return int.Parse(val);
            if (type == typeof(uint))
                return uint.Parse(val);
            if (type == typeof(short))
                return short.Parse(val);
            if (type == typeof(ushort))
                return ushort.Parse(val);
            if (type == typeof(byte))
                return byte.Parse(val);

            throw new InvalidOperationException("Don't know how to convert " + type);
        }

        public object Parse(string column, string val)
        {
            Type type;
            if (!this.Types.TryGetValue(column, out type))
                return val;

            return this.Parse(type, val);
        }
    }
}
