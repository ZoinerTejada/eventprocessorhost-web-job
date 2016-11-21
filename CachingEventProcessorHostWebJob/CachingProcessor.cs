using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.ServiceBus.Messaging;
using System.Diagnostics;
using System.Configuration;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Azure.WebJobs;
using Newtonsoft.Json;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Newtonsoft;

namespace CachingEventProcessorHostWebJob
{
    public class CachingProcessor : IEventProcessor
    {
        static string _redisConnectionString = "";
        private Stopwatch _checkpointStopWatch;

private void ProcessEvents(IEnumerable<EventData> events)
{
    IDatabase cache = Connection.GetDatabase();

    foreach (var eventData in events)
    {
        try
        {
            var eventBytes = eventData.GetBytes();
            var jsonMessage = Encoding.UTF8.GetString(eventBytes);
            var evt = JObject.Parse(jsonMessage);

            JToken temp;
            TempDataPoint datapoint;

            if (evt.TryGetValue("temp", out temp))
            {
            datapoint = JsonConvert.DeserializeObject<TempDataPoint>(jsonMessage);
            cache.StringSet("device:temp:latest:" + datapoint.deviceId, jsonMessage);
            cache.KeyExpire("device:temp:latest:" + datapoint.deviceId, TimeSpan.FromMinutes(120));
            cache.HyperLogLogAdd("device:temp:reportcounts:" + datapoint.deviceId, jsonMessage);
            }
        }
        catch (Exception ex)
        {
            LogError(ex.Message);
        }
    }
}


private void PrintStatusSnapshot()
{

    try
    {
        var redisClient = new StackExchangeRedisCacheClient(Connection, new NewtonsoftSerializer());
        var keys = redisClient.SearchKeys("device:temp:latest:" + "*");
        var dict = redisClient.GetAll<TempDataPoint>(keys);

        Console.WriteLine("=======================");
        Console.WriteLine("Latest Temp Readings: ");
        foreach (var key in dict.Keys)
        {
            Console.WriteLine($"\t{key}:\t{dict[key].temp}");
        }

        IDatabase cache = Connection.GetDatabase();
        var countKeys = redisClient.SearchKeys("device:temp:reportcounts:" + "*");

        Console.WriteLine("=======================");
        Console.WriteLine("Latest Report Counts: ");
        foreach (var key in countKeys)
        {
            Console.WriteLine($"\t{key}:\t{cache.HyperLogLogLength(key)}");
        }
    }
    catch (Exception ex)
    {
        LogError("PrintStatusSnapshot: " + ex.Message);
    }

}
        

        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            return ConnectionMultiplexer.Connect(_redisConnectionString);
        });

        public static ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        private static void LogError(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("{0} > Exception {1}", DateTime.Now, message);
            Console.ResetColor();
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            
            this._checkpointStopWatch = new Stopwatch();
            this._checkpointStopWatch.Start();

            _redisConnectionString = ConfigurationManager.AppSettings["redisConnectionString"];

            Console.WriteLine("CachingProcessor initialized. Partition '{0}', Offset '{1}'", context.Lease.PartitionId, context.Lease.Offset);

            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            try
            {
                ProcessEvents(messages);

                if (_checkpointStopWatch.Elapsed > TimeSpan.FromSeconds(3))
                {
                    await context.CheckpointAsync();
                    Console.WriteLine("Checkpoint partition {0} progress.", context.Lease.PartitionId);

                    if (context.Lease.PartitionId == "0")
                    {
                        PrintStatusSnapshot();
                    }
                    

                    _checkpointStopWatch.Restart();
                }
            }
            catch (Exception ex)
            {
                LogError("ProcessEventsAsync:" + ex.Message);
            }
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            try
            {
                Console.WriteLine("Processor Shutting Down... Partition '{0}', Reason '{1}'.", context.Lease.PartitionId, reason);

                if (reason == CloseReason.Shutdown)
                {
                    Console.WriteLine("Performing final checkpoint...");
                    await context.CheckpointAsync();
                }
            }
            catch (Exception ex)
            {
                LogError("CloseAsync: " + ex.Message);
            }
        }

        private class TempDataPoint
        {
            public double temp;
            public DateTime createDate;
            public string deviceId;
        }
    }

}
