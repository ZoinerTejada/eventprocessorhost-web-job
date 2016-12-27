using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Microsoft.ServiceBus.Messaging;
using System.Diagnostics;
using System.Configuration;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

using Microsoft.Azure.Documents.Client;

namespace DocDBEventProcessorHostWebJob
{
    public class DocDBProcessor : IEventProcessor
    {
        static string _docDBEndpointUri;
        static string _docDBKey;
        static string _docDBName = "telemetry";
        static string _docDBCollectionName = "temp";

        private Stopwatch _checkpointStopWatch;

        private async void ProcessEvents(IEnumerable<EventData> events)
        {
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
                        var res = await Client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(_docDBName, _docDBCollectionName), datapoint);
                        Console.WriteLine($"Sent: '{jsonMessage}'  RU cost: {res.RequestCharge}");
                    }
                }
                catch (Exception ex)
                {
                    LogError(ex.Message);
                }
            }
        }

        DocumentClient _client;
        DocumentClient Client
        {
            get
            {
                if (_client == null)
                {

                    _client = new DocumentClient(new Uri(_docDBEndpointUri), _docDBKey);
                }

                return _client;
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

            _docDBEndpointUri = ConfigurationManager.AppSettings["docDBEndpointUri"];
            _docDBKey = ConfigurationManager.AppSettings["docDBKey"];

            Console.WriteLine("CachingProcessor initialized. Partition '{0}', Offset '{1}'", context.Lease.PartitionId, context.Lease.Offset);

            return Task.CompletedTask;
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
