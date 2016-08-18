using System;
using Microsoft.Azure.WebJobs;
using System.Configuration;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace EventProcessorHostWebJob
{
    class Program
    {
        private static void Main()
        {
            var eventHubConnectionString = ConfigurationManager.AppSettings["eventHubConnectionString"];
            var eventHubName = ConfigurationManager.AppSettings["eventHubName"];
            var storageAccountName = ConfigurationManager.AppSettings["storageAccountName"];
            var storageAccountKey = ConfigurationManager.AppSettings["storageAccountKey"];

            var storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            var eventHubConfig = new EventHubConfiguration();
            eventHubConfig.AddReceiver(eventHubName, eventHubConnectionString);

            var config = new JobHostConfiguration(storageConnectionString);
            config.NameResolver = new EventHubNameResolver();
            config.UseEventHub(eventHubConfig);

            var host = new JobHost(config);
            host.RunAndBlock();
        }
    }

    public class EventHubNameResolver : INameResolver
    {
        public string Resolve(string name)
        {
            return ConfigurationManager.AppSettings[name].ToString();
        }
    }
}
