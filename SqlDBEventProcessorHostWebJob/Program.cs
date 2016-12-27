using System;
using Microsoft.Azure.WebJobs;
using System.Configuration;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace SqlDBEventProcessorHostWebJob
{
    class Program
    {
        static void Main()
        {
            var eventHubConnectionString = ConfigurationManager.AppSettings["eventHubConnectionString"];
            var eventHubName = ConfigurationManager.AppSettings["eventHubName"];
            var eventHubConsumerGroup = ConfigurationManager.AppSettings["eventHubConsumerGroup"];
            var storageAccountName = ConfigurationManager.AppSettings["storageAccountName"];
            var storageAccountKey = ConfigurationManager.AppSettings["storageAccountKey"];

            var storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            var eventHubConfig = new EventHubConfiguration();

            var eventProcessorHostName = Guid.NewGuid().ToString();
            var eventProcessorHost = new EventProcessorHost(eventProcessorHostName, eventHubName,
                eventHubConsumerGroup, eventHubConnectionString, storageConnectionString);

            eventHubConfig.AddEventProcessorHost(eventHubName, eventProcessorHost);

            var config = new JobHostConfiguration(storageConnectionString);
            config.UseEventHub(eventHubConfig);
            var host = new JobHost(config);

            Console.WriteLine("Registering EventProcessor...");

            var options = new EventProcessorOptions();
            options.ExceptionReceived += (sender, e) =>
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e.Exception);
                Console.ResetColor();
            };

            eventProcessorHost.RegisterEventProcessorAsync<SqlDbProcessor>(options);

            // The following code ensures that the WebJob will be running continuously
            host.RunAndBlock();

            eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }
    }
}
