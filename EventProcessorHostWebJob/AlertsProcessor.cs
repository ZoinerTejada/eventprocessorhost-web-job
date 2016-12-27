using System;
using System.Text;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.WebJobs.ServiceBus;

namespace EventProcessorHostWebJob
{
    public class AlertsProcessor
    {
        double _maxAlertTemp = 68;
        double _minAlertTemp = 65;

        public void ProcessEvents([EventHubTrigger("%eventhubname%")] EventData[] events)
        {
            foreach (var eventData in events)
            {
                try
                {
                    var eventBytes = eventData.GetBytes();
                    var jsonMessage = Encoding.UTF8.GetString(eventBytes);
                    var evt = JObject.Parse(jsonMessage);

                    JToken temp;
                    double tempReading;

                    if (evt.TryGetValue("temp", out temp))
                    {
                        tempReading = temp.Value<double>();

                        if (tempReading > _maxAlertTemp)
                        {
                            Console.WriteLine("Emitting above bounds: " + tempReading);
                        }
                        else if (tempReading < _minAlertTemp)
                        {
                            Console.WriteLine("Emitting below bounds: " + tempReading);
                        }
                    }


                }
                catch (Exception ex)
                {
                    LogError(ex.Message);
                }
            }
        }

        private static void LogError(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("{0} > Exception {1}", DateTime.Now, message);
            Console.ResetColor();
        }
    }
}
