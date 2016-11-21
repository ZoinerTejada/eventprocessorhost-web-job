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
using System.Data.SqlClient;

namespace SqlDBEventProcessorHostWebJob
{
    public class SqlDbProcessor : IEventProcessor
    {
        static string _sqlConnectionString = "";
        private Stopwatch _checkpointStopWatch;

        private void ProcessEvents(IEnumerable<EventData> events)
        {
            SqlConnection conn = new SqlConnection(_sqlConnectionString);

            string commandText = "INSERT [dbo].[RealtimeReadings] (deviceId, [temp], createDate) " +
                                        "VALUES (@deviceId, @temp, @createDate) ";

            conn.Open();

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

                        // if a transient error closed our connection, create a new one and open it
                        if (conn.State == System.Data.ConnectionState.Closed)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine("{0}: Re-creating closed connection");
                            Console.ResetColor();
                            conn = new SqlConnection(_sqlConnectionString);
                            conn.Open();
                        }

                        List<SqlParameter> parameters = new List<SqlParameter>() {
                            new SqlParameter("@deviceId", datapoint.deviceId),
                            new SqlParameter("@temp", datapoint.temp),
                            new SqlParameter("@createDate", datapoint.createDate)
                        };


                        try
                        {
                            using (SqlCommand cmd = new SqlCommand(commandText, conn))
                            {
                                cmd.CommandType = System.Data.CommandType.Text;
                                cmd.Parameters.AddRange(parameters.ToArray());
                                cmd.ExecuteNonQuery();
                            }
                        }
                        catch (SqlException sqlex)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine(sqlex.Message);
                            Console.ResetColor();

                            System.Threading.Thread.Sleep(200);
                        }
                        catch (Exception cmdex)
                        {
                            Console.ForegroundColor = ConsoleColor.Blue;
                            Console.WriteLine(cmdex.Message);
                            Console.ResetColor();

                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError(ex.Message);
                }
            }

            conn.Close();
            conn.Dispose();
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

            _sqlConnectionString = ConfigurationManager.AppSettings["sqlConnectionString"];

            Console.WriteLine("SqlDBProcessor initialized. Partition '{0}', Offset '{1}'", context.Lease.PartitionId, context.Lease.Offset);

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
