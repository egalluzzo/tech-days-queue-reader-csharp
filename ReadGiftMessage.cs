using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using DT = System.Data;
using QC = Microsoft.Data.SqlClient;

namespace Centric.TechDays.AzureFunctions
{
    public class GiftMessage
    {
        public Guid MessageId;
        public Volume GiftBoundingBox;
        public string Recipient;
    }

    public class Volume
    {
        public double Length;
        public double Height;
        public double Width;
    }

    public static class ReadGiftMessage
    {
        [FunctionName("ReadGiftMessage")]
        public static async void Run(
            [ServiceBusTrigger("eric-galluzzo", Connection = "TECHDAYSPRODUCTIONLINES_SERVICEBUS")] string queueItem,
            ILogger log)
        {
            log.LogInformation($"Read message: {queueItem}");
            GiftMessage giftMessage = JsonConvert.DeserializeObject<GiftMessage>(queueItem);

            var update = "INSERT INTO gifts" +
                    " (id, length, width, height, production_line, recipient, creation_date)" +
                    " VALUES (@id, @length, @width, @height, 'eric-galluzzo', @recipient, CURRENT_TIMESTAMP)";
            var parameters = new Dictionary<string, object>()
            {
                { "id", Guid.NewGuid() },
                { "length", giftMessage.GiftBoundingBox.Length },
                { "width", giftMessage.GiftBoundingBox.Width },
                { "height", giftMessage.GiftBoundingBox.Height },
                { "recipient", giftMessage.Recipient }
            };
            
            await ExecuteUpdate(update, parameters);
            log.LogInformation("Wrote message to database");
        }

        private static async Task<int> ExecuteUpdate(string update, Dictionary<string, object> parameters)
        {
            var connectionString = Environment.GetEnvironmentVariable("GIFTS_DB_CONNECTION_STRING");
            using (var connection = new QC.SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                using (var command = new QC.SqlCommand())
                {
                    command.Connection = connection;
                    command.CommandType = DT.CommandType.Text;
                    command.CommandText = update;
                    foreach (var item in parameters)
                    {
                        command.Parameters.AddWithValue(item.Key, item.Value);
                    }
                    return await command.ExecuteNonQueryAsync();
                }
            }
        }
    }
}
