using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Costmuch
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var endpoint = args[0];
            var key = args[1];
            var databaseName = args[2];
            var containerName = args[3];
            var partitionKeyValue = args[4];
            var documentsPath = args[5];

            var documents = Directory.GetFiles(documentsPath, "*.json");

            var comsos = new CosmosClient(endpoint, key);
            var container = comsos.GetDatabase(databaseName).GetContainer(containerName);

            foreach (var document in documents)
            {
                await MeasureWriteAsync(container, partitionKeyValue, await File.ReadAllTextAsync(document), document);
            }

            Console.Read();
        }

        private static async Task MeasureWriteAsync(Container container, string partitionKeyValue, string documentJson, string documentName)
        {
            Console.WriteLine($"Measuring document '{documentName}'...");

            var document = JsonConvert.DeserializeObject<JObject>(documentJson);

            var id = document["id"].ToString();

            var current = await container.GetItemQueryIterator<Document>($"SELECT * FROM c WHERE c.id = '{id}'").ReadNextAsync();

            if (current.Count > 0)
            {
                await container.DeleteItemAsync<object>(id, new PartitionKey(partitionKeyValue));
            }

            await WriteDocumentAsync(container, document, "Inserting");
            await WriteDocumentAsync(container, document, "Updating");
        }

        private static async Task WriteDocumentAsync(Container container, object document, string operationType)
        {
            Console.WriteLine($"  {operationType} document: {JsonConvert.SerializeObject(document)}");

            var response = await container.UpsertItemAsync(document);

            Console.WriteLine($"  RequestCharge = {response.RequestCharge}");
        }
    }
}
