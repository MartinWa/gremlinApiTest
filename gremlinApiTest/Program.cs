using System;
using System.Threading.Tasks;
using Gremlin.Net.CosmosDb;
using Newtonsoft.Json;

namespace gremlinApiTest
{
    internal class Program
    {
        private static async Task Main()
        {
            // Using https://github.com/evo-terren/Gremlin.Net.CosmosDb until CosmosDB supports bytecode


            using (var graphClient = new GraphClient(Secrets.Hostname, Secrets.Database, Secrets.Graph, Secrets.AuthKey))
            {
                var g = graphClient.CreateTraversalSource();
                var query = g
                    .AddV("node").Property("nodeId", 1).Property("Ugam", 3L).As("node1")
                    .AddV("node").Property("nodeId", 2).Property("Ugam", 3L).As("node2")
                    .AddE("child").From("node1").To("node2");

                //var queryString = query.ToGremlinQuery();
                var response = await graphClient.QueryAsync(query);

                Console.WriteLine();
                Console.WriteLine("Response status:");

                Console.WriteLine($"Code: {response.StatusCode}");
                Console.WriteLine($"RU Cost: {response.TotalRequestCharge}");

                Console.WriteLine();
                Console.WriteLine("Response:");
                foreach (var result in response)
                {
                    var json = JsonConvert.SerializeObject(result, Formatting.Indented);

                    Console.WriteLine(json);
                }
            }
        }
    }
}