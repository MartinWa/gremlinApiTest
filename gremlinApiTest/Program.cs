using System;
using System.Threading.Tasks;
using Gremlin.Net.CosmosDb;
using Gremlin.Net.CosmosDb.Structure;
using Newtonsoft.Json;

namespace gremlinApiTest
{
    internal class Program
    {
        private static double TotalRuCost = 0.0;

        private static async Task Main()
        {
            // Using https://github.com/evo-terren/Gremlin.Net.CosmosDb until CosmosDB supports bytecode
            using (var graphClient = new GraphClient(Secrets.Hostname, Secrets.Database, Secrets.Graph, Secrets.AuthKey))
            {
                var g = graphClient.CreateTraversalSource();
                var rootNode = g.AddV("node").Property("root", true).Property("depth", 0);
                var queryString = rootNode.ToGremlinQuery();
                HandleResponse(await graphClient.QueryAsync(rootNode));
                for (var depth = 1; depth < 10; depth++)
                {

                    var parentsQuery = g.V().Has("depth", depth-1);
                    var response = await graphClient.QueryAsync(parentsQuery);
                    var id = Math.Pow(10, depth);
                    foreach (var parentResult in response.Result)
                    {
                        var parentId = parentResult.Id;
                        var query = g
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).AddE("child").From(parentId)
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).AddE("child").From(parentId)
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).AddE("child").From(parentId)
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).AddE("child").From(parentId)
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).AddE("child").From(parentId);
                        queryString = query.ToGremlinQuery();
                        HandleResponse(await graphClient.QueryAsync(query));
                    }
                }
            }
        }

        private static void HandleResponse(GraphResult result)
        {
            if (result.StatusCode == 200)
            {
                TotalRuCost += result.TotalRequestCharge;
                return;
            }
            Console.WriteLine($"Code: {result.StatusCode}");
            Console.WriteLine($"RU Cost: {result.TotalRequestCharge}");
        }
    }
}