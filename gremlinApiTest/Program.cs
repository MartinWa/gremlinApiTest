using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Gremlin.Net.CosmosDb;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;


namespace gremlinApiTest
{
    internal class Program
    {
        private static double _totalRuCost;

        private static async Task Main()
        {
            try
            {
                // Using https://github.com/evo-terren/Gremlin.Net.CosmosDb until CosmosDB supports bytecode
                using (var graphClient = new GraphClient(Secrets.Hostname, Secrets.Database, Secrets.Graph, Secrets.AuthKey))
                {
                    var g = graphClient.CreateTraversalSource();

                    // Delete all
                    var deleteQuery = g.V().Drop();
                    await DebugPrintQuery(graphClient, deleteQuery);

                    // Create root node
                    var rootNodeQuery = g.AddV("node").Property("root", true).Property("depth", 0);
                    await DebugPrintQuery(graphClient, rootNodeQuery);

                    // Create nodes
                    for (var depth = 1; depth <= 1; depth++)
                    {

                        var parentsQuery = g.V().Has("depth", depth - 1);
                        var parentResults = await DebugPrintQuery(graphClient, parentsQuery);
                        var id = Convert.ToInt32(Math.Pow(10, depth));
                        foreach (var parentResult in parentResults)
                        {
                            var parentId = parentResult.Id;
                            var childrenQuery = g
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).As("node1")
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).As("node2")
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).As("node3")
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).As("node4")
                                .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", depth).As("node5")
                                .AddE("child").From("node1").To(parentId)
                                .AddE("child").From("node2").To(parentId)
                                .AddE("child").From("node3").To(parentId)
                                .AddE("child").From("node4").To(parentId)
                                .AddE("child").From("node5").To(parentId);
                            await DebugPrintQuery(graphClient, childrenQuery);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            Console.WriteLine($"Total RU cost: {_totalRuCost}");
            Console.ReadLine();
        }

        private static async Task DebugPrintQuery(IGraphClient graphClient, GraphTraversal<Vertex, Edge> query)
        {
            try
            {
                var queryString = query.ToGremlinQuery();
                var response = await graphClient.QueryAsync(query);
                if (response.StatusCode == 200)
                {
                    _totalRuCost += response.TotalRequestCharge;
                    Console.WriteLine($"{queryString}, {response.TotalRequestCharge}");
                    return;
                }
                Console.WriteLine($"Code: {response.StatusCode}");
                Console.WriteLine($"RU Cost: {response.TotalRequestCharge}");
                throw new HttpRequestException(response.StatusCode.ToString());
            }
            catch (ResponseException e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private static async Task<IReadOnlyCollection<Gremlin.Net.CosmosDb.Structure.Vertex>> DebugPrintQuery(IGraphClient graphClient, GraphTraversal<Vertex, Vertex> query)
        {
            try
            {
                var queryString = query.ToGremlinQuery();
                var response = await graphClient.QueryAsync(query);
                if (response.StatusCode == 200)
                {
                    _totalRuCost += response.TotalRequestCharge;
                    Console.WriteLine($"{queryString}, {response.TotalRequestCharge}");
                    return response.Result;
                }
                Console.WriteLine($"Code: {response.StatusCode}");
                Console.WriteLine($"RU Cost: {response.TotalRequestCharge}");
                throw new HttpRequestException(response.StatusCode.ToString());
            }
            catch (ResponseException e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}