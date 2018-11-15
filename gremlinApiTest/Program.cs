using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
//using Gremlin.Net.CosmosDb;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;


namespace gremlinApiTest
{
    internal class Program
    {
        //private static double _totalRuCost;
        private static void Main()
        {
            var server = new GremlinServer("localhost", 8182);
            var client = new GremlinClient(server);
            var remoteConnection = new DriverRemoteConnection(client);
            var graph = new Graph();
            try
            {
             //  CreateTestData(5, 5);
                var g = graph.Traversal().WithRemote(remoteConnection);
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                // Get Root node
                var root = g.V().Has("root", true);

                // Get all children NodeIds
              //  var allNodes = root.Repeat(__.Out()).Times(2).Path().Values<int>(new[]{"contentId"}).ToList();
                var allNodes = root.Repeat(__.Out().SimplePath()).Emit().Values<int>("contentId").ToList();

                Console.WriteLine($"Found {allNodes.Count} nodes:");
                Console.WriteLine(string.Join(", ", allNodes.OrderBy(c => c)));
            //    Console.WriteLine(string.Join(", ", allNodes));



                stopwatch.Stop();
                Console.WriteLine($"Operation took {stopwatch.Elapsed}");

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            remoteConnection.Dispose();
            client.Dispose();
        }

        private static void CreateTestData(int depth, int width)
        {
            // Given width=5 depth: 2=6v, 3=31v, 4=156, 5=781v, 6=3906v, 7=19531v, 8=97656v, 9=488281v, 10=2441406, 11=12207031v
            var server = new GremlinServer("localhost", 8182);
            var client = new GremlinClient(server);
            var remoteConnection = new DriverRemoteConnection(client);
            var graph = new Graph();
            var g = graph.Traversal().WithRemote(remoteConnection);

            Console.WriteLine("Deleting all old vertices");
            g.V().Drop().Iterate();
            var totalVertices = (Math.Pow(width, depth) - Math.Pow(width, 0)) / (width - 1); // Geometric series from k=1 to k=depth-1
            Console.WriteLine($"Will create {totalVertices} vertices with a width of {width} and a depth of {depth}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            g.AddV("node")
                .Property("contentId", 1)
                .Property("root", true)
                .Property("depth", 1)
                .Iterate();
            for (var d = 2; d <= depth; d++)
            {
                var parents = g.V().Has("depth", d - 1).ToList();
                var id = Convert.ToInt32(Math.Pow(10, d - 1));
                Console.WriteLine($"Creating depth {d} starting id on {id}");
                foreach (var parent in parents)
                {
                    for (var w = 0; w < width; w++)
                    {
                        g
                            .AddV("node")
                                //.Property("nodeId", id++)
                                .Property("contentId", id++)
                                .Property("Ugam", 3L)
                                .Property("depth", d)
                                .As("node")
                            .AddE("child").From("node").To(parent)
                            .AddE("parent").From(parent).To("node")
                            .Iterate();
                    }
                    g.V().Has(T.Id, parent.Id).Properties<int>("depth").Drop().Iterate();
                }
            }
            stopwatch.Stop();
            Console.WriteLine($"Operation took {stopwatch.Elapsed}");
            remoteConnection.Dispose();
            client.Dispose();
        }
    }
}


//        // // Using https://github.com/evo-terren/Gremlin.Net.CosmosDb until CosmosDB supports bytecode
//         // using (var graphClient = new GraphClient("localhost:8182", Secrets.Database, Secrets.Graph, Secrets.AuthKey))
//         // {
//         //  await CreateTestGraph(graphClient, 1);
//         //    var g = graphClient.CreateTraversalSource();
//         //    var root = await DebugPrintQuery(graphClient, getRootQuery);
//         // }
// private static async Task CreateTestGraph(IGraphClient graphClient, int depth)
// {
//     var g = graphClient.CreateTraversalSource();

//     // Delete all
//     var deleteQuery = g.V().Drop();
//     await DebugPrintQuery(graphClient, deleteQuery);

//     // Create root node
//     var rootNodeQuery = g.AddV("node").Property("root", true).Property("depth", 0);
//     await DebugPrintQuery(graphClient, rootNodeQuery);

//     // Create nodes
//     for (var d = 1; d <= depth; d++)
//     {
//         var parentsQuery = g.V().Has("depth", d - 1);
//         var parentResults = await DebugPrintQuery(graphClient, parentsQuery);
//         var id = Convert.ToInt32(Math.Pow(10, d));
//         foreach (var parentResult in parentResults)
//         {
//             var parentId = parentResult.Id;
//             var childrenQuery = g
//                 .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", d).As("node1")
//                 .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", d).As("node2")
//                 .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", d).As("node3")
//                 .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", d).As("node4")
//                 .AddV("node").Property("nodeId", id++).Property("Ugam", 3L).Property("depth", d).As("node5")
//                 .AddE("child").From("node1").To(parentId)
//                 .AddE("child").From("node2").To(parentId)
//                 .AddE("child").From("node3").To(parentId)
//                 .AddE("child").From("node4").To(parentId)
//                 .AddE("child").From("node5").To(parentId);
//             await DebugPrintQuery(graphClient, childrenQuery);
//         }
//     }
// }

// private static async Task DebugPrintQuery(IGraphClient graphClient, GraphTraversal<Vertex, Edge> query)
// {
//     try
//     {
//         var queryString = query.ToGremlinQuery();
//         var response = await graphClient.QueryAsync(query);
//         if (response.StatusCode == 200)
//         {
//             _totalRuCost += response.TotalRequestCharge;
//             Console.WriteLine($"{queryString}, {response.TotalRequestCharge}");
//             return;
//         }
//         Console.WriteLine($"Code: {response.StatusCode}");
//         Console.WriteLine($"RU Cost: {response.TotalRequestCharge}");
//         throw new HttpRequestException(response.StatusCode.ToString());
//     }
//     catch (ResponseException e)
//     {
//         Console.WriteLine(e);
//         throw;
//     }
// }

// private static async Task<IReadOnlyCollection<Gremlin.Net.CosmosDb.Structure.Vertex>> DebugPrintQuery(IGraphClient graphClient, GraphTraversal<Vertex, Vertex> query)
// {
//     try
//     {
//         var queryString = query.ToGremlinQuery();
//         var response = await graphClient.QueryAsync(query);
//         if (response.StatusCode == 200)
//         {
//             _totalRuCost += response.TotalRequestCharge;
//             Console.WriteLine($"{queryString}, {response.TotalRequestCharge}");
//             return response.Result;
//         }
//         Console.WriteLine($"Code: {response.StatusCode}");
//         Console.WriteLine($"RU Cost: {response.TotalRequestCharge}");
//         throw new HttpRequestException(response.StatusCode.ToString());
//     }
//     catch (ResponseException e)
//     {
//         Console.WriteLine(e);
//         throw;
//     }
// }