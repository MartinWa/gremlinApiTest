using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;

namespace gremlinApiTest
{
    internal class Program
    {
        //private static double _totalRuCost;
        private static async Task Main()
        {
            var server = new GremlinServer("localhost", 8182);
            var client = new GremlinClient(server);
            var remoteConnection = new DriverRemoteConnection(client);
            var graph = new Graph();
            try
            {
                // Given width=5 depth: 2=6v, 3=31v, 4=156, 5=781v, 6=3906v, 7=19531v, 8=97656v, 9=488281v, 10=2441406, 11=12207031v
                //await CreateTestDataCosmosDbAsync(8, 5);
                
                //  CreateTestData(5, 5);
                // var g = graph.Traversal().WithRemote(remoteConnection);
                // var stopwatch = new Stopwatch();
                // stopwatch.Start();

                // // Get Root node
                // var root = g.V().Has("root", true);

                // // Get all children NodeIds
                // var allNodes = root.Repeat(__.Out().SimplePath()).Emit().Values<int>("contentId").ToList();

                // Console.WriteLine($"Found {allNodes.Count} nodes:");
                // Console.WriteLine(string.Join(", ", allNodes.OrderBy(c => c)));



                // // Get path of all nodes
                // //  var allNodes = root.Repeat(__.Out()).Path().Values<int>(new[]{"contentId"}).ToList();
                // stopwatch.Stop();
                // Console.WriteLine($"Operation took {stopwatch.Elapsed}");

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            remoteConnection.Dispose();
            client.Dispose();
        }


        private static async Task CreateTestDataCosmosDbAsync(int depth, int width)
        {
            var gremlinServer = new GremlinServer(Secrets.Hostname, 443, true, $"/dbs/{Secrets.Database}/colls/{Secrets.Graph}", Secrets.AuthKey);
            using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(), GremlinClient.GraphSON2MimeType))
            {
                Console.WriteLine("Deleting all old vertices");
                await SubmitCosmosDbRequest(gremlinClient, "g.V().drop()");
                var totalVertices = (Math.Pow(width, depth) - Math.Pow(width, 0)) / (width - 1); // Geometric series from k=1 to k=depth-1
                Console.WriteLine($"Will create {totalVertices} vertices with a width of {width} and a depth of {depth}");
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                await SubmitCosmosDbRequest(gremlinClient, "g.addV('node').property('contentId', 1).property('root', true).property('depth', 1)");
                for (var d = 2; d <= depth; d++)
                {
                    var parents = await SubmitCosmosDbRequest(gremlinClient, $"g.V().Has('depth', {d - 1})");
                    var id = Convert.ToInt32(Math.Pow(10, d - 1));
                    Console.WriteLine($"Creating depth {d} starting id on {id}");
                    foreach (var parent in parents)
                    {
                        var query = $"g.V('{parent["id"]}').as('parent')";
                        for (var w = 0; w < width; w++)
                        {
                            query += $".addV('node').property('contentId', {id++}).property('Ugam', 3L).property('depth', {d}).as('node{id}')" +
                                      $".addE('child').from('node{id}').to('parent').addE('parent').from('parent').to('node{id}')";
                        }
                        await SubmitCosmosDbRequest(gremlinClient, query);
                        await SubmitCosmosDbRequest(gremlinClient, $"g.V('{parent["id"]}').properties('depth').drop()");
                    }
                }
                stopwatch.Stop();
                Console.WriteLine($"Operation took {stopwatch.Elapsed}");
            }
        }

        private static Task<ResultSet<dynamic>> SubmitCosmosDbRequest(GremlinClient gremlinClient, string query)
        {
            try
            {
                return gremlinClient.SubmitAsync<dynamic>(query);
            }
            catch (ResponseException e)
            {
                Console.WriteLine("\tRequest Error!");

                // Print the Gremlin status code.
                Console.WriteLine($"\tStatusCode: {e.StatusCode}");

                // On error, ResponseException.StatusAttributes will include the common StatusAttributes for successful requests, as well as
                // additional attributes for retry handling and diagnostics.
                // These include:
                //  x-ms-retry-after-ms         : The number of milliseconds to wait to retry the operation after an initial operation was throttled. This will be populated when
                //                              : attribute 'x-ms-status-code' returns 429.
                //  x-ms-activity-id            : Represents a unique identifier for the operation. Commonly used for troubleshooting purposes.
                PrintStatusAttributes(e.StatusAttributes);
                Console.WriteLine($"\t[\"x-ms-retry-after-ms\"] : { GetValueAsString(e.StatusAttributes, "x-ms-retry-after-ms")}");
                Console.WriteLine($"\t[\"x-ms-activity-id\"] : { GetValueAsString(e.StatusAttributes, "x-ms-activity-id")}");
                throw;
            }
        }

        private static void PrintStatusAttributes(IReadOnlyDictionary<string, object> attributes)
        {
            Console.WriteLine($"\tStatusAttributes:");
            Console.WriteLine($"\t[\"x-ms-status-code\"] : { GetValueAsString(attributes, "x-ms-status-code")}");
            Console.WriteLine($"\t[\"x-ms-total-request-charge\"] : { GetValueAsString(attributes, "x-ms-total-request-charge")}");
        }

        public static string GetValueAsString(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            return JsonConvert.SerializeObject(GetValueOrDefault(dictionary, key));
        }

        public static object GetValueOrDefault(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            if (dictionary.ContainsKey(key))
            {
                return dictionary[key];
            }

            return null;
        }


        private static void CreateTestData(int depth, int width)
        {
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