using System;
using System.Threading.Tasks;
using Gremlin.Net.CosmosDb;
using Newtonsoft.Json;

namespace gremlinApiTest
{
    internal class Program
    {
        private static void Main()
        {
            // Using https://github.com/evo-terren/Gremlin.Net.CosmosDb until CosmosDB supports bytecode


            using (var graphClient = new GraphClient(Secrets.Hostname, Secrets.Database, Secrets.Graph, Secrets.AuthKey))
            {
                var g = graphClient.CreateTraversalSource();

            //    var query = 
                var v1 = g.AddV("node").Property("nodeId", 1).Property("Ugam", 3L);
                var v2 = g.AddV("node").Property("nodeId", 2).Property("Ugam", 3L);
                var e1 = v1.AddE("child").To(v2);
                var query = e1.ToGremlinQuery();
                var response = graphClient.SubmitAsync(query).Result;


                Console.WriteLine();
                Console.WriteLine("Response status:");

              //  Console.WriteLine($"Code: {response.StatusCode}");
              //  Console.WriteLine($"RU Cost: {response.TotalRequestCharge}");

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




//var graph = new Graph();
//var g = graph.Traversal().WithRemote(new DriverRemoteConnection(new GremlinClient(new GremlinServer("localhost", 8182))));
//var v1 = g.AddV("node").Property("nodeId", 1).Property("Ugam", 3L);
//var v2 = g.AddV("node").Property("nodeId", 2).Property("Ugam", 3L);
//var e1 = v1.AddE("child").To(v2);


//var gremlinServer = new GremlinServer(Secrets.Hostname, Secrets.Port, true, "/dbs/" + Secrets.Database + "/colls/" + Secrets.Collection, Secrets.AuthKey);
//using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(), GremlinClient.GraphSON2MimeType))
//{
//    var query = "g.V()";
//    var resultSet = SubmitRequest(gremlinClient, query).Result;
//    if (resultSet.Count > 0)
//    {
//        Console.WriteLine("\tResult:");
//        foreach (var result in resultSet)
//        {
//            // The vertex results are formed as Dictionaries with a nested dictionary for their properties
//            string output = JsonConvert.SerializeObject(result);
//            Console.WriteLine($"\t{output}");
//        }
//        Console.WriteLine();
//    }

//    // Print the status attributes for the result set.
//    // This includes the following:
//    //  x-ms-status-code            : This is the sub-status code which is specific to Cosmos DB.
//    //  x-ms-total-request-charge   : The total request units charged for processing a request.
//    PrintStatusAttributes(resultSet.StatusAttributes);
//    Console.WriteLine();
//}

//        private static Task<ResultSet<dynamic>> SubmitRequest(GremlinClient gremlinClient, string query)
//        {
//            try
//            {
//                return gremlinClient.SubmitAsync<dynamic>(query);
//            }
//            catch (ResponseException e)
//            {
//                Console.WriteLine("\tRequest Error!");

//                // Print the Gremlin status code.
//                Console.WriteLine($"\tStatusCode: {e.StatusCode}");

//                // On error, ResponseException.StatusAttributes will include the common StatusAttributes for successful requests, as well as
//                // additional attributes for retry handling and diagnostics.
//                // These include:
//                //  x-ms-retry-after-ms         : The number of milliseconds to wait to retry the operation after an initial operation was throttled. This will be populated when
//                //                              : attribute 'x-ms-status-code' returns 429.
//                //  x-ms-activity-id            : Represents a unique identifier for the operation. Commonly used for troubleshooting purposes.
//                PrintStatusAttributes(e.StatusAttributes);
//                Console.WriteLine($"\t[\"x-ms-retry-after-ms\"] : { GetValueAsString(e.StatusAttributes, "x-ms-retry-after-ms")}");
//                Console.WriteLine($"\t[\"x-ms-activity-id\"] : { GetValueAsString(e.StatusAttributes, "x-ms-activity-id")}");

//                throw;
//            }
//        }
//        private static void PrintStatusAttributes(IReadOnlyDictionary<string, object> attributes)
//        {
//            Console.WriteLine("\tStatusAttributes:");
//            Console.WriteLine($"\t[\"x-ms-status-code\"] : { GetValueAsString(attributes, "x-ms-status-code")}");
//            Console.WriteLine($"\t[\"x-ms-total-request-charge\"] : { GetValueAsString(attributes, "x-ms-total-request-charge")}");
//        }

//        public static string GetValueAsString(IReadOnlyDictionary<string, object> dictionary, string key)
//        {
//            return JsonConvert.SerializeObject(GetValueOrDefault(dictionary, key));
//        }
//        public static object GetValueOrDefault(IReadOnlyDictionary<string, object> dictionary, string key)
//        {
//            return dictionary.ContainsKey(key) ? dictionary[key] : null;
//        }
//    }
//}
