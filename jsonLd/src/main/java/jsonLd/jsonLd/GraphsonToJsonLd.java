package jsonLd.jsonLd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class GraphsonToJsonLd {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		/** There typically needs to be only one Cluster instance in an application.*/
        
        Cluster cluster;

        /**
         * Use the Cluster instance to construct different Client instances (e.g. one for sessionless communication
         * and one or more sessions). A sessionless Client should be thread-safe and typically no more than one is
         * needed unless there is some need to divide connection pools across multiple Client instances. In this case
         * there is just a single sessionless Client instance used for the entire App.
         */
        Client client;

        try {
            // Attempt to create the connection objects
            cluster = Cluster.build(new File("src/cluster.yaml")).create();
            client = cluster.connect();
        } catch (FileNotFoundException e) {
            // Handle file errors.
            System.out.println("Couldn't find the configuration file.");
            e.printStackTrace();
            return;
        }
        
        ArrayList<String> query = new ArrayList<String>();
        query.add("g.V('context').valueMap()");
        
        // After connection is successful, execute the nodes creation and then the edges

        String contextResult = queryCosmos(client, query);
        try {
			parseContext(contextResult);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub


		
	}
	
	public static void parseContext(String result) throws ParseException {
		/*JSONParser parser = new JSONParser(); 
		JSONObject json = (JSONObject) parser.parse(result);
		
		System.out.println(json);*/

	}
	
	public static String queryCosmos(Client client, ArrayList<String> queries) {
	       for (String query : queries) {
	            System.out.println("\nSubmitting this Gremlin query: " + query);

	            // Submitting remote query to the server.
	            ResultSet results = client.submit(query);

	            CompletableFuture<List<Result>> completableFutureResults;
	            CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
	            List<Result> resultList;
	            Map<String, Object> statusAttributes;

	            try{
	                completableFutureResults = results.all();
	                completableFutureStatusAttributes = results.statusAttributes();
	                resultList = completableFutureResults.get();
	                statusAttributes = completableFutureStatusAttributes.get();            
	            }
	            catch(ExecutionException | InterruptedException e){
	                e.printStackTrace();
	                break;
	            }
	            catch(Exception e){
	                ResponseException re = (ResponseException) e.getCause();
	                /*
	                // Response status codes. You can catch the 429 status code response and work on retry logic.
	                System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code")); 
	                System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code")); 
	                
	                // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
	                System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));

	                // Total Request Units (RUs) charged for the operation, upon failure.
	                System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));
	                
	                // ActivityId for server-side debugging
	                System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));*/
	                System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
	                
	                throw(e);
	            }

	            for (Result result : resultList) {
	                System.out.println("\nQuery result:");
	                System.out.println(result.getClass()+"\n");
	                	                
	                return(result.toString());
	            }

	            // Status code for successful query. Usually HTTP 200.
	            System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

	            // Total Request Units (RUs) charged for the operation, after a successful run.
	            System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
	            
	        }
		return "";
		
		
	}

}
