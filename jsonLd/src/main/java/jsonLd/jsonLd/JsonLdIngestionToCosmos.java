package jsonLd.jsonLd;

import java.io.FileNotFoundException;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class JsonLdIngestionToCosmos {

	static ArrayList<String> edges = new ArrayList<String>();
	static ArrayList<String> nodes = new ArrayList<String>();
	static ArrayList<String> context = new ArrayList<String>();
	static ArrayList<String> contextEdges = new ArrayList<String>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();
         
        /**This logic is to read a json ld file from a path
         * Change the path to point to your file*/
        
        try (FileReader reader = new FileReader("C:\\jsonld.json"))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            
            //Parse file to JSON Object
            JSONObject jsonObject = (JSONObject) obj;

       /**
        * If we were successful reading the file, now we can create the gremlin queries to insert nodes and edges into Cosmos Db
        * 
        *      1. createContextNode: recieves the context inside the json ld object
        *      		First, we create the query to insert a context node and store the query in context Array List.
        *      		This method also adds into contextEdges the list of properties that are edges
        *      2. parseJsonLdGraph: receives the json ld file
        *      		Create the queries to insert the nodes and store the queries in nodes Array List
        *      3. parseJsonLdEdges: receives the json ld file
        *      		Create the queries to insert the edges and store the queries in edges Array List
        */
       
            //Store the gremlin queries to create a context node with the json ld info
            createContextNode((JSONObject) jsonObject.get("@context"));
            //Store the gremlin queries to create nodes in an Array List by calling the parse JsonLdObject method
            parseJsonLdGraph(jsonObject);
            //Store the gremlin queries to create edges in an Array List by calling the parse JsonLdObject method
            parseJsonLdEdges(jsonObject);

 
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        
        
        /**
         * Create the Cosmos Db connection.
         * The cluster will be created through a yaml config file. Change the parameters in the file for the connection.
         * There typically needs to be only one Cluster instance in an application.
         */
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

        /**
         * After connection is successful, execute the nodes creation and then the edges
         * We write the nodes first, and the edges at the end.
         */
        
        
        writeIntoCosmos(client, nodes);
        writeIntoCosmos(client, context);
        writeIntoCosmos(client, edges);
        

        System.out.println("Successfully executed insertion");

        // Properly close all opened clients and the cluster
        cluster.close();

        System.exit(0);
    }
        
	
	public static void writeIntoCosmos(Client client, ArrayList<String> queries) {
	    /**
	     * This method iterates through a string array with queries and prints the result of the execution.
	     * */   
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
	                System.out.println(result.toString());
	            }

	            // Status code for successful query. Usually HTTP 200.
	            System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

	            // Total Request Units (RUs) charged for the operation, after a successful run.
	            System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
	        }
		
		
	}
		
	public static String createVertex(JSONObject jsonld) {
		/**
		 * This method receives a json object. 
		 * 1. First, we assiggn the node label with the @type property in the json object
		 * 2. We assign the node property 'jsonldid' with the @id property in the json object
		 * 3. We iterate through the rest of json properties to assign it to the current node
		 * */
		
		//Assign the node label with the @type property in the json object
		String label = "";
		if (jsonld.get("@type").getClass() == String.class) {
			label = (String) jsonld.get("@type");	
		} 
		else if(jsonld.get("@type").getClass() == JSONArray.class) { 	
			label = (String)((JSONArray) jsonld.get("@type")).get(0);		    
		} 
		else {
			label = "undifined";
		}

		//Assign the node property 'jsonldid' with the @id property in the json object
		String id = (String) jsonld.get("@id"); 
		String insertion = "g.addV('"+label+"').property('jsonldid', '"+id+"').property('partitionkey', '"+id+"')";
		
		//Iterate through the rest of json properties to assign it to the current node
    	@SuppressWarnings("unchecked")
		List<String>  keys = new ArrayList<String>(jsonld.keySet());
    	for (int i = 0; i < keys.size(); i++) {    		
    		if( !keys.get(i).contains("@type") && !keys.get(i).contains("@id")  ) {    			
    			if (jsonld.get(keys.get(i)).getClass() == JSONArray.class) {    				
    				for (int j = 0; j< ((JSONArray) jsonld.get(keys.get(i))).size(); j++) {    					
    					insertion += (".property(list, '"+keys.get(i)+"', '"+ ((JSONArray) jsonld.get(keys.get(i))).get(j).toString().replace("'", "") +"')"); 					
    				}
    			}
    			else if(jsonld.get(keys.get(i)).getClass() == JSONObject.class) {
    				for (int j = 0; j< ((JSONObject) jsonld.get(keys.get(i))).size(); j++) {   					
    					String value = (String) ((JSONObject) jsonld.get(keys.get(i))).get(j);   				   					
    					insertion += (".property(list, '"+keys.get(i)+"', '"+  value +"')"); 					
    				}
    			}
    			else {    				
    				insertion += (".property('"+keys.get(i)+"' , '"+  jsonld.get(keys.get(i)).toString().replace("'", "") +"')"); 
    			}   			 		   			 
    		}    		    		
    	}	
    //	Add the query string to nodes Array List
    	nodes.add(insertion);
    	return insertion;
	}
	
	public static String createEdges(JSONObject jsonld) {
		/**
		 * This method receives a json object. 
		 * 1. First, we assign the edge 'jsonldid' property with @id
		 * 2. We iterate through the properties inside the json object
		 * 3. We check if the property is inside the list of edges contained in the context
		 * 4. If it is inside the list, we check to see if it is a valid url
		 * 5. If it is compliant with both conditions, we assign the edge or edges. Depending on how many came inside a json object. 
		 * 		(one if it's a string, or many if those are contained inside an array or a nested json)
		 * */	

		//Assiggn the edge 'jsonldid' property with @id
    	String id = (String) jsonld.get("@id");
    	
    	String insertion = "";
    	
    	
    	@SuppressWarnings("unchecked")
		List<String>  keys = new ArrayList<String>(jsonld.keySet());
    	//Iterate through the properties inside the json object
    	for (int i = 0; i < keys.size(); i++) {
    		//Check if the property is inside the list of edges contained in the context   
        		if( contextEdges.contains(keys.get(i))){
        			if (jsonld.get(keys.get(i)).getClass() == JSONArray.class) {        				
        				for (int j = 0; j< ((JSONArray) jsonld.get(keys.get(i))).size(); j++) {
        					//if the value contains an url, it has an edge       					
            				String value = (String) ((JSONArray) jsonld.get(keys.get(i))).get(j);	
                			if(isValid(value)) {        			
                		    	edges.add("g.V().has('jsonldid','"+id+"').addE('"+keys.get(i)+"').to(g.V().has('jsonldid','"+value+"')).property('partitionkey', '"+id+"')");
                				insertion += "g.V().has('jsonldid','"+id+"').addE('"+keys.get(i)+"').to(g.V().has('jsonldid','"+value+"')).property('partitionkey', '"+id+"')";
                			}
                			            							
        				}
        			}
        			else if(jsonld.get(keys.get(i)).getClass() == JSONObject.class) {
        				for (int j = 0; j< ((JSONObject) jsonld.get(keys.get(i))).size(); j++) {
        					//if the value contains an url, it has an edge
            				String value = (String) ((JSONObject) jsonld.get(keys.get(i))).get(j);            				
                			if(isValid(value)) {
                				edges.add("g.V().has('jsonldid', '"+id+"').addE('"+keys.get(i)+"').to(g.V().has('jsonldid', '"+value+"')).property('partitionkey', '"+id+"')");
                				insertion += "g.V().has('jsonldid', '"+id+"').addE('"+keys.get(i)+"').to(g.V().has(jsonldid', '"+value+"')).property('partitionkey', '"+id+"')";
                			}                			            							
        				}        				
        			}
        			else {
        				//if the value contains an url, it has an edge
        				String value = (String) jsonld.get(keys.get(i));	            			
            			if(isValid(value)) {
            				edges.add("g.V().has('jsonldid', '"+id+"').addE('"+keys.get(i)+"').to(g.V().has('jsonldid', '"+value+"')).property('partitionkey', '"+id+"')");
            		    	insertion += "g.V().has('jsonldid', '"+id+"').addE('"+keys.get(i)+"').to(g.V().has('jsonldid', '"+value+"')).property('partitionkey', '"+id+"')";
            			}
            					 
        			}
       			 
        		}
		
    		
    	}
    	
    	return insertion;
		
	}
	
	public static void createContextNode(JSONObject jsonld) {		
		/**
		 * This method adds the insert gremlin query to the context Array List 
		 * This method is needed to store a context node inside Cosmos Db.
		 * 1. First, we assign the vertex id and partition key as context so we always keep it the same. (It can be assigned from a variable 
		 *    if many context are going to be stored in the same graph)
		 * 2. Iterate through the json objects and add the properties to the node.
		 * */
		
		//Assign the vertex id and partition key
		String insertion = "g.addV('context').property('id', 'context').property('partitionkey', 'context')";
		
        @SuppressWarnings("unchecked")
		List<String>  keys = new ArrayList<String>(jsonld.keySet());
        //Iterate through the json objects and add the properties to the node
        for (int i = 0; i < keys.size(); i++) {

        	if(jsonld.get(keys.get(i)).getClass() == JSONObject.class) {
				
				contextEdges.add(keys.get(i));
        		
        		 @SuppressWarnings("unchecked")
         		List<String>  keysContext = new ArrayList<String>(((JSONObject) jsonld.get(keys.get(i))).keySet());
        		for (int j = 0; j< keysContext.size(); j++) {
					
					String value = (String) ((JSONObject) jsonld.get(keys.get(i))).get(keysContext.get(j));
										
					insertion += (".property(list, '"+keys.get(i)+"', '"+  value +"')"); 	
					
					
				}
        	}
        	else if(jsonld.get(keys.get(i)).getClass() == String.class) {
        		insertion += (".property('"+keys.get(i)+"' , '"+  jsonld.get(keys.get(i)).toString().replace("'", "") +"')"); 
        	}
        }   		    
    	    	context.add(insertion);   	    		
	}
	
    public static void parseJsonLdGraph(JSONObject jsonObject)
    {    
    	/**
    	 * This method iterates through the json objects inside the json file and calls the createVertex method which assigns the query to an Array List
    	 * */
    	//Extract the array inside @graph json ld
        JSONArray graphlist = (JSONArray) jsonObject.get("@graph");              
        String gremlinQueries[] = new String[graphlist.size()];        
        //Iterate over graph array
        for (int i = 0; i < graphlist.size(); i++) {        	
        	gremlinQueries[i] = createVertex((JSONObject)graphlist.get(i));
        	//System.out.println(gremlinQueries[i]);
        }            	     
    }
    
    public static void parseJsonLdEdges(JSONObject jsonObject)
    {    
    	/**
    	 * This method iterates through the json objects inside the json file and calls the createEdges method which adds the query to an Array List
    	 * */
    	//Extract the array inside @context json ld
        JSONArray graphlist = (JSONArray) jsonObject.get("@graph");      
        
        String gremlinQueries[] = new String[graphlist.size()];
        
        //Iterate over graph array
        for (int i = 0; i < graphlist.size(); i++) {        	
        	gremlinQueries[i] = createEdges((JSONObject)graphlist.get(i));
        }          	     
    }
    
    /* Returns true if url is valid */
    public static boolean isValid(String url) 
    { 
        /* Try creating a valid URL */
        try { 
            new URL(url).toURI(); 
            return true; 
        } 
          
        // If there was an Exception 
        // while creating URL object 
        catch (Exception e) { 
            return false; 
        } 
    } 

}
