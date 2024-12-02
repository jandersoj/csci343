package edu.unca.cs.csci343;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.logging.Level;

import com.vesoft.nebula.driver.graph.data.Edge;
import com.vesoft.nebula.driver.graph.data.NRecord;
import com.vesoft.nebula.driver.graph.data.Node;
import com.vesoft.nebula.driver.graph.data.Path;
import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import com.vesoft.nebula.driver.graph.exception.AuthFailedException;
import com.vesoft.nebula.driver.graph.exception.IOErrorException;
import com.vesoft.nebula.driver.graph.net.NebulaClient;
import com.vesoft.nebula.proto.common.ZonedTime;

public class ConnectionExample {
    private static final java.util.logging.Logger log = java.util.logging.Logger.getLogger("ConnectionExample");
    
    private static void put(String key, String value, NebulaClient client)
    		throws IOErrorException {
    	String gql = "INSERT OR REPLACE (TYPED Entry{key: \""+ key + "\", val: \""+ value +"\"})";
    	ResultSet result = client.execute(gql);
    	if (!result.isSucceeded()) {
    		log.log(Level.SEVERE, String.format("Execute: ‘%s’, failed: %s",
    				gql, result.getErrorMessage()));
    		System.exit(1);
    	}
    }
    
    private static String get(String key, NebulaClient client)
    		throws IOErrorException {
    	String gql = "MATCH (n {key: \""+ key + "\"}) return n.val";
    	ResultSet result = client.execute(gql);
    	if (!result.isSucceeded()) {
    		log.log(Level.SEVERE, String.format("Execute: ‘%s’, failed: %s",
    				gql, result.getErrorMessage()));
    		System.exit(1);
    	}
    	if (! result.hasNext()) return null;
    	return result.next().get(0).asString();
    }
    
    private static String lookup(String domainName, NebulaClient client)
    		throws UnknownHostException, IOErrorException {
    	String returnval = get(domainName, client);
    	if (returnval == null) {
    		InetAddress address = InetAddress.getByName(domainName);
    		returnval = address.getHostAddress();
    		put(domainName, returnval, client);
    	}
    	return returnval;
    }
    
    //Private method to iterate through the existing records, verify each, and update if need be.
    //Although I'm not quite sure how to test it.
    private static void cacheChecker(ResultSet result, NebulaClient client) throws UnknownHostException, IOErrorException {
    	while(result.hasNext()) {
        	ResultSet.Record curr = result.next();
        	String thisKey = curr.get(0).asString();
        	String thisVal = curr.get(1).asString();
        	
        	String newVal = lookup(thisKey, client);
        	if(newVal != thisVal) {
        		System.out.println("Updating value for key "+thisKey+"...");
        		String update = "UPDATE (TYPED Entry{key: \""+ thisKey + "\", val: \""+ newVal + "\"})";
        		result = client.execute(update);
        	}
        }  
    }
    
	public static void main(String[] args) throws AuthFailedException, IOErrorException, UnknownHostException {
		
        String username = "jander15";
		NebulaClient client = NebulaClient.builder("localhost:9669", username, "Nebula1234").build();

        String schema_select = "SESSION SET SCHEMA `/" + username + "`";
        client.execute(schema_select);
        
        String gql_string = "CREATE GRAPH TYPE IF NOT EXISTS KeyValue AS {\n"
        		+ "NODE Entry ( LABEL Entry {key STRING, val STRING, PRIMARY KEY(key)})\n"
        		+ "}";
        
        ResultSet result = client.execute(gql_string);
        
        gql_string = "CREATE GRAPH IF NOT EXISTS AddressCache::KeyValue";
        result = client.execute(gql_string);
        
        gql_string = "SESSION SET GRAPH AddressCache";
        client.execute(gql_string);
       
        lookup("arden.cs.unca.edu", client);
        lookup("www.facebook.com", client);
        lookup("www.google.com", client);
        lookup("www.unca.edu", client);

        String all_nodes = "MATCH (p) return p.key, p.val";
        result = client.execute(all_nodes);
       
//        put("lastLookup","www.unca.edu", client);   
//        String lastLookedUp = get("lastLookup", client);
//        String lastLookedUpIP = get(lastLookedUp, client);
//        System.out.println(lastLookedUpIP);
        
        if (!result.isSucceeded()) {
            log.log(Level.SEVERE, String.format("Execute: `%s', failed: %s",
                    gql_string, result.getErrorMessage()));
            System.exit(1);
        }

        List<String> colNames = result.getColumnNames();
        
        cacheChecker(result, client);
            
        //it updates fine but the iteration is weird--it doesn't start over. 
        //I couldn't figure out how to reset the pointer because it's pretty limited functionality
        //I don't care because the assignment just said to update, 
        //but I thing it could be remedied with the built in forEach method... 
        //(as seen in https://github.com/vesoft-inc/nebula-java/blob/master/client/src/main/java/com/vesoft/nebula/client/graph/data/ResultSet.java)
        //if i could only figure out how to fix it. But anyway, updating works!
        
        while(result.hasNext()) {
        	ResultSet.Record record = result.next();
        	String myKey = record.get(0).asString();
        	String myVal = record.get(1).asString();
        	System.out.println(myKey + " => "+ myVal);
        }    
        
        
        while(result.hasNext()) {
        	
            ResultSet.Record record = result.next();          
            for (ValueWrapper value : record.values()) {

            	if (value.isInt()) {
                    int myData = value.asInt();
                }
                if (value.isLong()) {
                    long myData = value.asLong();
                }
                if (value.isBoolean()) {
                	boolean myData = value.asBoolean();
                }
                if (value.isFloat()) {
                	float myData = value.asFloat();
                }
                if (value.isDouble()) {
                	double myData = value.asDouble();
                }
                if (value.isString()) {
                	String myData = value.asString();
                }
                if (value.isLocalTime()) {
                	LocalTime myData = value.asLocalTime();
                }
                if (value.isDate()) {
                	LocalDate myData = value.asDate();
                }
                if (value.isLocalDateTime()) {
                    LocalDateTime myData = value.asLocalDateTime();
                }
                if (value.isList()) {
                    List<ValueWrapper> myData = value.asList();
                }
                if (value.isNode()) {
                	Node myData = value.asNode();
                }
                if (value.isEdge()) {
                	Edge myData = value.asEdge();
                }
                if (value.isPath()) {
                    Path myData = value.asPath();
                }
                if (value.isRecord()) {
                    NRecord myData = value.asRecord();
                }
                if (value.isZonedDateTime()) {
                	ZonedDateTime myData = value.asZonedDateTime().withZoneSameInstant(ZoneId.systemDefault());
                }
                if (value.isZonedTime()) {
                	OffsetTime myData = value.asZonedTime();
                }
                
            }
        }
        
        client.close();
        
	}

}
