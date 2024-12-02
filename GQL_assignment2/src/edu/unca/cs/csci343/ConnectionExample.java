package edu.unca.cs.csci343;

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

	public static void main(String[] args) throws AuthFailedException, IOErrorException {
        String username = "";
		NebulaClient client = NebulaClient.builder("hostname:9669", username, "password").build();

        String schema_select = "SESSION SET SCHEMA `/" + username + "`";
        client.execute(schema_select);
        
        String gql_string = ""; // put your query here
        
        ResultSet result = client.execute(gql_string);
        if (!result.isSucceeded()) {
            log.log(Level.SEVERE, String.format("Execute: `%s', failed: %s",
                    gql_string, result.getErrorMessage()));
            System.exit(1);
        }

        // get column names
        List<String> colNames = result.getColumnNames();

        
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
