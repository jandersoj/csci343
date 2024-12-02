package edu.unca.cs.csci343;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import com.vesoft.nebula.driver.graph.data.Edge;
import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import com.vesoft.nebula.driver.graph.data.Node;

public class NebulaGraphToJSON {

    // Converts all GQL returned data structures to JSON
    // All primitive types are converted to strings with the toString() method
    // Nodes and edges are converted to a respective JSON document
    // Paths are constructed as nested documents with the key of each sub document being the edge or node label set
    // Primitives and lists require a fieldName to create a valid JSON document, the fieldname parameter is otherwise unused.
    public static JSONObject toJSON(String fieldName, ValueWrapper rec) {
    	if (rec.isNode() || rec.isEdge()) {
    		return DocToJSON(rec);
    	} else if (rec.isPath()) { 
    		
    		JSONObject last = null;
    		String lastFieldName = null;

    		List<ValueWrapper> path = rec.asPath().values();
    		for(int i = path.size() - 1; i >= 0; i --) {
    			ValueWrapper v = path.get(i);
    			JSONObject cur = DocToJSON(v);
    			
    			List<String> labels;
    			// for creating a document, use next labels as field name
    			if (v.isNode()) {
       				labels = v.asNode().getLabels();
    			} else {
    				labels = v.asEdge().getLabels();
    			}
       			String curFieldName = String.join(",", labels);

       			if (last != null) {
        			cur.put(lastFieldName, last.toMap());    				
    			}
				last = cur;
				lastFieldName = curFieldName;
			
    		}

    		return last;
    	} else if (rec.isList()) {
    		JSONObject returnval = new JSONObject();
    		for(ValueWrapper v : rec.asList()) {
    			if (isPrimitive(v))
    				returnval.append(fieldName, v.toString());
    			else 
    				returnval.append(fieldName, toJSON(null, v));
    		}
    		return returnval;
    	} else {
    		return new JSONObject(fieldName, rec.toString());
    	}
    }
    
    // returns true if the type is unstructured
    public static boolean isPrimitive(ValueWrapper value) {
    	return ! (value.isEdge() || value.isList() || value.isNode() || value.isPath() || value.isRecord());
    }
    
    // converts nodes or edges to json, preserving key: value names
    // values are converted to strings using the toString() method.
    public static JSONObject DocToJSON(ValueWrapper rec) {
    	
    	JSONObject returnval = new JSONObject();
    	Map<String, ValueWrapper> rawMap;
    	
    	if (rec.isNode()) {
    		Node n = rec.asNode();
    		rawMap = n.getProperties();
    	} else if (rec.isEdge()) {
    		Edge e = rec.asEdge();
    		rawMap = e.getProperties();
    	} else 
    		return null;
    	
    	for (String s: rawMap.keySet()) {
    	
    		ValueWrapper vw = rawMap.get(s);
   			returnval.put(s, vw.toString());
    	}
    	
    	if (rec.isEdge()) {
    		Edge e = rec.asEdge();

    		returnval.put("srcId", e.getSrcId());
    		returnval.put("dstId", e.getDstId());		
    	}
    	
    	return returnval;
    }

    
    public static List<JSONObject> processDocumentList(ResultSet results, String insertField) {
    	return processDocumentList(results, insertField, true);
    }

    public static List<JSONObject> processDocumentList(ResultSet results, String insertField, boolean collapseSingleEntry) {
    	List<JSONObject> returnval = new LinkedList<JSONObject>();
    	
    	while (results.hasNext()) {
	       	ResultSet.Record record = results.next();
	
	       	ValueWrapper node1 = record.values().get(0);
	       	ValueWrapper node2 = record.values().get(1);
	
	       	// have we already processed node1? if so, choose that as the document, otherwise create a new one
	       	JSONObject append = null;
	       	for (JSONObject j : returnval) {
	       		if (j.get("id").equals(node1.asNode().getProperties().get("id").asString())) {
	       			append = j;
	       			break;
	       		}
	       	}
	       	if (append == null) {
	       		append = NebulaGraphToJSON.DocToJSON(node1);
	       		returnval.add(append);
	       	}
	        // add on the second node to a list called "insertField"
	       	if (collapseSingleEntry)
	       		append.accumulate(insertField, NebulaGraphToJSON.DocToJSON(node2));
	       	else
	       		append.append(insertField, NebulaGraphToJSON.DocToJSON(node2));	       		
    	}
    	
        return returnval;    	
    	
    }
}
