/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.data.sputnik.ext.LoggerStatistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * 
 */
public class Log4JLogger implements EventLogger {

    private static final Logger log = LogManager.getLogger(Log4JLogger.class);
    private static final Logger dataLogger = LogManager.getLogger("data-logger");
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void logEvent(String event) {
    	try {
    		//testme(event);
            Object theValue = null;
            if (event.startsWith("{")) { //plain json object = Map
                theValue = mapper.readValue(event, Map.class);
            } else if (event.startsWith("[")) { //array of json objects = List
                theValue = mapper.readValue(event, List.class);
            } else { //unknown, so leave it as the literal string
                theValue = event;
            }
            dataLogger.info(mapper.writeValueAsString(theValue));
        } catch (IOException ex) {
            log.error("Error logging event", ex);
        }
    }

    @Override
    public void shutdown() {
        //nothing to shutdown
    }
    
    public void testme(String event){
    	 ObjectMapper mapper = new ObjectMapper();
		 try {
			 	JsonNode node = mapper.readTree(event);
			    Iterator<String> fieldNames = node.fieldNames();
			    while(fieldNames.hasNext()){
			    	 String fieldName = fieldNames.next();
			    	 JsonNode fieldValue = node.get(fieldName);
			    	byte[] x = Bytes.toBytes(fieldName);
			    	byte[] y = Bytes.toBytes(fieldValue.toString());
			    }
		    
		    }catch(Exception e){
		    	e.printStackTrace();
		    }
		//	printAll("root", node);
		//	System.out.print("\n");
			
		
    }
    
    public void printAll(String name, JsonNode node){
    	if(!node.isValueNode()){
    		Iterator<String> fieldNames = node.fieldNames();
    		System.out.print("{ ");
    	     while(fieldNames.hasNext()){
   		         String fieldName = fieldNames.next();
   		         JsonNode fieldValue = node.get(fieldName);
   		         if (fieldValue.isArray()) {
   		        	System.out.print(fieldName + " [ ");
   		        	List<String> a = new ArrayList<>(1);
	   		        Iterator<JsonNode> itr = fieldValue.iterator();
	   		        while (itr.hasNext()) {
	   		            JsonNode n = itr.next();
	   		            printAll(fieldName, n);
	   		            System.out.print(", ");
	   		        }
	   		     System.out.print(" ] ");
   		         } else {
   		        	printAll(fieldName, fieldValue);
   		        	System.out.print(", ");
   		         }
   		     }
    	     System.out.print(" } ");
    	}else{
    		Iterator<Entry<String, JsonNode>> itParams = node.fields();
    		  while (itParams.hasNext()) {
    		    Entry<String, JsonNode> elt = itParams.next();
    		    System.out.println(elt.getKey());
    		}
    		if(node.isInt()){
    				System.out.print(name + " : " + node.intValue());
		         }else if(node.isDouble()){
		        	 System.out.print(name + " : " + node.doubleValue());
		         }else if(node.isTextual()){
		        	 System.out.print(name + " : " + node.textValue());
		         }else if(node.isLong()){
		        	 System.out.print(name + " : " + node.longValue());
		         }else
		        	 System.out.print(name + " : " + node.textValue());
		        	 
    		
    	}
    		
    }

	@Override
	public LoggerStatistics getStats() {
		// TODO Auto-generated method stub
		return null;
	}

}
