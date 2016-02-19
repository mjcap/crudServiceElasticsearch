package com.capbpm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

//import javax.persistence.Column;
//import javax.persistence.Table;
import javax.xml.bind.JAXBException;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.index.query.QueryBuilders.*;


@RestController
public class GetController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    private static int upper = 100;
    private static int lower = 1;
        
    /*
     *     	Settings settings = ImmutableSettings.settingsBuilder()
    	        .put("cluster.name", "elasticsearch").build();
    	Client client = new TransportClient(settings)
    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300));

    	//generate table and CRUD
 
    	//no concept of generating a table
    	//this will create and index with field names matching the json doc with "" values
    	client.prepareIndex("customer", "customer")
        .setSource(putJsonDocument("",
                                   "",
                                   "",
                                   "",
                                   "")).execute().actionGet();
    	
    	//Create/insert a record
    	client.prepareIndex("customer", "customer")
        .setSource(putJsonDocument("a",
                                   "b",
                                   "c",
                                   "d",
                                   "e")).execute().actionGet();
    	
    	//Retrieve/select a record
    	searchDocument(client, "customer", "customer", "fld1", "a");	

    	//Update a record
    	ArrayList<String> matchingIdx = searchDocumentForId(client, "customer", "customer", "fld1", "a");
    	for (String s:matchingIdx){
    	   updateDocument(client, "customer", "customer", s, "fld2", "I HAVE BEEN CHANGED");
    	}
    	
    	
    	//Delete a record
    	ArrayList<String> matchingIdx = searchDocumentForId(client, "customer", "customer", "fld1", "a");
    	for (String s:matchingIdx){
    		System.out.println("searchDocumentForIdx returned index="+s);
        	deleteDocument(client, "customer", "customer", s);    		
    	}
     */

    @RequestMapping("/generateTable3")
    public String generateTables(@RequestParam(value="input") String input) throws JAXBException, JSONException{
         String result = "";
         String mainTableName = null;
         String tableName = null;
         boolean mapFlag = false, mapEntryFlag = false;
         boolean done = false;
         JSONObject jsonResult = new JSONObject();
         jsonResult.put("status", "error");
         
         ArrayList<String> tableNameArrList = new ArrayList<String>();      
         HashMap<String,ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();

     	 RestTemplate restTemplate = new RestTemplate(); 
     	 String s = restTemplate.getForObject(input, String.class);
 	     String[] xsdArray = s.split("\\r?\\n");
 	     Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();    
 	     for (String line : xsdArray){
 	    	line = line.trim();
 	    	line = line.replace("<", "");
 	    	line = line.replace(">", "");
 	    	
 	    	System.out.println("line=["+line+"]");
 	    	String[] lineStrArr = line.split(" ");
 	    	for (String element : lineStrArr){
 	    		//this is the START of the xs:complexType tag and indicates we have a table
 	    		if (element.equals("xs:complexType")){
            		String complexTypeName=findValue(lineStrArr,"name=");
            		if (complexTypeName.equals("Map")){
            			mapFlag = true;
            			mapEntryFlag = false;
            		}
            		else if (complexTypeName.equals("MapEntry")){
            			        done = false;
            			        mapFlag = false;
            			        mapEntryFlag = true;
	            				tableName = mainTableName+"mapentry";
	            			    tableNameArrList.add(tableName);
	            			    tableNameAndColumns.put(tableName, new ArrayList<Column>());
	            	}
	            	else{    	
	            		        done = false;
	            		        mapFlag = false;
	            		        mapEntryFlag = false;
	            		        
	                    		mainTableName = complexTypeName.toLowerCase();
	                    		tableName = mainTableName;
	                    		tableNameArrList.add(tableName);
	                    		tableNameAndColumns.put(tableName, new ArrayList<Column>());  
	            	}	            	
            	}
            	else if ((element.equals("xs:element")) && (mapFlag == false) && (done == false)){
            		String keyName = findValue(lineStrArr,"name=");
            		String valueType = findValue(lineStrArr,"type=");
            		if (valueType.equals("tns:Map")){
            		  valueType = "xs:string";	
            		}
            		else if (valueType.equals("xs:anyType")){
            		  valueType = "xs:string";
            		}
            		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
            		keyNameValueTypePairs.put(keyName, valueType);
            		
            		Column col = new Column(keyName, valueType);
            		ArrayList columnArrList = tableNameAndColumns.get(tableName);
            		if (columnArrList.size() == 0){
            			col.setKey(true);
            		}
            		columnArrList.add(col);
            		tableNameAndColumns.put(tableName,columnArrList);
            	} 	
 	    		//this is the END of the xs:complexType tag and indicates the end of this table's definition
            	else if (element.equals("/xs:complexType")){
            		done = true;
            	}
 	    	} 	    	
 	     }
	     
         Hashtable<String, Hashtable<String, Object>> tableNameBlankRecordValues = generateCreateStrings(tableNameAndColumns);
 		 
 		 Iterator<String> i = tableNameBlankRecordValues.keySet().iterator();
 		 System.out.println("creating");
 		 while (i.hasNext()){
 			String indexName = i.next();
 			System.out.println("table="+indexName);
 			Hashtable<String, Object> blankValueHash = tableNameBlankRecordValues.get(indexName);
 			//Iterator<String> i2 = blankValueHash.keySet().iterator();  
 			//while (i2.hasNext()){
 			//	String columnName = i2.next();
 			//	System.out.println("  columnName:"+columnName+" columnValue:["+blankValueHash.get(columnName)+"]");
 			//}
 	    	Settings settings = ImmutableSettings.settingsBuilder()
	    	        .put("cluster.name", "elasticsearch").build();
	    	Client client = new TransportClient(settings)
	    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300));
	    	client.prepareIndex(indexName, indexName)
	    	.setId("0")
	        .setSource(blankValueHash).execute().actionGet();
 		 }
 
         return jsonResult.toString();
    }
  
    public Hashtable<String, Hashtable<String, Object>> generateCreateStrings(HashMap<String, ArrayList<Column>> tableNameAndColumns){
    	ArrayList<String> result = new ArrayList();
    	String createString;
    	String tableName;
	    Hashtable<String,Hashtable<String, Object>> tableNameBlankRecordValues = new Hashtable<String,Hashtable<String, Object>>();
	    Hashtable<String, Object> blankRecordValues = null;
	    
    	Iterator<String> i = tableNameAndColumns.keySet().iterator();
    	while (i.hasNext()){
    	   blankRecordValues = new Hashtable<String, Object>();	
    	   tableName = i.next();
    	   System.out.println("generateCreateStrings tableName="+tableName);
    	   ArrayList<Column> columns = tableNameAndColumns.get(tableName);
    	   String primaryKey = null;
    	   for (Column col: columns){
    		   String colName = col.getColumnName();
    		   String colType = col.getColumnType();
    		   String type = null;
    		   boolean isPrimaryKey = col.isKey();
    		   
   	    	   if (colType.equals("xs:string")){
   	    		blankRecordValues.put(colName, "");
	    	   }
	    	   else if (colType.equals("xs:dateTime")){
	    		   blankRecordValues.put(colName, "");
	    	   }
	    	   else if (colType.equals("xs:boolean")){
	    		   blankRecordValues.put(colName, new Boolean(false));
	    	   }
	    	   else if (colType.equals("xs:int")){
	    		   blankRecordValues.put(colName, new Integer(0));
	    	   }
	    	   else if (colType.equals("xs:double")){
	    		   blankRecordValues.put(colName, new Double(0));
	    	   }    		   
   	    	   System.out.println("  colName="+colName+" colValue=["+blankRecordValues.get(colName));
    	   }
    	   tableNameBlankRecordValues.put(tableName, blankRecordValues);
    	}
    			 
    	return tableNameBlankRecordValues;
    }    
    
    @RequestMapping("/deleteTable")
    public String deleteTable(@RequestParam(value="input") String input) throws JSONException{
    	JSONObject jsonResult = new JSONObject();
    	jsonResult.put("status", "error");
    	
    	String tableName = input;
    	String deleteTableString = "drop table "+tableName;
    	
    	Settings settings = ImmutableSettings.settingsBuilder()
    	        .put("cluster.name", "elasticsearch").build();
    	Client client = new TransportClient(settings)
    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300));

    	DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(tableName)).actionGet();
    	if (delete.isAcknowledged()) {
    		jsonResult = new JSONObject();
    		jsonResult.put("status", "success");
    	}
   	
    	
    	return jsonResult.toString();
    }    
    
    //generates table from xsd
    @RequestMapping("/generateTable")
    public String generate(@RequestParam(value="input") String input) throws JAXBException, JSONException{
    	
    	String tableName=null;
    	
    	RestTemplate restTemplate = new RestTemplate();
    	String s = restTemplate.getForObject(input, String.class);
	    System.out.println("GenerateController generate() rest call returned s="+s);   
	    Hashtable<String,Object> blankRecordValues = new Hashtable<String, Object>();
	    
	    String[] xsdArray = s.split("\\r?\\n");
	    Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();
	    
	    for (String line : xsdArray){
	    	//System.out.println("GenerateController generate() line="+line);
	    	line = line.trim();
	    	line = line.replace("<", "");
	    	line = line.replace(">", "");
	    	String[] lineStrArr = line.split(" ");
            for (String element : lineStrArr){
            	//System.out.println("GenerateController generate() element="+element);
            	if (element.indexOf("/xs:complexType") != -1){
            		/*
            		 * CREATE TABLE emp (
  empID int,
  deptID int,
  first_name varchar,
  last_name varchar
);
            		 */
            		String createTable = "CREATE TABLE "+tableName + "(";
            		String type = null;
            		
            		//System.out.println("GenerateController generate() Create table "+ tableName);
            		//System.out.println("GenerateController generate() Columns:");
            		Enumeration e = keyNameValueTypePairs.keys();
            		boolean execute = true;
            		boolean hasAtLeastOneColumn = false;
            		String primaryKey = null;
            		
            	    while (e.hasMoreElements()){
            	    	String columnNameXsd = (String)e.nextElement();
            	    	type = null;
            	    	String xsdType = keyNameValueTypePairs.get(columnNameXsd);
            	    	if (xsdType.compareTo("xs:string")==0){
            	    	    blankRecordValues.put(columnNameXsd, "");
            	    		type = "String";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:dateTime")==0){
            	    		blankRecordValues.put(columnNameXsd, "");
            	    		type = "String";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:boolean")==0){
            	    		blankRecordValues.put(columnNameXsd, new Boolean(false));
            	    		type = "Boolean";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:int")==0){
            	    		blankRecordValues.put(columnNameXsd, new Integer(0));
            	    		type = "Number";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:double")==0){
            	    		blankRecordValues.put(columnNameXsd, new Double(0));
            	    		type = "Number";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	
            	    	if (type != null){
            	    	  System.out.println("GenerateController generate() colName="+columnNameXsd+" colType="+type);
            	    	  createTable = createTable + columnNameXsd + " " + type;
            	    	  
            	    	  if (primaryKey == null){
            	    		  primaryKey = columnNameXsd;
            	    	  }
              	    	  if (e.hasMoreElements()){
            	    		createTable = createTable +", ";
            	    	  }           	    	  
            	    	  
            	    	}
            	    	


            	    }
            	    createTable = createTable +", PRIMARY KEY (" + primaryKey +"));";
            	    if (execute && hasAtLeastOneColumn){
            	    	Settings settings = ImmutableSettings.settingsBuilder()
            	    	        .put("cluster.name", "elasticsearch").build();
            	    	Client client = new TransportClient(settings)
            	    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300));
            	    	client.prepareIndex("customer", "customer")
            	    	.setId("0")
            	        .setSource(blankRecordValues).execute().actionGet();
            	    	
            	    }
            	    keyNameValueTypePairs = new Hashtable<String,String>();
            	}
            	else if (element.indexOf("xs:complexType") != -1){
            		tableName=findValue(lineStrArr,"name=");
            		//System.out.println("GenerateController generate() tableName="+tableName);
            	}
            	else if (element.indexOf("xs:element") != -1){
            		String keyName = findValue(lineStrArr,"name=");
            		String valueType = findValue(lineStrArr,"type=");
            		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
            		keyNameValueTypePairs.put(keyName, valueType);
            	}
            }
	    }
	    
	    
    	JSONObject jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString();
    }

    @RequestMapping("/create")
    public String create(@RequestParam(value="input") String input) throws JSONException {
    	
		JSONObject jo = new JSONObject(input);
		String table = jo.getString("table");    	
    	
		Settings settings = ImmutableSettings.settingsBuilder()
    	        .put("cluster.name", "elasticsearch").build();
    	Client client = new TransportClient(settings)
    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300));    	
		HashMap<String, String> tableColumnNamesTypes = getMetadata(table);

		HashMap<String,Object> insertMap = new HashMap<String,Object>();
		
		JSONArray columnNameValueArr = jo.getJSONArray("colval");
		for (int idx=0; idx < columnNameValueArr.length(); idx++){
			JSONObject colValObj = columnNameValueArr.getJSONObject(idx);
			String column = colValObj.getString("column");
			String type = tableColumnNamesTypes.get(column);
			
			
			/*if (type.compareTo("string") == 0){
				SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
				String val = colValObj.getString("value");
				insertMap.put(column, val);
			}
			else if (type.compareTo("double") == 0){
				insertMap.put(column, colValObj.getDouble("value"));
			}else if (type.compareTo("long") == 0){
				insertMap.put(column, colValObj.getLong("value"));
			}else if (type.compareTo("string") == 0){
				insertMap.put(column, colValObj.getString("value"));
			}
			else if (type.compareTo("boolean") == 0){
				insertMap.put(column, colValObj.getBoolean("value"));
			}*/
			
			if (type.compareTo("string") == 0){
				//SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
				String val = colValObj.getString("value");
				insertMap.put(column, val);
			}
			else if (type.compareTo("double") == 0){
				insertMap.put(column, colValObj.getDouble("value"));
			}else if (type.compareTo("long") == 0){
				insertMap.put(column, colValObj.getLong("value"));
			}else if (type.compareTo("boolean") == 0){
				insertMap.put(column, colValObj.getBoolean("value"));
			}
			
		}
		
    	client.prepareIndex(table, table).setSource(insertMap).execute().actionGet();
    	
    	JSONObject jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString();    	
    }

    @RequestMapping("/read")
    public String here(@RequestParam(value="input") String input) throws JSONException{
    	
    	JSONArray ja = new JSONArray();
    	JSONObject jo = new JSONObject(input);
		String table = jo.getString("table");
		HashMap<String, String> tableColumnNamesTypes = getMetadata(table);
		JSONArray compArr = jo.getJSONArray("comps");
		
    	RestTemplate restTemplate = new RestTemplate();
    	
    	//this is an equality search
    	//String a = "http://odm.capbpm.com:9200/customer/customer/_search?q=age:32";
    	//this is a range search on numbers
    	String a = "http://odm.capbpm.com:9200/customer/customer/_search?q={q}";
    	HashMap<String,String> m = generateQueryString(compArr, tableColumnNamesTypes);
    	//  m.put("q", "age:[0 TO 1]");
    	//  m.put("q", "-_id:0 AND (age:[20 TO 40])");
    	
    	Iterator i = m.keySet().iterator();
    	while (i.hasNext()){
    		String key = (String) i.next();
    		String val = m.get(key);
    		System.out.println("key="+key+" val="+val);
    	}
    	
    	String s = restTemplate.getForObject(a, String.class,m);
    	JSONObject joResult = new JSONObject(s);
    	System.out.println("joResult=");
    	System.out.println(joResult.toString(2));
    	/*
    	 * 
			 {
			  "took": 15,
			  "timed_out": false,
			  "_shards": {
			    "total": 5,
			    "successful": 5,
			    "failed": 0
			  },
			  "hits": {
			    "total": 1,
			    "max_score": 2.25,
			    "hits": [{
			      "_index": "customer",
			      "_type": "customer",
			      "_id": "AVE-wuheTJZ8QzqUe-pH",
			      "_score": 2.25,
			      "_source": {
			        "firstName": "joe",
			        "lastName": "fabietz",
			        "accountBalance": 1,
			        "isActive": true,
			        "startDate": "2015-11-23 12:00:00",
			        "age": 32
			      }
			    }]
			  }

              JSONArray resultArr = j.getJSONObject("hits").getJSONArray("hits");
              JSONArray resultsWithoutElasticsearchMetadata = new JSONArray();
              for (JSONObject jo: resultArr){
                 resultsWithoutElasticsearchMetadata.add(jo.get("_source")
              }
    	 */
        JSONArray resultArr = joResult.getJSONObject("hits").getJSONArray("hits");
        JSONArray resultsWithoutElasticsearchMetadata = new JSONArray();
        for (int idx = 0; idx < resultArr.length(); idx++){
        	JSONObject subObj = resultArr.getJSONObject(idx);
            resultsWithoutElasticsearchMetadata.put(subObj.get("_source"));
        }
    	
    	System.out.println(resultsWithoutElasticsearchMetadata.toString(2));
    	return resultsWithoutElasticsearchMetadata.toString();

    }

    @RequestMapping("/delete")
    public String delete(@RequestParam(value="input") String input) throws JSONException{
    	
    	JSONArray ja = new JSONArray();
    	JSONObject jo = new JSONObject(input);
		String table = jo.getString("table");
		HashMap<String, String> tableColumnNamesTypes = getMetadata(table);
		JSONArray compArr = jo.getJSONArray("comps");
		
    	RestTemplate restTemplate = new RestTemplate();
    	
    	//this is an equality search
    	//String a = "http://odm.capbpm.com:9200/customer/customer/_search?q=age:32";
    	//this is a range search on numbers
    	String a = "http://odm.capbpm.com:9200/customer/customer/_search?q={q}";
    	HashMap<String,String> m = generateQueryString(compArr, tableColumnNamesTypes);
    	//  m.put("q", "age:[0 TO 1]");
    	//  m.put("q", "-_id:0 AND (age:[20 TO 40])");
    	
    	Iterator i = m.keySet().iterator();
    	while (i.hasNext()){
    		String key = (String) i.next();
    		String val = m.get(key);
    		System.out.println("key="+key+" val="+val);
    	}
    	
    	String s = restTemplate.getForObject(a, String.class,m);
    	JSONObject joResult = new JSONObject(s);
    	System.out.println("joResult=");
    	System.out.println(joResult.toString(2));
    	/*
    	 * 
			 {
			  "took": 15,
			  "timed_out": false,
			  "_shards": {
			    "total": 5,
			    "successful": 5,
			    "failed": 0
			  },
			  "hits": {
			    "total": 1,
			    "max_score": 2.25,
			    "hits": [{
			      "_index": "customer",
			      "_type": "customer",
			      "_id": "AVE-wuheTJZ8QzqUe-pH",
			      "_score": 2.25,
			      "_source": {
			        "firstName": "joe",
			        "lastName": "fabietz",
			        "accountBalance": 1,
			        "isActive": true,
			        "startDate": "2015-11-23 12:00:00",
			        "age": 32
			      }
			    }]
			  }

        JSONArray resultArr = joResult.getJSONObject("hits").getJSONArray("hits");
        ArrayList<String> matchingIds = new ArrayList<String>();
        for (int idx = 0; idx < resultArr.length(); idx++){
        	JSONObject subObj = resultArr.getJSONObject(idx);
            matchingIds.add(subObj.get("_id"));
        }
    	 */
    	
		Settings settings = ImmutableSettings.settingsBuilder()
    	        .put("cluster.name", "elasticsearch").build();
    	Client client = new TransportClient(settings)
    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300)); 
    	
        JSONArray resultArr = joResult.getJSONObject("hits").getJSONArray("hits");
        ArrayList<String> matchingIds = new ArrayList<String>();
        for (int idx = 0; idx < resultArr.length(); idx++){
        	JSONObject subObj = resultArr.getJSONObject(idx);
            matchingIds.add(subObj.getString("_id"));
            deleteDocument(client, "customer", "customer", subObj.getString("_id"));
        }
        
    	JSONObject jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString(); 

    }
    
    @RequestMapping("/update")
    public String update(@RequestParam(value="input") String input) throws JSONException{
    	
    	JSONArray ja = new JSONArray();
    	JSONObject jo = new JSONObject(input);
		String table = jo.getString("table");
		HashMap<String, String> tableColumnNamesTypes = getMetadata(table);
		JSONArray compArr = jo.getJSONArray("comps");
		JSONArray updateColumnArr = jo.getJSONArray("colval");
		
    	RestTemplate restTemplate = new RestTemplate();
    	
    	//this is an equality search
    	//String a = "http://odm.capbpm.com:9200/customer/customer/_search?q=age:32";
    	//this is a range search on numbers
    	String a = "http://odm.capbpm.com:9200/customer/customer/_search?q={q}";
    	HashMap<String,String> m = generateQueryString(compArr, tableColumnNamesTypes);
    	//  m.put("q", "age:[0 TO 1]");
    	//  m.put("q", "-_id:0 AND (age:[20 TO 40])");
    	
    	Iterator i = m.keySet().iterator();
    	while (i.hasNext()){
    		String key = (String) i.next();
    		String val = m.get(key);
    		System.out.println("key="+key+" val="+val);
    	}
    	
    	String s = restTemplate.getForObject(a, String.class,m);
    	JSONObject joResult = new JSONObject(s);
    	System.out.println("joResult=");
    	System.out.println(joResult.toString(2));
    	/*
    	 * 
			 {
			  "took": 15,
			  "timed_out": false,
			  "_shards": {
			    "total": 5,
			    "successful": 5,
			    "failed": 0
			  },
			  "hits": {
			    "total": 1,
			    "max_score": 2.25,
			    "hits": [{
			      "_index": "customer",
			      "_type": "customer",
			      "_id": "AVE-wuheTJZ8QzqUe-pH",
			      "_score": 2.25,
			      "_source": {
			        "firstName": "joe",
			        "lastName": "fabietz",
			        "accountBalance": 1,
			        "isActive": true,
			        "startDate": "2015-11-23 12:00:00",
			        "age": 32
			      }
			    }]
			  }

        JSONArray resultArr = joResult.getJSONObject("hits").getJSONArray("hits");
        ArrayList<String> matchingIds = new ArrayList<String>();
        for (int idx = 0; idx < resultArr.length(); idx++){
        	JSONObject subObj = resultArr.getJSONObject(idx);
            matchingIds.add(subObj.get("_id"));
        }
    	 */
    	
		Settings settings = ImmutableSettings.settingsBuilder()
    	        .put("cluster.name", "elasticsearch").build();
    	Client client = new TransportClient(settings)
    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300)); 
    	
        JSONArray resultArr = joResult.getJSONObject("hits").getJSONArray("hits");
        ArrayList<String> matchingIds = new ArrayList<String>();
        for (int idx = 0; idx < resultArr.length(); idx++){
        	JSONObject subObj = resultArr.getJSONObject(idx);

        	for (int idx2=0; idx2 < updateColumnArr.length(); idx2++){
        		JSONObject columnNameNewValue = updateColumnArr.getJSONObject(idx2);
        		String columnName = columnNameNewValue.getString("column");
        		Object newValue = columnNameNewValue.getString("value");
	        	//updateDocument(Client client, String index, String type,String id, String field, Object newValue)
	            updateDocument(client, "customer", "customer", subObj.getString("_id"), columnName, newValue);
        	}
        }
        
    	JSONObject jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString(); 

    }
    
    public HashMap<String, String> generateQueryString(JSONArray compArr, HashMap<String, String> tableColumnNamesTypes) throws JSONException{
    	HashMap<String, String> queryMap = new HashMap<String, String>();
    	String comparisonClause = "-_id:0 AND (";
    	
    	/*
    	 * comparisons ==, <=, >=, <, >, !=
    	 * ONLY SUPPORTS RANGE SEARCHES SO <, <= must be of form 
    	 * 
    	 * exclusive minValue < startDate < maxValue
    	 * {"column":"startDate" , "comp":"between", "minValue":"2015-11-23 12:00:00", "maxValue":"xxxxxxxxxxxx" }
    	 * 
    	 * or 
    	 * 
    	 * inclusive minValue <= startDate <= maxValue
    	 * 
    	 * if =  columnName:value                       {"column":"columnName" , "comp":"=", "value":"value" }
    	 * if != -columnName:value                      {"column":"columnName" , "comp":"!=", "value":"value" }
    	 * if <, <= MUST provide minValue & maxValue   
    	 *      < columnName:{minValue to maxValue}     {"column":"columnName" , "comp":"<", "value1":"value", "value2":"value" }
    	 *      <= columnName:[minValue to maxValue]    {"column":"columnName" , "comp":"<=", "value1":"value", "value2":"value" }
    	 */
		//JSONArray compArr = jo.getJSONArray("comps");
		for (int idx=0; idx < compArr.length(); idx++){
			ArrayList<Object> valuesList = new ArrayList<Object>();
			
			JSONObject compObj = compArr.getJSONObject(idx);
			String columnName = compObj.getString("column");
			String comp = compObj.getString("comp");
			if ((comp.equals("<") || comp.equals("<="))){
			   valuesList.add(compObj.getString("value1"));
			   valuesList.add(compObj.getString("value2"));
			}
			else{
				valuesList.add(compObj.getString("value"));;
			}
			
            String type = tableColumnNamesTypes.get(columnName);
			if (type.compareTo("string") == 0){
				//comparisonClause = comparisonClause + key + " " + comp + " " + parsedDate.getTime();
				comparisonClause = comparisonClause + generateLuceneComp(columnName, type, comp, valuesList);
			}
			else if (type.compareTo("double") == 0){
				//comparisonClause = comparisonClause + key + " " + comp + " " + new Double(val).doubleValue();
				comparisonClause = comparisonClause + generateLuceneComp(columnName, type, comp, valuesList);
			}else if (type.compareTo("long") == 0){
				//comparisonClause = comparisonClause + key + " " + comp + " " + new Integer(val).intValue();
				comparisonClause = comparisonClause + generateLuceneComp(columnName, type, comp, valuesList);
			}else if (type.compareTo("boolean") == 0){
			    //comparisonClause = comparisonClause + "=" + val;
				comparisonClause = comparisonClause + generateLuceneComp(columnName, type, comp, valuesList);
			}
			
			comparisonClause = comparisonClause + ")";
			
		}
		queryMap.put("q",comparisonClause);
    	return queryMap;
    }
    
    public String generateLuceneComp(String fieldName, String fieldType, String compType, ArrayList<Object>valuesList){
    	String result = "";
    	
    	
    	if (compType.compareTo("<=") == 0){
    	   if (fieldType.equals("string")){
    		   //fieldName:["valueList[0]" TO "valueList[1]"]
    		   result = fieldName + ":[\"" + valuesList.get(0) + "\" TO \"" + valuesList.get(1) + "\"]";  
    	   }
    	   else{
    		   //fieldName:[valueList[0] TO valueList[1]]
    		   result = fieldName + ":[" + valuesList.get(0) + " TO " + valuesList.get(1) + "]";  
    	   }
    	}
    	else if (compType.compareTo("<") == 0){
     	   if (fieldType.equals("string")){
    		   //fieldName:{"valueList[0]" TO "valueList[1]"}
    		   result = fieldName + ":{\"" + valuesList.get(0) + "\" TO \"" + valuesList.get(1) + "\"}";  
    	   }
    	   else{
    		   //fieldName:[valueList[0] TO valueList[1]]
    		   result = fieldName + ":{" + valuesList.get(0) + " TO " + valuesList.get(1) + "}";  
    	   }    		
    	}
    	else if (compType.compareTo("!=") == 0){
    		// -fieldName:valueList[0]
    		if (fieldType.equals("string")){
    		  result = "-" + fieldName + ":\"" + valuesList.get(0) + "\"";
    		}
    		else{
    			result = "-" + fieldName + ":" + valuesList.get(0) ;
    		}
    	}
    	else if (compType.compareTo("=") == 0){
    		// fieldName:valueList[0]
    		if (fieldType.equals("string")){
    		  result = fieldName + ":\"" + valuesList.get(0) + "\"";
    		}
    		else{
    			result = fieldName + ":" + valuesList.get(0) ;
    		}    		
    	}
    	return result;
    }
    	/*@RequestMapping("/read")
    public String read(@RequestParam(value="input") String input) {
    	
    }
    
    public void searchDocument(Client client, String index, String type,
            String field, String value){
			SearchResponse response = client.prepareSearch(index)
			              .setTypes(type)
			              .setSearchType(SearchType.QUERY_AND_FETCH)
			              .setQuery(QueryBuilders.termQuery(field, value))
			              .setFrom(0).setSize(60).setExplain(true)
			              .execute()
			              .actionGet();
			SearchHit[] results = response.getHits().getHits();
			System.out.println("Current results: " + results.length);
			for (SearchHit hit : results) {
				System.out.println("------------------------------");
				Map<String,Object> result = hit.getSource();    
				System.out.println(result);
			}
}*/
 
    public ArrayList<String> searchDocumentForId(Client client, String index, String type,
            String field, String value){
    	
    	ArrayList<String> result = new ArrayList<String>();
			SearchResponse response = client.prepareSearch(index)
			              .setTypes(type)
			              .setSearchType(SearchType.QUERY_AND_FETCH)
			              .setQuery(QueryBuilders.termQuery(field, value))
			              .setFrom(0).setSize(60).setExplain(true)
			              .execute()
			              .actionGet();
			SearchHit[] results = response.getHits().getHits();
			System.out.println("Current results: " + results.length);
			for (SearchHit hit : results) {
				System.out.println("------------------------------");
				result.add(hit.getId());    
			}
			return result;
}
    
    /*public static Map<String, Object> putJsonDocument(String title, String content, String postDate, 
            String tags, String author){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("fld1", title);
		jsonDocument.put("fld2", content);
		jsonDocument.put("fld3", postDate);
		jsonDocument.put("fld4", tags);
		jsonDocument.put("fld5", author);
		return jsonDocument;
    } */
    
    public static Map<String, Object> putJsonDocument(String title, String content, String postDate, 
            String tags, String author){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("fld1", title);
		jsonDocument.put("fld2", content);
		jsonDocument.put("fld3", postDate);
		jsonDocument.put("fld4", tags);
		jsonDocument.put("fld5", author);
		return jsonDocument;
    }    
    

    
    public static void deleteDocument(Client client, String index, String type, String id){
        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }
    
    public static void updateDocument(Client client, String index, String type, 
            String id, String field, Object newValue){
		Map<String, Object> updateObject = new HashMap<String, Object>();
		updateObject.put(field, newValue);
		client.prepareUpdate(index, type, id)
		.setScript("ctx._source." + field + "=" + field,ScriptService.ScriptType.INLINE)
		.setScriptParams(updateObject).execute().actionGet();
    }
    
    public HashMap<String, String> getMetadata(String tableName) throws JSONException{

    	HashMap<String,String> columnNameType = new HashMap<String,String>();
    	
    	RestTemplate restTemplate = new RestTemplate();
    	String s = restTemplate.getForObject("http://odm.capbpm.com:9200/customer/_mapping/"+tableName, String.class);
    	JSONObject metadata=new JSONObject(s);
  
    	//metadata.getJSONObject(tableName).getJSONObject("mappings").getJSONObject("properties");
    	JSONObject columnsAndTypesJSONObjects = metadata.getJSONObject(tableName).getJSONObject("mappings").getJSONObject(tableName).getJSONObject("properties");
	    System.out.println("GenerateController generate() rest call returned metadata=");
	    Iterator i = columnsAndTypesJSONObjects.keys();
	    while (i.hasNext()){
	    	String columnName = (String) i.next();
	    	JSONObject typeObject = columnsAndTypesJSONObjects.getJSONObject(columnName);
	    	String type = typeObject.getString("type");
	    	columnNameType.put(columnName, type);
	    }
	    System.out.println(metadata.getJSONObject(tableName).getJSONObject("mappings").getJSONObject(tableName).getJSONObject("properties").toString(2)); 
	 
	    i = columnNameType.keySet().iterator();
	    while (i.hasNext()){
	    	String cn = (String) i.next();
	    	System.out.println("columnName="+cn+" type="+columnNameType.get(cn));
	    }
	    
	    return columnNameType;
    }
    
    //xs:complexType name="Customer"
    public String findValue(String[] arr, String key){
    	//System.out.println("findValue arr="+arr+" key="+key);
    	String value=null;
    	for (int idx = 1; idx < arr.length; idx++){
    		if (arr[idx].indexOf(key) != -1){
    			String[] keyValueArr=arr[idx].split("=");
    			value = keyValueArr[1];
    			value = value.replace("\"", "");
    			break;
    		}
    	}
    	return value;
    	
    }    
}