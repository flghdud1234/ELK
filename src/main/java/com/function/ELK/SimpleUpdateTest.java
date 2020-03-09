package com.function.ELK;

import java.io.IOException;
import java.util.HashMap;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class SimpleUpdateTest {

	public SimpleUpdateTest() {
	
		// Create Client
		RestHighLevelClient client = new RestHighLevelClient(
			RestClient.builder(new HttpHost("localhost", 9200, "http")));

		
		/***********************************************
		 * 			1. index/id값  제공하면 update 개념 		   *
		 ***********************************************/ 
		String INDEX_NAME1 = "test_response_time";
		String SEARCH_ID1 = "TKFJe3ABI49iKiz6qMwa";
	    
		// Create Request
	    IndexRequest request1 = new IndexRequest(INDEX_NAME1).id(SEARCH_ID1);
	    
	    // Create Request Body
	    HashMap<String, Object> jsonMap1 = new HashMap<String, Object>();
	    jsonMap1.put("stamp_time", "202001070003");
	    jsonMap1.put("host_name", "SKT-ACL4086");
	    jsonMap1.put("response_time", 7988);
	    jsonMap1.put("@version", 1);
	    jsonMap1.put("path", "D:/ELK/data/Median_200225/median.csv");
	    jsonMap1.put("message", "202001070003	SKT-ACL4086	7988");
	    jsonMap1.put("@timestamp", "2020-02-25T07:39:48.197Z");
	    
	    // Bind Request and Body
	    request1.source(jsonMap1, XContentType.JSON);
	    
	    // Get Response
	    try {
	    	//Synchronous execution
			IndexResponse response1 = client.index(request1, RequestOptions.DEFAULT);
			System.out.println("Result : " + response1.toString());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
	    
		/***********************************************
		 * 			2. index값만  제공하면 insert 개념 		   *
		 ***********************************************/  
		String INDEX_NAME2 = "test_index";
	    
		// Create Request
	    IndexRequest request2 = new IndexRequest(INDEX_NAME2);
	    
	    // Create Request Body
	    HashMap<String, Object> jsonMap2 = new HashMap<String, Object>();
	    jsonMap2.put("stamp_time", "202001070003");
	    jsonMap2.put("host_name", "SKT-ACL4086");
	    jsonMap2.put("response_time", 7988);
	    
	    // Bind Request and Body
	    request2.source(jsonMap2, XContentType.JSON);
	    
	    // Get Response
	    try {
	    	//Synchronous execution
			IndexResponse response2 = client.index(request2, RequestOptions.DEFAULT);
			System.out.println("Result : " + response2.toString());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	    
	    	
				
		// Close Client
		try {
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new SimpleUpdateTest();
	}
}
