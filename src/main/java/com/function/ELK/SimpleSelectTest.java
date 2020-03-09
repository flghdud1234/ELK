package com.function.ELK;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

public class SimpleSelectTest {

	public SimpleSelectTest() {
		
		//selectOneDocument();
		//selectOneDocumentSource();
		selectMultipleDocument(); // 아직 실패중
	}
	
	public void selectOneDocument() {
		// Create Client
		RestHighLevelClient client = new RestHighLevelClient(
			RestClient.builder(new HttpHost("localhost", 9200, "http")));	
		
		/***********************************************
		 * 			1. index/id 값을 통한 1건 조회 		   	   *
		 ***********************************************/ 
		String INDEX_NAME = "test_index";
		String DOCUMENT_ID = "GKnIrnABUYKPAKsh0amt";
		
		GetRequest request = new GetRequest(INDEX_NAME, DOCUMENT_ID);
		
		// Optional [Setting Request Option]
		String[] includes = new String[]{"host_name", "response_time", "stamp_time"};
		String[] excludes = Strings.EMPTY_ARRAY;
		FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
		request.fetchSourceContext(fetchSourceContext);
		

		// Get Response
		try {
			GetResponse response = client.get(request, RequestOptions.DEFAULT);
			System.out.println("Result : " + response.toString());
			
			String index = response.getIndex();
			String id = response.getId();
			
			if(response.isExists()) {
				long version= response.getVersion();
				String sourceAsString = response.getSourceAsString();
				Map<String, Object> sourceAsMap = response.getSourceAsMap();
				byte[] sourceAsBytes = response.getSourceAsBytes();
				
				System.out.println("index :" + index);
				System.out.println("id :" + id);
				System.out.println("version : " + version);
				System.out.println("sourceAsString : " + sourceAsString);
				System.out.println("sourceAsMap : " + sourceAsMap);
				System.out.println("sourceAsBytes : " + sourceAsBytes);
				
			} else {
				System.out.println("No data found");
			}	
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
	
	public void selectOneDocumentSource() {
		// Create Client
		RestHighLevelClient client = new RestHighLevelClient(
			RestClient.builder(new HttpHost("localhost", 9200, "http")));	
		
		/***********************************************
		 * 			2. index/id 값을 통한 1건  _source 조회            *
		 ***********************************************/ 
		String INDEX_NAME = "test_index";
		String DOCUMENT_ID = "GKnIrnABUYKPAKsh0amt";
		
		GetRequest request = new GetRequest(INDEX_NAME, DOCUMENT_ID);
		
		// Optional [Setting Request Option]
		String[] includes = new String[]{"host_name", "response_time", "stamp_time"};
		String[] excludes = Strings.EMPTY_ARRAY;
		FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
		request.fetchSourceContext(fetchSourceContext);

		// Get Response
		try {
			GetResponse response = client.get(request, RequestOptions.DEFAULT);
			Map<String, Object> source = response.getSource();
			
			System.out.println("Result format1 : " + response.getSource().toString());
			System.out.println("Result format2 [stamp_time] : " + source.get("stamp_time"));
			System.out.println("Result format2 [response_time]: " + source.get("response_time"));
			System.out.println("Result format2 [host_name]: " + source.get("host_name"));
			
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
	
	public void selectMultipleDocument() {
		// Create Client
		RestHighLevelClient client = new RestHighLevelClient(
			RestClient.builder(new HttpHost("localhost", 9200, "http")));	
		
		/***********************************************
		 * 			3. index 값을 통한 다건 조회             		   *
		 ***********************************************/ 
		String INDEX_NAME = "test_response_time";
		String DOCUMENT_ID = "*";
		
		GetRequest request = new GetRequest(INDEX_NAME, DOCUMENT_ID);
		
		// Optional [Setting Request Option]
		String[] includes = new String[]{"host_name", "response_time", "stamp_time"};
		String[] excludes = Strings.EMPTY_ARRAY;
		FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
		request.fetchSourceContext(fetchSourceContext);

		// Get Response
		try {
			GetResponse response = client.get(request, RequestOptions.DEFAULT);
			System.out.println("Result : " + response.toString());			
			
			//Map<String, Object> source = response.getSource();
			
			//System.out.println("Result format1 : " + response.getSource().toString());
			//System.out.println("Result format2 [stamp_time] : " + source.get("stamp_time"));
			//System.out.println("Result format2 [response_time]: " + source.get("response_time"));
			//System.out.println("Result format2 [host_name]: " + source.get("host_name"));
			
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
	
	
	
	
	
	public static void main(String args[]) {
		new SimpleSelectTest();
	}
}
