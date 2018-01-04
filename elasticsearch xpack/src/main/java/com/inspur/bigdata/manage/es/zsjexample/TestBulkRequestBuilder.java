package com.inspur.bigdata.manage.es.zsjexample;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Md5 工具
 */
public class TestBulkRequestBuilder {

	public static TransportClient getClient() throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", "es").put("transport.type", "netty4")
				.put("http.type", "netty4").build();
		TransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.6"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.7"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.8"), 9300));

		return client;
	}

	public static void testProcessor() throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", "es").build();
		TransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.6"), 9300));
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

		Map<String, Object> json = new HashMap();
		json.put("bdcdyh", "1");
		json.put("postDate", "2017-11-12 12:22:23");
		Map<String, Object> json2 = new HashMap();
		json2.put("bdcdyh", "2");
		json2.put("postDate", "xiugaiqian");
		Map<String, Object> json3 = new HashMap();
		json3.put("bdcdyh", "2");
		json3.put("postDate", "xiugaihou");
		IndexRequestBuilder update1 = client.prepareIndex("test001", "test001", "001").setSource(json);
		IndexRequestBuilder update2 = client.prepareIndex("test001", "test001", "002").setSource(json2);
		UpdateRequestBuilder update3 = client.prepareUpdate("test001", "test001", "002").setDocAsUpsert(true)
				.setDoc(json3);
		bulkRequestBuilder.add(update1);
		bulkRequestBuilder.add(update2);
		bulkRequestBuilder.add(update3);
		// bulkRequestBuilder.add(update3);
		BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
		if (bulkItemResponse.hasFailures()) {
			System.out.println(bulkItemResponse.buildFailureMessage());
		}
		client.close();
	}

	public static void testwqm() throws FileNotFoundException, IOException {
		String filePath = "D:/json1.txt";
		File file = new File(filePath);

		// Parse the JSON document
		final ObjectMapper mapper = new ObjectMapper();
		final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);

		try (InputStream in = new FileInputStream(file);) {
			rootNodeRef.set(mapper.readTree(in));

		}

		final JsonNode rootNode = rootNodeRef.get();

		if (rootNode.isArray()) {
			return;
		}

		final JsonNode contentNode = rootNode.get("content");
		String json = mapper.writeValueAsString(contentNode);// contentNode.toString();

		// es 操作

		// String json = contentNode.toString();
		System.out.println(json);

		TransportClient client = getClient();
		IndexResponse response = client.prepareIndex("testjson", "study").setId("1").setSource(json, XContentType.JSON)
				.get();

		System.out.println("写入成功" + ";fragment:" + response.isFragment());
	}
	
	 public static void testBachBulkRequestBuilder() throws Exception {
	        Settings settings = Settings.builder()
	                .put("cluster.name", "es")
	                .build();
	        TransportClient client = new PreBuiltTransportClient(settings);
	        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.6"), 9300));
	        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
	
		// IndexRequest re=new IndexRequest("a", "b");
		// re.source(source);
		// client.index(re).get();
	        /*
	         * write
	        
	        
	
	        for(int i=0;i<1;i++){
	            Map<String, Object> json = new HashMap();
	            json.put("bdcdyh", i);
	            bulkRequestBuilder.add(client.prepareIndex("tohdfs", "tohdfs", i+"").setSource(json));
	        }
	        BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
	        if (bulkRequestBuilder.numberOfActions() > 0) {
	            BulkResponse bulkItemResponse1 = bulkRequestBuilder.execute().actionGet();
	            if (bulkItemResponse.hasFailures()) {
	                System.out.println(bulkItemResponse1.buildFailureMessage());
	            }
	        }
	         client.close();
	         */

	


	        for (int i =1; i <=20000; i++) {
	            Map<String, Object> json = new HashMap();
	            json.put("bdcdyh", i);
	            System.out.println(i);
	            bulkRequestBuilder.add(client.prepareUpdate("tohdfs", "tohdfs",String.valueOf(0)).setDocAsUpsert(true).setDoc(json));
	        }

	        if (bulkRequestBuilder.numberOfActions() > 0) {
	            BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
	            if (bulkItemResponse.hasFailures()) {
	                System.out.println(bulkItemResponse.buildFailureMessage());
	            }
	        }

	        

	        client.close();
	        
	    }

	public static void main(String[] args) throws Exception {

//		testDt();
		testBachBulkRequestBuilder();

	}

}
