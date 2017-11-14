package com.inspur.bigdata.manage.es.example;

import java.io.IOException;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;

public class EsTest {
	public static String clustername = "es";
	public static String hostname = "10.110.18.130";
	public static TransportClient client = null;
	public static String indexName = "test001";
	public static String typeName = "type001";
	public static String documentID = "1";
	public static void main(String[] args) {
		try {
			TransportClient client = ESUtils.getClient(clustername,hostname);
			ESUtils.listIndex(client);
//			ESUtils.createIndex(client,indexName);
//			ESUtils.existIndex(client,indexName);
//			ESUtils.listTypes(client,indexName);
//			ESUtils.writeDocument(client,indexName,typeName);
//			ESUtils.queryDocument(client,indexName,typeName);
//			ESUtils.deleteDocument(client,indexName,typeName,documentID);
			client.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
