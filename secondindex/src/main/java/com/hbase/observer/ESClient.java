package com.hbase.observer;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ESClient {
	// ElasticSearch的集群名称
	public static String clusterName="es";
	// ElasticSearch的host
	public static String nodeHost="10.110.13.176";
	// ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
	public static int nodePort=9300;
	// ElasticSearch的索引名称
	public static String indexName="study1";
	// ElasticSearch的类型名称
	public static String typeName="esstudy";
	// ElasticSearch Client
	public static TransportClient client;
	
	static{
		try {
			initEsClient();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	/**
	 * get Es config
	 *
	 * @return
	 */
	public static String getInfo() {
		List<String> fields = new ArrayList<String>();
		try {
			for (Field f : ESClient.class.getDeclaredFields()) {
				fields.add(f.getName() + "=" + f.get(null));
			}
		} catch (IllegalAccessException ex) {
			ex.printStackTrace();
		}
		return StringUtils.join(fields, ", ");
	}

	/**
	 * init ES client
	 */
	public static void initEsClient() throws UnknownHostException {
		// Settings settings = ImmutableSettings.settingsBuilder()
		// .put("cluster.name", ESClient.clusterName).build();
		// client = new TransportClient(settings)
		// .addTransportAddress(new InetSocketTransportAddress(
		// ESClient.nodeHost, ESClient.nodePort));

		// Settings settings = Settings.builder()
		// .put("cluster.name", ESClient.clusterName).build();
		Settings settings = Settings.builder().put("cluster.name", ESClient.clusterName).put("transport.type", "netty3")
				.put("http.type", "netty3").build();
		client = new PreBuiltTransportClient(settings);
		// client.addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
		// .addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
		client.addTransportAddress(
				new InetSocketTransportAddress(InetAddress.getByName(ESClient.nodeHost), ESClient.nodePort));
	}

	/**
	 * Close ES client
	 */
	public static void closeEsClient() {
		client.close();
	}
}
