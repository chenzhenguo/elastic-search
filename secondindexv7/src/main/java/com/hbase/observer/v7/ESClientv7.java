package com.hbase.observer.v7;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ESClientv7 {
	private static final Log log = LogFactory.getLog(ESClientv7.class);
	// ElasticSearch的集群名称
	public static String clusterName;
	// ElasticSearch的host
	public static String nodeHost;
	public static int nodePort;
	public static TransportClient client;

	/**
	 * get Es config
	 *
	 * @return
	 */
	public static String getInfo() {
		List<String> fields = new ArrayList<String>();
		try {
			for (Field f : ESClientv7.class.getDeclaredFields()) {
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
		if (client == null) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
			log.info("开始 initESClient:"+sdf.format(new Date()));
			Settings settings = Settings.builder().put("cluster.name", ESClientv7.clusterName)
					.put("client.transport.sniff", "true").put("transport.type", "netty3").put("http.type", "netty3")
					.build();
			client = new PreBuiltTransportClient(settings);
			String[] hosts = ESClientv7.nodeHost.split(";");
			for (String host : hosts) {
				client.addTransportAddress(
						new InetSocketTransportAddress(InetAddress.getByName(host), ESClientv7.nodePort));
			}
			log.info("完成 initESClient:"+sdf.format(new Date()));
		}
	}

	/**
	 * Close ES client
	 */
	public static void closeEsClient() {
		try{
		client.close();
		}catch(Exception e){
			String fullStackTrace = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e);
            System.out.println("closeEsClient:" + fullStackTrace);
		}
	}
}
