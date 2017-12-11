package com.hbase.observer.v2;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ESClientv2 {
	private static final Log log = LogFactory.getLog(ESClientv2.class);
    // ElasticSearch的集群名称
    public static String clusterName;
    // ElasticSearch的host
    public static String nodeHost;
    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
    public static int nodePort;
    // ElasticSearch的索引名称
    public static String indexName;
    // ElasticSearch的类型名称
    public static String typeName;
    // ElasticSearch Client
    public static TransportClient client;
    public static  BulkProcessor bulkProcessor;

    /**
     * get Es config
     *
     * @return
     */
    public static String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : ESClientv2.class.getDeclaredFields()) {
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
        Settings settings = Settings.builder()
                .put("cluster.name", ESClientv2.clusterName)
                .put("transport.type", "netty3")
                .put("http.type", "netty3")
                .build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ESClientv2.nodeHost), ESClientv2.nodePort));
        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			}
		}).setBulkActions(10000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
}

    /**
     * Close ES client
     */
    public static void closeEsClient() {
    	ESClientv2.bulkProcessor.close();
        client.close();
    }
}
