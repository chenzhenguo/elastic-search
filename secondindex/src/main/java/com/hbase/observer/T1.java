package com.hbase.observer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class T1 {

	public static void main(String[] args) throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "es").put("transport.type", "netty4")
				.put("http.type", "netty4").build();
		TransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.18.47"), 9300));
		BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			}
		}).setBulkActions(10000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
		Map<String, String> map = new HashMap<String, String>();
		bulkProcessor.add(new IndexRequest("test003", "test003", "001").source(map));
		bulkProcessor.close();
	}

}
