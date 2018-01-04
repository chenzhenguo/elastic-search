package com.inspur.bigdata.manage.es.zsjexample;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Testpageketi2 {
	public static Logger logger = LoggerFactory.getLogger(Testpageketi2.class);
	public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
	public static SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	// public static String hostname = "192.168.56.119";
	// public static String hostname = "10.110.18.131";
	public static String hostname = "10.110.13.176";
	// public static String hostname = "localhost";
	public static String clustername = "es";
	public static String index = "keti2";
	public static String type = "keti2";
	public static int count = 2;
	public static ObjectMapper mapper = new ObjectMapper();

	public static void main(String[] args) {
		try {
			if (args.length >= 1 && args[0] != null) {
				index = args[0];
			}
			if (args.length >= 2 && args[1] != null) {
				type = args[1];
			}
			if (args.length >= 3 && args[2] != null) {
				count = Integer.valueOf(args[2]);
			}
			SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
			TransportClient client = getClient1withNOxpack();
			System.out.println(formatDate.format(new Date()));
			bulkWriteMethod(client);
			// listIndex(client);
			// bulkWriteOneByOneMethod(client);
			client.close();
			System.out.println(formatDate.format(new Date()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	public static int getScopeInt(int min, int max) {
		Random random = new Random();
		int s = random.nextInt(max) % (max - min + 1) + min;
		return s;
	}
	
	/**
	 * 可以设定一次提交多少条，提交间隔，满足多少M提交，主动刷新提交
	 *
	 * @param client
	 * @throws UnknownHostException
	 */
	public static void bulkWriteMethod(TransportClient client) throws IOException {
		BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			public void beforeBulk(long executionId, BulkRequest request) {
				// System.out.println("t3 beforeBulk:" + formatDate.format(new
				// Date()));
			}

			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				// System.out.println("t3 afterBulk success:" +
				// formatDate.format(new Date()));
			}

			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				// System.out.println("t3 afterBulk failure:" +
				// formatDate.format(new Date()));
			}
		}).setBulkActions(10000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
		String qxdm = "";
		// 不设定es索引的id，默认自动生成
		for (int i = 1; i < count; i++) {
			qxdm = DataGenUtil.generatorqxdmStr();
			// uuid = BaseUtil.getUUid();
			// bdcdyh = DataGenUtil.getBdcdyh(qxdm);
			// bulkProcessor.add(new IndexRequest("pageindex", "tweet",
			// String.valueOf(i))
			bulkProcessor.add(new IndexRequest(index, type).source(XContentFactory.jsonBuilder().startObject()
					.field("bdcdyh", DataGenUtil.getBdcdyh(qxdm)).field("lx", getScopeInt(1, 20))
					.field("uuid", UUID.randomUUID().toString().replaceAll("-", "")).field("qx", qxdm).field("zl", AreaUtil.getAreaCodeName(qxdm))
					.field("records", getScopeInt(0, 1))
					// .field("zjh",DataGenUtil.getIdCardNo())
					.field("postDate", getRandomTime("2015-01-01", "2017-12-31")).endObject()));
		}
		bulkProcessor.close();
		// bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
	}

	public static TransportClient getClient1withNOxpack() throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", clustername).put("transport.type", "netty4")
				.put("http.type", "netty4").build();
		TransportClient client = new PreBuiltTransportClient(settings);
		// client.addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
		// .addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9300));
		return client;
	}

	/**
	 * 获取所有的索引
	 *
	 * @param client
	 * @throws UnknownHostException
	 */
	public static void listIndex(TransportClient client) throws UnknownHostException {
		ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet();
		// 获取所有索引
		String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
		for (String index : indexs) {
			System.out.println(index);
		}
	}

	/**
	 * 获取一条一条数据，批量写入
	 *
	 * @param client
	 * @throws UnknownHostException
	 */
	public static void bulkWriteOneByOneMethod(TransportClient client) throws IOException {
		XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		System.out.println("aaa" + formatDate.format(new Date()));
		for (int i = 1; i < count; i++) {
			bulkRequest.add(client.prepareIndex("pageindex", "tweet", String.valueOf(i))
					.setSource(XContentFactory.jsonBuilder().startObject().field("idstr", String.valueOf(i))
							.field("user", "kimchy".concat(String.valueOf(i))).field("postDate", new Date())
							.endObject()));
			//
			// bulkRequest.add(client.prepareIndex("test001", "tweet", "2")
			// .setSource(XContentFactory.jsonBuilder()
			// .startObject()
			// .field("user", "kimchy")
			// .field("postDate", new Date())
			// .field("message", "another post")
			// .endObject()
			// )
			// );
		}
		BulkResponse bulkResponse = bulkRequest.get();
		System.out.println("bbb" + formatDate.format(new Date()));
		if (bulkResponse.hasFailures()) {
			System.out.println(bulkResponse.toString());
		}
	}

	/**
	 * 返回一个指定区间的随机事件
	 * 
	 * @return
	 */
	public static String getRandomTime(String startStr, String endStr) {

		try {
			Date start = format.parse(startStr);
			Date end = format.parse(endStr);// 构造结束日期
			long date = start.getTime() + (long) (Math.random() * (end.getTime() - start.getTime()));
			return format.format(new Date(date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // 构造开始日期
		return "2015-11-28 20:09:58";
	}

}
