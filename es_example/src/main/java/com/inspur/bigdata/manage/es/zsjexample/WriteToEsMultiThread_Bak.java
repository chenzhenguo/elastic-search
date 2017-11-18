package com.inspur.bigdata.manage.es.zsjexample;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 客体和权利人
 * 
 * @author wj
 *
 */

public class WriteToEsMultiThread_Bak {
	public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
	public static SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	// public static String hostname = "192.168.56.119";
	// public static String hostname = "10.110.18.131";
	public static String hostname = "10.110.13.179";
	// public static String hostname = "localhost";
	public static String clustername = "es";
	public static double count = 10000.0d;
	public static ObjectMapper mapper = new ObjectMapper();

	public static TransportClient getClient1withNOxpack() throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", clustername).put("transport.type", "netty4")
				.put("http.type", "netty4").build();
		TransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9300));
		return client;
	}

	public static void process(BulkProcessor bulkProcessor, ExecutorService threadPool,double total) throws IOException {

		// 不设定es索引的id，默认自动生成

		

		for (int i = 1; i < total; i++) {

			threadPool.submit(() -> {

				try {
					String qxdm = DataGenUtil.generatorqxdmStr();
					String bdcdyh = DataGenUtil.getBdcdyh(qxdm);
					// 客体索引
					bulkProcessor.add(new IndexRequest("keti3", "keti3").source(XContentFactory.jsonBuilder()
							.startObject().field("bdcdyh", bdcdyh).field("lx", BaseUtil.getScopeInt(1, 20))
							.field("uuid", BaseUtil.getUUid()).field("qx", qxdm)
							.field("zl", AreaUtil.getAreaCodeName(qxdm)).field("records", BaseUtil.getScopeInt(0, 1))
							.field("postDate", BaseUtil.getRandomTime("2015-01-01", "2017-12-31")).endObject()));

					// wn.addKetiCount();
					// 一个客体有几个权利人，每个权利人暂定一个权利
					int qlrcount = BaseUtil.getScopeInt(1, 3);
					for (int a = 0; a < qlrcount; a++) {
						String qluuid = BaseUtil.getUUid();
						bulkProcessor.add(new IndexRequest("qlr3", "qlr3").source(XContentFactory.jsonBuilder()
								.startObject().field("bdcdyh", bdcdyh).field("zjh", DataGenUtil.getIdCardNo())
								.field("uuid", BaseUtil.getUUid()).field("qluuid", qluuid)
								.field("lx", DataGenUtil.getQlrlx()).field("xm", RandomValue.getChineseName())
								// 随机生成几个汉字，作为单位地址
								.field("dw", RandomValue.getRandomChineseString())
								.field("records", BaseUtil.getScopeInt(0, 1))
								.field("postDate", BaseUtil.getRandomTime("2015-01-01", "2017-12-31")).endObject()));

						// wn.addQlrCount();
						bulkProcessor.add(new IndexRequest("ql3", "ql3").source(XContentFactory.jsonBuilder()
								.startObject().field("bdcdyh", bdcdyh).field("uuid", qluuid)
								.field("qllx", DataGenUtil.getQllx()).field("qlxz", DataGenUtil.getQlxz())
								.field("qx", qxdm)
								// 随机生成几个汉字，作为单位地址
								// .field("dw",
								// RandomValue.getRandomChineseString())
								.field("records", BaseUtil.getScopeInt(0, 1))
								.field("postDate", BaseUtil.getRandomTime("2015-01-01", "2017-12-31")).endObject()));
						// wn.addQlCount();

					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {

				}

			});

//			long end = System.currentTimeMillis();
//			double cost = (end - start) / 1000.0;
//			System.out.println(Math.round(count / cost) + "条/秒");
		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length >= 1 && args[0] != null) {
			count = Integer.valueOf(args[0]);
		}
		long start = System.currentTimeMillis();
		TransportClient client = WriteToEsMultiThread_Bak.getClient1withNOxpack();

		ExecutorService threadPool = Executors.newFixedThreadPool(10);

		BulkProcessor[] bulkProcessors = new BulkProcessor[6];

		for (int i = 0; i < bulkProcessors.length; i++) {

			bulkProcessors[i] = BulkProcessor.builder(client, new BulkProcessor.Listener() {
				public void beforeBulk(long executionId, BulkRequest request) {
				}

				public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
					// System.out.println("尝试插入："+request.numberOfActions()+"条记录");
				}

				public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				}
			}).setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB)).setBulkActions(10000).build();

			process(bulkProcessors[i], threadPool,count/bulkProcessors.length);

		}
		threadPool.shutdown();
		while (true) {
			if (threadPool.isTerminated()) {
				System.out.println("---END---\n");

				final long end = System.currentTimeMillis();
				
				double cost = (end - start) / 1000.0;
				System.out.println(Math.round(count / cost) + "条/秒");
				
				break;
			}

		}

	}

}
