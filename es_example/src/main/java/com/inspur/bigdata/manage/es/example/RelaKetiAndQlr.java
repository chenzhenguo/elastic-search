package com.inspur.bigdata.manage.es.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 造一批有联系的客体和权利人数据
 * @author zhaoshengjie
 *
 */
public class RelaKetiAndQlr {
	public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
	public static SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	// public static String hostname = "192.168.56.119";
	// public static String hostname = "10.110.18.131";
	public static String hostname = "10.110.13.178";
	// public static String hostname = "localhost";
	public static String clustername = "es";
	public static String index = "qlr";
	public static String type = "qlr";
	public static int count = 100000;
	public static ObjectMapper mapper = new ObjectMapper();
	public static List<String> bdcdyhlist = new ArrayList<String>();
	
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
	public static void bulkWriteMethodKeti(TransportClient client) throws IOException {
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
		}).setBulkActions(1000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
		String qxdm = "";
		String bdcdyh = "";
		//不设定es索引的id，默认自动生成
		for (int i = 1; i < bdcdyhlist.size(); i++) {
			bdcdyh = bdcdyhlist.get(i);
			qxdm = bdcdyh.substring(0,6);
//			bulkProcessor.add(new IndexRequest("pageindex", "tweet", String.valueOf(i))
            bulkProcessor.add(new IndexRequest("keti", "keti")
					.source(XContentFactory.jsonBuilder().startObject()
							.field("bdcdyh", bdcdyh)
							.field("lx", BaseUtil.getScopeInt(1, 20))
							.field("uuid",BaseUtil.getUUid())
							.field("qx", qxdm)
							.field("zl", AreaUtil.getAreaCodeName(qxdm))
							.field("records", BaseUtil.getScopeInt(0, 1))
//							.field("zjh",DataGenUtil.getIdCardNo())
							.field("postDate", getRandomTime("2015-01-01", "2017-12-31")).endObject()));
		}
		bulkProcessor.close();
		// bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
	}
	public static void bulkWriteMethodqlr(TransportClient client) throws IOException {
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
		}).setBulkActions(1000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
		String qxdm = "";
		String bdcdyh = "";
		int t = 0;
		//不设定es索引的id，默认自动生成
		for (int i = 0; i < bdcdyhlist.size(); i++) {
			t = Integer.valueOf(BaseUtil.getScopeInt(1, 5));
			bdcdyh = bdcdyhlist.get(i);
			qxdm = bdcdyh.substring(0,6);
//            uuid = BaseUtil.getUUid();
//            bdcdyh = DataGenUtil.getBdcdyh(qxdm);
//			bulkProcessor.add(new IndexRequest("pageindex", "tweet", String.valueOf(i))
			for(int k=0;k< t;k++){
            bulkProcessor.add(new IndexRequest("qlr", "qlr")
					.source(XContentFactory.jsonBuilder().startObject()
							.field("bdcdyh", bdcdyh)
							.field("zjh",DataGenUtil.getIdCardNo())
							.field("uuid",BaseUtil.getUUid())
							.field("xm", RandomValue.getChineseName())
							//随机生成几个汉字，作为单位地址
							.field("dw", RandomValue.getRandomChineseString())
							.field("records", BaseUtil.getScopeInt(0, 1))
							.field("postDate", getRandomTime("2015-01-01", "2017-12-31")).endObject()));
			}
		}
		bulkProcessor.close();
		// bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
	}
	
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
	
	public static void readBdcdyhFile() throws Exception{
		File file = new File("d://bdcdyhFile");
		InputStream is = new FileInputStream(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		 String line = "";
		 while ((line = reader.readLine()) != null) {
//			 System.out.println(line);
//			 System.out.println(line.substring(0, 6));
			 bdcdyhlist.add(line);
		 }
	}
	public static void main(String[] args) throws Exception {
		TransportClient client = getClient1withNOxpack();
		readBdcdyhFile();
		bulkWriteMethodKeti(client);
		bulkWriteMethodqlr(client);
	}

}
