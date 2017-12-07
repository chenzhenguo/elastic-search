package com.inspur.bigdata.manage.es.zsjexample;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

enum Config {
//	INDEX_KETI("keti1_5"), INDEX_QLR("qlr1_5"), INDEX_QL("ql1_5"), TYPE_KETI("keti1_5"), TYPE_QLR("qlr1_5"), TYPE_QL(
//			"ql1_5");

	 INDEX_KETI("keti1_10"), INDEX_QLR("qlr1_10"), INDEX_QL("ql1_10"),
	 TYPE_KETI("keti1_10"), TYPE_QLR(
	 "qlr1_10"), TYPE_QL("ql1_10");
	private String content;

	Config(String content) {
		this.content = content;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}

public class QueryKeti {
	public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
	public static String clustername = "es";

	public static void main(String[] args) throws UnknownHostException, InterruptedException {

		TransportClient client = getClient1withNOxpack();
		// 查询场景：查询出客体前100条记录，无条件，根据每条客体记录的不动产单元号，获取权利人 毫秒
		//getQyrsBYNone(client);

		// 查询场景：查询出客体前100条记录，条件：不动产单元号(坐落)，根据每条客体记录的不动产单元号获取权利人 毫秒
		 getQyrsByBdcdyhOrZl(client, "zl","安丘");
		// getQyrsByBdcdyhOrZl(client,
		// "bdcdyh","210500588529GB97927F654278671");

		// 查询出客体前100条记录，条件：坐落+行政区划，根据每条客体记录的不动产单元号，获取权利人 毫秒
		// getQlrByZlXzqh(client, "涞源", "130630");
		client.close();

	}

	/****
	 * 获取es客户端
	 * 
	 * @return TransportClient
	 * @throws UnknownHostException
	 */
	public static TransportClient getClient1withNOxpack() throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", clustername).build();
		TransportClient client = new PreBuiltTransportClient(settings);
		// client.addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
		// .addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.18.130"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.18.131"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.18.132"), 9300));

		return client;
	}

	/******
	 * 查询出客体前100条记录，条件：坐落+行政区划，根据每条客体记录的不动产单元号，获取权利人 毫秒
	 */
	public static void getQlrByZlXzqh(TransportClient client, String zl, String xzqh) throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息

		SearchRequestBuilder ketiSearchRB = client.prepareSearch(Config.INDEX_KETI.getContent())
				.setTypes(Config.TYPE_KETI.getContent()).setSize(100);

		BoolQueryBuilder ketiBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("records", "0")).filter(QueryBuilders.termsQuery("zl", zl))
				.filter(QueryBuilders.termsQuery("qx", xzqh));

		SearchResponse ketiResponse = ketiSearchRB.setQuery(ketiBoolQueryQueryBuilder1).execute().actionGet();

		SearchHits ketiHits = ketiResponse.getHits();

		long ketiEnd = System.currentTimeMillis();

		long cost = ketiEnd - start;

		System.out.println("keti costtime:" + cost + "ms");

		List<String> bdcdyhList = new ArrayList<String>();
		for (int i = 0; i < ketiHits.getHits().length; i++) {
			bdcdyhList.add(String.valueOf(ketiHits.getHits()[i].getSource().get("bdcdyh")));

		}

		BoolQueryBuilder qlrBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("records", "0")).filter(QueryBuilders.termsQuery("bdcdyh", bdcdyhList));

		SearchRequestBuilder qlrRb = client.prepareSearch(Config.INDEX_QLR.getContent())
				.setTypes(Config.TYPE_QLR.getContent()).setSize(100);
		SearchResponse response1 = qlrRb.setQuery(qlrBoolQueryQueryBuilder1).execute().actionGet();
		SearchHits hits1 = response1.getHits();
		long qlrCount = hits1.getTotalHits();
		for (int j = 0; j < hits1.getHits().length; j++) {
			// System.out.print("\txm:" +
			// hits1.getHits()[j].getSource().get("xm") + "\tbdc:"
			// + hits1.getHits()[j].getSource().get("bdcdyh") + "\tzjh:"
			// + hits1.getHits()[j].getSource().get("zjh") + "\trecords:"
			// + hits1.getHits()[j].getSource().get("records") + "\n");
		}

		long end = System.currentTimeMillis();

		System.out.println("客体总条数" + ketiHits.getTotalHits() + ",总耗时：" + (end - start) + "ms," + qlrCount
				+ "条 qlr cost:" + (end - ketiEnd));
	}

	/****
	 * 查询场景：查询出客体前100条记录，条件：不动产单元号(或坐落)，根据每条客体记录的不动产单元号获取权利人 毫秒
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param size
	 * @throws UnknownHostException
	 */
	public static void getQyrsByBdcdyhOrZl(TransportClient client, String key, String paramV)
			throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息

		SearchRequestBuilder ketiSearchRB = client.prepareSearch(Config.INDEX_KETI.getContent())
				.setTypes(Config.TYPE_KETI.getContent()).setSize(100);

		// SearchResponse response =
		// client.prepareSearch(index).setTypes(type).setSize(size).execute().actionGet();

		BoolQueryBuilder ketiBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("records", "0")).filter(QueryBuilders.termsQuery(key, paramV));

		SearchResponse ketiResponse = ketiSearchRB.setQuery(ketiBoolQueryQueryBuilder1).execute().actionGet();

		SearchHits ketiHits = ketiResponse.getHits();

		long ketiEnd = System.currentTimeMillis();

		long cost = ketiEnd - start;

		System.out.println("keti costtime:" + cost + "ms");

		List<String> bdcdyhList = new ArrayList<String>();
		for (int i = 0; i < ketiHits.getHits().length; i++) {
			bdcdyhList.add(String.valueOf(ketiHits.getHits()[i].getSource().get("bdcdyh")));

		}

		BoolQueryBuilder qlrBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("records", "0")).filter(QueryBuilders.termsQuery("bdcdyh", bdcdyhList));

		SearchRequestBuilder qlrRb = client.prepareSearch(Config.INDEX_QLR.getContent())
				.setTypes(Config.TYPE_QLR.getContent()).setSize(100);
		SearchResponse response1 = qlrRb.setQuery(qlrBoolQueryQueryBuilder1).execute().actionGet();
		SearchHits hits1 = response1.getHits();
		long qlrCount = hits1.getTotalHits();
		for (int j = 0; j < hits1.getHits().length; j++) {
			System.out.print("\txm:" + hits1.getHits()[j].getSource().get("xm") + "\tbdc:"
					+ hits1.getHits()[j].getSource().get("bdcdyh") + "\tzjh:"
					+ hits1.getHits()[j].getSource().get("zjh") + "\trecords:"
					+ hits1.getHits()[j].getSource().get("records") + "\n");
		}

		long end = System.currentTimeMillis();

		System.out.println("客体总条数" + ketiHits.getTotalHits() + ",总耗时：" + (end - start) + "ms," + qlrCount
				+ "条 qlr cost:" + (end - ketiEnd));

	}

	public static void getQyrsBYNone(TransportClient client) throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息

		SearchRequestBuilder ketiSearchRB = client.prepareSearch(Config.INDEX_KETI.getContent())
				.setTypes(Config.TYPE_KETI.getContent()).setSize(100);

		// SearchResponse response =
		// client.prepareSearch(index).setTypes(type).setSize(size).execute().actionGet();

		BoolQueryBuilder ketiBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("records", 0));

		SearchResponse ketiResponse = ketiSearchRB.setQuery(ketiBoolQueryQueryBuilder1).execute().actionGet();

		SearchHits ketiHits = ketiResponse.getHits();

		long ketiEnd = System.currentTimeMillis();

		long cost = ketiEnd - start;

		System.out.println("keti costtime:" + cost + "ms");

		List<String> bdcdyhList = new ArrayList<String>();
		for (int i = 0; i < ketiHits.getHits().length; i++) {
			bdcdyhList.add(String.valueOf(ketiHits.getHits()[i].getSource().get("bdcdyh")));

		}

		BoolQueryBuilder qlrBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("records", 0)).must(QueryBuilders.termsQuery("bdcdyh", bdcdyhList));

		SearchRequestBuilder qlrRb = client.prepareSearch(Config.INDEX_QLR.getContent())
				.setTypes(Config.INDEX_QLR.getContent()).setSize(100);
		SearchResponse response1 = qlrRb.setQuery(qlrBoolQueryQueryBuilder1).execute().actionGet();
		SearchHits hits1 = response1.getHits();
		long qlrCount = hits1.getTotalHits();
		for (int j = 0; j < hits1.getHits().length; j++)
			;

		long end = System.currentTimeMillis();

		System.out.println("客体总条数" + ketiHits.getTotalHits() + ",总耗时：" + (end - start) + "ms," + qlrCount
				+ "条 qlr cost:" + (end - ketiEnd));

	}

}
