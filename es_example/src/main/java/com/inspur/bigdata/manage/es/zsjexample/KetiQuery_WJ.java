package com.inspur.bigdata.manage.es.zsjexample;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KetiQuery_WJ {
	public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
	public static String hostname = "10.110.13.176";
	// public static String hostname = "localhost";
	public static String clustername = "es";
	public static String index = "test001";
	public static String type = "type001";
	public static ObjectMapper mapper = new ObjectMapper();

	public static void main(String[] args) throws UnknownHostException {

		TransportClient client = getClient1withNOxpack();
		// 通过坐落查询统计相关信息
		queryByZl(client);

		// 查询指定的不动产单元号列表，批量查询出权利人相关信息
		// getQyrsByBdcdyhList(client);

		client.close();

	}

	/***
	 * 查询坐落 模糊查询
	 * 
	 * @param client
	 * @param count
	 * @param leixing
	 * @throws UnknownHostException
	 */
	public static void queryByZl(TransportClient client) throws UnknownHostException {
		SearchRequestBuilder srb = client.prepareSearch("keti2").setTypes("keti2").setSize(100);

		MatchPhraseQueryBuilder boolQueryQueryBuilder = QueryBuilders.matchPhraseQuery("zl", "东城");

		// MatchQueryBuilder boolQueryBuilder= QueryBuilders.matchQuery("zl",
		// "中");

		long start = System.currentTimeMillis();
		SearchResponse response = srb.setQuery(boolQueryQueryBuilder).execute().actionGet();

		SearchHits hits = response.getHits();

		System.out.println("总共有" + hits.getTotalHits() + "条记录");

		for (int i = 0; i < hits.getHits().length; i++) {
			System.out.print("\t_id:" + hits.getHits()[i].getSource().get("_id"));
			System.out.print("\tqx:" + hits.getHits()[i].getSource().get("qx"));
			System.out.print("\tzl:" + hits.getHits()[i].getSource().get("zl"));
			System.out.println("");
		}

		long end = System.currentTimeMillis();

		System.out.println("总耗时：" + ((end - start)) + "豪秒");

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
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9300));
		return client;
	}

	/**
	 * 客体和权利人 不动产单元号 行政区划 展示包含权利人、证件号 bdcdyh:130209109640GB04190F344126562
	 * 
	 * @param client
	 * @throws UnknownHostException
	 */
	public static void getRecord0ByBdcdyh(TransportClient client, String bdcdyh) throws UnknownHostException {
		long a1 = Calendar.getInstance().getTimeInMillis();
		SearchRequestBuilder responsebuilder = client.prepareSearch("keti").setTypes("keti");
		SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");

		// term查不出来，估计是分词了
		// TermQueryBuilder boolQueryQueryBuilder =
		// QueryBuilders.termQuery("bdcdyh", "620522865057GB22282F729085473");
		MatchQueryBuilder boolQueryQueryBuilder = QueryBuilders.matchQuery("bdcdyh", bdcdyh);
		SearchResponse response = responsebuilder.setQuery(boolQueryQueryBuilder).execute().actionGet();
		SearchHits hits = response.getHits();
		// System.out.println("客体："+hits.getTotalHits());
		int temp = 0;
		for (int i = 0; i < hits.getHits().length; i++) {
			System.out.print("bdcdyh:" + hits.getHits()[i].getSource().get("bdcdyh"));
			System.out.print("\tlx:" + hits.getHits()[i].getSource().get("lx"));
			System.out.print("\tuuid:" + hits.getHits()[i].getSource().get("uuid"));
			System.out.print("\tqx:" + hits.getHits()[i].getSource().get("qx"));
			System.out.print("\tzl:" + hits.getHits()[i].getSource().get("zl"));
			System.out.print("\trecords:" + hits.getHits()[i].getSource().get("records"));
			System.out.print("\tpostDate:" + hits.getHits()[i].getSource().get("postDate") + "\n");

			BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
					.must(QueryBuilders.matchQuery("bdcdyh", bdcdyh)).must(QueryBuilders.termQuery("records", "0"));
			SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1).execute().actionGet();
			SearchHits hits1 = response1.getHits();
			// System.out.println("权利人："+hits1.getTotalHits());
			for (int j = 0; j < hits1.getHits().length; j++) {
				System.out.print("\txm:" + hits1.getHits()[j].getSource().get("xm"));
				System.out.print("\tzjh:" + hits1.getHits()[j].getSource().get("zjh"));
				System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records"));
			}
			System.out.println("");
		}
		long a2 = Calendar.getInstance().getTimeInMillis();
		System.out.println("    2:" + formatDate.format(new Date()));
	}

	/****
	 * 查询指定的不动产单元号列表，批量查询出权利人相关信息
	 * 
	 * @param client
	 * @throws UnknownHostException
	 */
	public static void getQyrsByBdcdyhList(TransportClient client) throws UnknownHostException {

		// 首先获取课题的100条信息
		SearchResponse response = client.prepareSearch("keti1").setTypes("keti1").setSize(100).execute().actionGet();
		SearchHits hits = response.getHits();

		// 组装不动产编号
		List<String> bdcList = new ArrayList<String>(100);
		for (int i = 0; i < hits.getHits().length; i++)
			bdcList.add(String.valueOf(hits.getHits()[i].getSource().get("bdcdyh")));

		// 将一批
		String[] bdcs = new String[100];
		TermsQueryBuilder termsQueryBdcs = QueryBuilders.termsQuery("bdcdyh", bdcList.toArray(bdcs));

		SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr1").setTypes("qlr1");

		SearchResponse response1 = responsebuilder1.setQuery(termsQueryBdcs).execute().actionGet();
		SearchHits hits1 = response1.getHits();

		for (int i = 0; i < hits1.getHits().length; i++) {
			System.out.println(String.valueOf(hits.getHits()[i].getSource().get("bdcdyh")));
		}

	}

	public static void noconditionQueryKetiqlrWithSystem(TransportClient client) throws UnknownHostException {
		SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");
		SearchResponse response = client.prepareSearch("keti").setTypes("keti").setSize(50).execute().actionGet();
		SearchHits hits = response.getHits();
		for (int i = 0; i < hits.getHits().length; i++) {
			String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
			System.out.print("bdcdyh:" + hits.getHits()[i].getSource().get("bdcdyh"));
			System.out.print("\tlx:" + hits.getHits()[i].getSource().get("lx"));
			System.out.print("\tuuid:" + hits.getHits()[i].getSource().get("uuid"));
			System.out.print("\tqx:" + hits.getHits()[i].getSource().get("qx"));
			System.out.print("\tzl:" + hits.getHits()[i].getSource().get("zl"));
			System.out.print("\trecords:" + hits.getHits()[i].getSource().get("records"));
			System.out.print("\tpostDate:" + hits.getHits()[i].getSource().get("postDate") + "\n");
			BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
					.must(QueryBuilders.matchQuery("bdcdyh", bdcdyh)).must(QueryBuilders.termQuery("records", "0"));
			SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1).execute().actionGet();
			SearchHits hits1 = response1.getHits();
			for (int j = 0; j < hits1.getHits().length; j++) {
				System.out.print("\txm:" + hits1.getHits()[j].getSource().get("xm"));
				System.out.print("\tzjh:" + hits1.getHits()[j].getSource().get("zjh"));
				System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records"));
			}
			System.out.println("");
		}
	}

	// 最简单没条件查询，查询某个索引下的某个类型
	public static void simpleQueryWithoutMethodqlr(TransportClient client) throws UnknownHostException {
		// long a1 = Calendar.getInstance().getTimeInMillis();
		SearchResponse response = client.prepareSearch("qlr").setTypes("qlr").setSize(100).execute().actionGet();
		SearchHits hits = response.getHits();
		System.out.println("获取记录：" + hits.getTotalHits() + "条");
		int temp = 0;
		for (int i = 0; i < hits.getHits().length; i++) {
			System.out.print(hits.getHits()[i].getSource().get("bdcdyh"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("zjh"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("uuid"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("xm"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("dw"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("records"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("postDate") + "\n");
		}
		// long a2 = Calendar.getInstance().getTimeInMillis();
		// System.out.println(String.valueOf(a2-a1).concat("毫秒"));
		// System.out.println("---------------------------------------------------------");
	}

	// 最简单没条件查询，查询某个索引下的某个类型
	public static void simpleQueryWithoutMethodKeti(TransportClient client) throws UnknownHostException {
		// long a1 = Calendar.getInstance().getTimeInMillis();
		SearchResponse response = client.prepareSearch("keti1").setTypes("keti1").setSize(10).execute().actionGet();
		SearchHits hits = response.getHits();
		System.out.println("获取记录：" + hits.getTotalHits() + "条");
		int temp = 0;
		for (int i = 0; i < hits.getHits().length; i++) {
			System.out.print(hits.getHits()[i].getSource().get("bdcdyh"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("lx"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("uuid"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("qx"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("zl"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("records"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("postDate") + "\n");
		}
		// long a2 = Calendar.getInstance().getTimeInMillis();
		// System.out.println(String.valueOf(a2-a1).concat("毫秒"));
		// System.out.println("---------------------------------------------------------");
	}

	public static void queryKetiWithLeixing(TransportClient client, int count, String leixing)
			throws UnknownHostException {
		SearchRequestBuilder responsebuilder = client.prepareSearch("keti1").setTypes("keti1");
		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("lx", leixing)).filter(QueryBuilders.termQuery("records", "0"));
		SearchResponse response = responsebuilder.setQuery(boolQueryQueryBuilder1).setSize(count).execute().actionGet();
		SearchHits hits = response.getHits();
		for (int i = 0; i < hits.getHits().length; i++) {
			System.out.print(hits.getHits()[i].getSource().get("bdcdyh"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("lx"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("uuid"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("qx"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("zl"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("records"));
			System.out.print("\t" + hits.getHits()[i].getSource().get("postDate") + "\n");
		}
	}

	public static void queryKetiCountWithLeixing(TransportClient client, int count, String leixing)
			throws UnknownHostException {
		SearchRequestBuilder responsebuilder = client.prepareSearch("keti1").setTypes("keti1");
		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("lx", leixing)).filter(QueryBuilders.termQuery("records", "0"));
		SearchResponse response = responsebuilder.setQuery(boolQueryQueryBuilder1).setSize(count).execute().actionGet();
		SearchHits hits = response.getHits();
		//
		// System.out.print( hits.getHits().length + "\n");
		long length = response.getHits().getTotalHits();
		System.out.print("总共记录:".concat(String.valueOf(length) + "条\n"));

	}

	/**
	 * 按地区统计客体数量
	 * 
	 * @param client
	 * @param count
	 * @param leixing
	 * @throws UnknownHostException
	 */
	public static void andiqutongjiketishuliang(TransportClient client, int count, String leixing)
			throws UnknownHostException {
		// SearchRequestBuilder srb =
		// client.prepareSearch("keti1").setTypes("keti1").setSize(100);
		SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1");
		TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qxtj").field("qx").size(4000);
		// ValueCountAggregationBuilder gradeTermsBuilder =
		// AggregationBuilders.count("gradeAgg").field("qx");
		srb.addAggregation(gradeTermsBuilder);
		//
		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("records", "0"));

		SearchResponse sr = srb.setQuery(boolQueryQueryBuilder1).execute().actionGet();
		Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
		StringTerms gradeTerms = (StringTerms) aggMap.get("qxtj");
		Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
		long a = 0;
		while (gradeBucketIt.hasNext()) {
			StringTerms.Bucket gradeBucket = gradeBucketIt.next();
			System.out.println(gradeBucket.getKey() + "地区有" + gradeBucket.getDocCount() + "条记录。");
			a = a + gradeBucket.getDocCount();
		}
		System.out.println("总共记录条数：" + String.valueOf(a));

	}

}
