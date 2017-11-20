package com.inspur.bigdata.manage.es.zsjexample;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.KeyedFilter;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class QueryQlr {
	public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
	public static String clustername = "es";

	public static void main(String[] args) throws UnknownHostException, InterruptedException {

		TransportClient client = getClient1withNOxpack();

		// 查询场景：查询权利人，条件：权利人，查询权利人信息
		// getQyrsBYName(client, "闻珊宁");
		// 查询场景：查询权利人，条件：zjh，查询权利人信息
		// getQyrsBYZjh(client,"610722198110030371");

		// 查询场景：查询权利人，条件：权利人+工作单位，查询权利人信息
		//getQyrsBYNameAndDw(client, "松馨", "悲簿屿忘庞赞宠指爷毫杰辞");

		// 查询场景：查询出权利人前100条记录，条件：无条件;
		 getQyrsBYNone(client);

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
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.174"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.175"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.176"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.177"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.178"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.179"), 9300));

		return client;
	}

	// 查询场景：查询权利人，条件：权利人+工作单位，查询权利人信息
	public static void getQyrsBYNameAndDw(TransportClient client, String nm, String dw) throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息

		SearchRequestBuilder qlrSearchRB = client.prepareSearch("qlr_1y").setTypes("qlr_1y").setSize(100);

		BoolQueryBuilder qlrBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchPhraseQuery("xm", nm)).must(QueryBuilders.matchPhraseQuery("dw", dw))
				.must(QueryBuilders.termQuery("records", 0));

		SearchResponse qlrResponse = qlrSearchRB.setQuery(qlrBoolQueryQueryBuilder1).execute().actionGet();

		SearchHits qlrHits = qlrResponse.getHits();

		Set<String> zjSets = new HashSet<String>(100);

		for (int i = 0; i < qlrHits.getHits().length; i++) {
			zjSets.add(String.valueOf(qlrHits.getHits()[i].getSource().get("zjh")));
		}
		long getQlr100End = System.currentTimeMillis();
		System.out.println("get100Qlr cost :" + (getQlr100End - start));

		ExecutorService threadPool = Executors.newFixedThreadPool(100);

		System.out.println("zjhm nums is :" + qlrHits.getTotalHits());
		for (String zjh : zjSets) {

			threadPool.submit(() -> {

				long threadStart = System.currentTimeMillis();
				System.out.println(Thread.currentThread().getName() + "zjh:" + zjh + "start time :" + (threadStart));

				SearchRequestBuilder qlrTj = client.prepareSearch("qlr_1y").setTypes("qlr_1y").setSize(100);

				BoolQueryBuilder qlrTjBool = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("zjh", zjh))
						.filter(QueryBuilders.termQuery("records", "0"));

				SearchResponse qlrTjResponse = qlrTj.setQuery(qlrTjBool).execute().actionGet();

				SearchHits qlrTjHits = qlrTjResponse.getHits();

				System.out.println("zjh:" + zjh + ",total:" + qlrTjHits.getTotalHits());

				long tEnd = System.currentTimeMillis();
				System.out.println(
						Thread.currentThread().getName() + "zjh:" + zjh + "cost time :" + (tEnd - threadStart));

			});

		}

		threadPool.shutdown();

		while (true) {
			if (threadPool.isTerminated()) {
				long end = System.currentTimeMillis();

				System.out.println("权利人总条数" + qlrHits.getTotalHits() + ",权利人总耗时：" + (end - start) + "ms,");
				break;
			}

		}
		client.close();

	}

	// 查询场景：查询权利人，条件：zjh，查询权利人信息
	public static void getQyrsBYZjh(TransportClient client, String id) throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息

		SearchRequestBuilder qlrSearchRB = client.prepareSearch("qlr_1y").setTypes("qlr_1y").setSize(100);

		BoolQueryBuilder qlrBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchPhraseQuery("zjh", id)).must(QueryBuilders.termQuery("records", 0));

		SearchResponse qlrResponse = qlrSearchRB.setQuery(qlrBoolQueryQueryBuilder1).execute().actionGet();

		SearchHits qlrHits = qlrResponse.getHits();

		Set<String> zjSets = new HashSet<String>(100);

		for (int i = 0; i < qlrHits.getHits().length; i++) {
			zjSets.add(String.valueOf(qlrHits.getHits()[i].getSource().get("zjh")));
		}
		long getQlr100End = System.currentTimeMillis();
		System.out.println("get100Qlr cost :" + (getQlr100End - start));

		ExecutorService threadPool = Executors.newFixedThreadPool(100);

		System.out.println("zjhm nums is :" + qlrHits.getTotalHits());
		for (String zjh : zjSets) {

			threadPool.submit(() -> {

				long threadStart = System.currentTimeMillis();
				System.out.println(Thread.currentThread().getName() + "zjh:" + zjh + "start time :" + (threadStart));

				SearchRequestBuilder qlrTj = client.prepareSearch("qlr_1y").setTypes("qlr_1y").setSize(100);

				BoolQueryBuilder qlrTjBool = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("zjh", zjh))
						.filter(QueryBuilders.termQuery("records", "0"));

				SearchResponse qlrTjResponse = qlrTj.setQuery(qlrTjBool).execute().actionGet();

				SearchHits qlrTjHits = qlrTjResponse.getHits();

				System.out.println("zjh:" + zjh + ",total:" + qlrTjHits.getTotalHits());

				long tEnd = System.currentTimeMillis();
				System.out.println(
						Thread.currentThread().getName() + "zjh:" + zjh + "cost time :" + (tEnd - threadStart));

			});

		}

		threadPool.shutdown();

		while (true) {
			if (threadPool.isTerminated()) {
				long end = System.currentTimeMillis();

				System.out.println("权利人总条数" + qlrHits.getTotalHits() + ",权利人总耗时：" + (end - start) + "ms,");
				break;
			}

		}

	}

	// 查询场景：查询权利人，条件：权利人，查询权利人信息
	public static void getQyrsBYName(TransportClient client, String name) throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息

		SearchRequestBuilder qlrSearchRB = client.prepareSearch("qlr_1y").setTypes("qlr_1y").setSize(100);

		BoolQueryBuilder qlrBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchPhraseQuery("xm", name)).filter(QueryBuilders.termQuery("records", 0));

		SearchResponse qlrResponse = qlrSearchRB.setQuery(qlrBoolQueryQueryBuilder1).execute().actionGet();

		SearchHits qlrHits = qlrResponse.getHits();

		Set<String> zjSets = new HashSet<String>(100);

		for (int i = 0; i < qlrHits.getHits().length; i++) {
			zjSets.add(String.valueOf(qlrHits.getHits()[i].getSource().get("zjh")));
		}
		long getQlr100End = System.currentTimeMillis();
		System.out.println("100条权利人耗时:" + (getQlr100End - start));

		// int jhTime = zjSets.size();
		// KeyedFilter[] fAs = new KeyedFilter[jhTime];
		//
		// int j = 0;
		// for (String zjh : zjSets) {
		//
		// fAs[j++] = new FiltersAggregator.KeyedFilter(zjh,
		// QueryBuilders.termQuery("zjh", zjh));
		// }
		//
		// getJh(client, fAs);

		ExecutorService threadPool = Executors.newFixedThreadPool(100);

		System.out.println("zjhm nums is :" + qlrHits.getTotalHits());
		for (String zjh : zjSets) {

			threadPool.submit(() -> {

				long threadStart = System.currentTimeMillis();
				System.out.println(Thread.currentThread().getName() + "zjh:" + zjh + "start time :" + (threadStart));

				SearchRequestBuilder qlrTj = client.prepareSearch("qlr_1y").setTypes("qlr_1y").setSize(100);

				BoolQueryBuilder qlrTjBool = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("zjh", zjh));

				SearchResponse qlrTjResponse = qlrTj.setQuery(qlrTjBool).execute().actionGet();

				SearchHits qlrTjHits = qlrTjResponse.getHits();

				System.out.println("zjh:" + zjh + ",total:" + qlrTjHits.getTotalHits());

				long tEnd = System.currentTimeMillis();
				System.out.println(
						Thread.currentThread().getName() + "zjh:" + zjh + "cost time :" + (tEnd - threadStart));

			});

		}

		threadPool.shutdown();

		while (true) {
			if (threadPool.isTerminated()) {
				long end = System.currentTimeMillis();

				System.out.println("权利人总条数" + qlrHits.getTotalHits() + ",权利人总耗时：" + (end - start) + "ms,");
				break;
			}

		}

		// long end = System.currentTimeMillis();
		//
		// System.out.println("权利人总条数" + qlrHits.getTotalHits() + ",权利人总耗时：" +
		// (end - start) + "ms,");

	}

	// 查询场景：查询出权利人前100条记录，条件：无条件
	public static void getQyrsBYNone(TransportClient client) throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息

		SearchRequestBuilder qlrSearchRB = client.prepareSearch("qlr_1y").setTypes("qlr_1y").setSize(100);

		BoolQueryBuilder qlrBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("records", 0));

		SearchResponse qlrResponse = qlrSearchRB.setQuery(qlrBoolQueryQueryBuilder1).execute().actionGet();

		SearchHits qlrHits = qlrResponse.getHits();

		Set<String> zjSets = new HashSet<String>(100);

		for (int i = 0; i < qlrHits.getHits().length; i++) {
			zjSets.add(String.valueOf(qlrHits.getHits()[i].getSource().get("zjh")));
		}
		long getQlr100End = System.currentTimeMillis();
		System.out.println("get100Qlr cost :" + (getQlr100End - start));
		// int jhTime = zjSets.size();
		// KeyedFilter[] fAs = new KeyedFilter[jhTime];
		//
		// int j = 0;
		// for (String zjh : zjSets) {
		//
		// fAs[j++] = new FiltersAggregator.KeyedFilter(zjh,
		// QueryBuilders.termQuery("zjh", zjh));
		// }

		// getJh(client, fAs);

		ExecutorService threadPool = Executors.newFixedThreadPool(100);

		for (String zjh : zjSets) {

			threadPool.submit(() -> {

				long threadStart = System.currentTimeMillis();
				System.out.println(Thread.currentThread().getName() + "zjh:" + zjh + "start time :" + (threadStart));

				SearchRequestBuilder qlrTj = client.prepareSearch("qlr_1y").setTypes("qlr_1y").setSize(100);

				BoolQueryBuilder qlrTjBool = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("zjh", zjh))
						.filter(QueryBuilders.termQuery("records", "0"));

				SearchResponse qlrTjResponse = qlrTj.setQuery(qlrTjBool).execute().actionGet();

				SearchHits qlrTjHits = qlrTjResponse.getHits();

				System.out.println("zjh:" + zjh + ",total:" + qlrTjHits.getTotalHits());

				long tEnd = System.currentTimeMillis();
				System.out.println(
						Thread.currentThread().getName() + "zjh:" + zjh + "cost time :" + (tEnd - threadStart));

			});

		}

		threadPool.shutdown();

		while (true) {
			if (threadPool.isTerminated()) {
				long end = System.currentTimeMillis();

				System.out.println("权利人总条数" + qlrHits.getTotalHits() + ",权利人总耗时：" + (end - start) + "ms,");
				break;
			}

		}

	}

	public static void getJh(TransportClient client, KeyedFilter[] kfs) throws UnknownHostException {

		long start = System.currentTimeMillis();

		AggregationBuilder gradeTermsBuilder = AggregationBuilders.filters("qlrtj", kfs);

		SearchRequestBuilder srb = client.prepareSearch("qlr").setTypes("qlr").setSize(100);
		srb.addAggregation(gradeTermsBuilder);

		SearchResponse sr = srb.execute().actionGet();

		Filters agg = sr.getAggregations().get("qlrtj");

		long totalAll = 0;
		for (Filters.Bucket entry : agg.getBuckets()) {
			String key = entry.getKeyAsString();
			long docCount = entry.getDocCount();
			System.out.println("zjh" + key + ",count:" + docCount);
			totalAll += docCount;
		}

		long end = System.currentTimeMillis();
		System.out.println("聚合总共条数：" + totalAll + ",聚合总耗时：" + (end - start) + "ms");

	}

}
