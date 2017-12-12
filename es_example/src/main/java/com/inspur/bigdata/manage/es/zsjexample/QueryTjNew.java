package com.inspur.bigdata.manage.es.zsjexample;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class QueryTjNew {

	// static String ketiIndexName = "keti10yi";
	// static String ketiTypeName = "keti10yi";
	// static String ketiIndexName="keti10_5";
	// static String ketiTypeName="keti10_5";
	static String ketiIndexName = "keti10_10";
	static String ketiTypeName = "keti10_10";

	// static String qlIndexName = "ql10yi";
	// static String qlTypeName = "ql10yi";
	// static String qlIndexName="ql10_5";
	// static String qlTypeName="ql10_5";
	static String qlIndexName = "ql10_10";
	static String qlTypeName = "ql10_10";

	// static String qlrIndexName = "qlr10yi";
	// static String qlrTypeName = "qlr10yi";
	// static String qlrIndexName="qlr10_5";
	// static String qlrTypeName="qlr10_5";
	static String qlrIndexName = "qlr10_10";
	static String qlrTypeName = "qlr10_10";

	public static void main(String[] args) throws UnknownHostException {

		TransportClient client = getClient1withNOxpack();

		// 场景1：按照省统计不动产数量
		// tiKetiNmByProvinceMul(client);
		// 场景2：按照权利类型统计权利数量
		// tjByQllxByMul(client);
		// 场景3：按照省和权利人类型统计权利人数量（按照权利人类型）
		tjKetiNumLeixingGroupByProvince(client);
		// 场景4：按照省统计不动产证和不动产登记号的数量

	}

	public static TransportClient getClient1withNOxpack() throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", "es").build();
		TransportClient client = new PreBuiltTransportClient(settings);
		// client.addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
		// .addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.6"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.7"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.8"), 9300));

		return client;
	}

	// 场景1：按照省统计 客体数量
	@Deprecated
	private static void tiKetiNmByProvinceMulti(TransportClient client) throws UnknownHostException {

		long start = System.currentTimeMillis();

		ExecutorService threadPool = Executors.newFixedThreadPool(34);
		for (String qx : StaticValues.getShenglist()) {

			threadPool.submit(() -> {

				KeyedFilter fa = new FiltersAggregator.KeyedFilter(qx, QueryBuilders.prefixQuery("qx", qx));
				AggregationBuilder gradeTermsBuilder = AggregationBuilders.filters("ketitj", fa);
				
				SearchRequestBuilder srb = client.prepareSearch(ketiIndexName).setTypes(ketiTypeName).setSize(100);
				srb.addAggregation(gradeTermsBuilder);

				SearchResponse sr = srb.execute().actionGet();

				Filters agg = sr.getAggregations().get("ketitj");

				for (Filters.Bucket entry : agg.getBuckets()) {
					String key = entry.getKeyAsString();
					long docCount = entry.getDocCount();
					System.out.println("省" + key + ",count:" + docCount);
				}

			});

		}
		threadPool.shutdown();

		while (true) {
			if (threadPool.isTerminated()) {
				long endTime = System.currentTimeMillis();
				System.out.println("cost time:" + ((endTime - start) / 1000) + "s");
				break;
			}

		}

	}

	// 场景1：按照省统计 客体数量
	private static void tiKetiNmByProvinceMul(TransportClient client) throws UnknownHostException {

		long start = System.currentTimeMillis();

		ExecutorService threadPool = Executors.newFixedThreadPool(34);
		CountDownLatch singnal = new CountDownLatch(34);
		for (String qx : StaticValues.getShenglist()) {

			threadPool.submit(() -> {

				BoolQueryBuilder ketiBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
						.filter(QueryBuilders.termQuery("records", 0)).must(QueryBuilders.prefixQuery("qx", qx));

				SearchRequestBuilder srb = client.prepareSearch(ketiIndexName).setTypes(ketiTypeName).setSize(100);
				srb.setQuery(ketiBoolQueryQueryBuilder1);

				SearchResponse sr = srb.execute().actionGet();
				SearchHits hits = sr.getHits();

				System.out.println("省" + qx + ",有" + hits.getTotalHits());
				singnal.countDown();

			});

		}
		threadPool.shutdown();
		while (true) {
			try {
				singnal.await();
				long endTime = System.currentTimeMillis();
				System.out.println("cost time:" + (endTime - start) + "ms");
				break;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	// 场景1 按照省统计客体数量
	@Deprecated
	private static void tiKetiNmByProvinceSingle(TransportClient client) throws UnknownHostException {

		long start = System.currentTimeMillis();

		KeyedFilter[] fas = new KeyedFilter[StaticValues.getShenglist().size()];

		int i = 0;
		for (String qx : StaticValues.getShenglist()) {

			fas[i++] = new FiltersAggregator.KeyedFilter(qx, QueryBuilders.prefixQuery("qx", qx));
		}

		AggregationBuilder gradeTermsBuilder = AggregationBuilders.filters("ketitj", fas);

		SearchRequestBuilder srb = client.prepareSearch(ketiIndexName).setTypes(ketiTypeName).setSize(100);
		srb.addAggregation(gradeTermsBuilder);

		SearchResponse sr = srb.execute().actionGet();

		Filters agg = sr.getAggregations().get("ketitj");

		for (Filters.Bucket entry : agg.getBuckets()) {
			String key = entry.getKeyAsString();
			long docCount = entry.getDocCount();
			System.out.println("省" + key + ",count:" + docCount);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("cost time:" + ((endTime - start) / 1000) + "s");

	}

	// 场景2：按照权利类型统计
	@Deprecated
	public static void tjByQllx(TransportClient client) throws UnknownHostException {

		long startTime = System.currentTimeMillis();

		AggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qllxtj").field("qllx");

		BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("records", 0));

		SearchRequestBuilder srb = client.prepareSearch(qlIndexName).setTypes(qlTypeName).setSize(100);
		srb.addAggregation(gradeTermsBuilder);
		srb.setQuery(boolQueryQueryBuilder);

		SearchResponse sr = srb.execute().actionGet();

		Terms genders = sr.getAggregations().get("qllxtj");

		for (Terms.Bucket entry : genders.getBuckets()) {
			System.out.println("lx:" + entry.getKey() + ",nums:" + entry.getDocCount());
		}

		long endTime = System.currentTimeMillis();
		System.out.println("总耗时:" + (endTime - startTime) + "ms");

	}

	// 场景2：按照权利类型统计
	public static void tjByQllxByMul(TransportClient client) throws UnknownHostException {

		long startTime = System.currentTimeMillis();

		BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("records", 0));

		SearchRequestBuilder srb = client.prepareSearch(qlIndexName).setTypes(qlTypeName);

		SearchResponse sr = srb.setQuery(boolQueryQueryBuilder).execute().actionGet();

		SearchHits hits = sr.getHits();

		Set<String> qllxs = new HashSet<String>();

		for (int i = 0; i < hits.getHits().length; i++) {
			qllxs.add(hits.getHits()[i].getSource().get("qllx").toString());
		}

		ExecutorService threadPool = Executors.newFixedThreadPool(qllxs.size());

		CountDownLatch count = new CountDownLatch(qllxs.size());
		for (String qllx : qllxs) {
			threadPool.submit(() -> {

				BoolQueryBuilder queryExp = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", 0))
						.must(QueryBuilders.termQuery("qllx", qllx));

				SearchRequestBuilder srbu = client.prepareSearch(qlIndexName).setTypes(qlTypeName);
				SearchResponse resultResponse = srbu.setQuery(queryExp).execute().actionGet();

				SearchHits qlhits = resultResponse.getHits();
				System.out.println("qllx:" + qllx + ",num:" + qlhits.getTotalHits());
				count.countDown();

			});
		}

		while (true) {
			try {
				count.await();
				long endTime = System.currentTimeMillis();
				System.out.println("cost time:" + (endTime - startTime) + "ms");
				break;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	// 场景3： 按照省和权利人类型统计权利人数量
	public static void tjKetiNumLeixingGroupByProvince(TransportClient client) {

		List<String> list = StaticValues.getShenglist();

		ExecutorService threadPool = Executors.newFixedThreadPool(34);
		long startTime = System.currentTimeMillis();

		CountDownLatch count = new CountDownLatch(34);
		for (String sheng : list) {

			threadPool.submit(() -> {
				AggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qlrlxtj").field("lx");

				BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery()
						.filter(QueryBuilders.termQuery("records", 0)).filter(QueryBuilders.prefixQuery("qx", sheng));

				SearchRequestBuilder srb = client.prepareSearch(qlrIndexName).setTypes(qlrTypeName).setSize(100);
				srb.addAggregation(gradeTermsBuilder);
				srb.setQuery(boolQueryQueryBuilder);

				SearchResponse sr = srb.execute().actionGet();

				Terms genders = sr.getAggregations().get("qlrlxtj");

				System.out.println("省：" + sheng);
				for (Terms.Bucket entry : genders.getBuckets()) {
					System.out.println("lx:" + entry.getKey() + ",nums:" + entry.getDocCount());
				}
				count.countDown();
			});

		}
		threadPool.shutdown();

		while (true) {
			try {
				count.await();
				long endTime = System.currentTimeMillis();
				System.out.println("cost time:" + (endTime - startTime) + "ms");
				break;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	// 场景3： 按照省和权利人类型统计权利人数量
	@Deprecated
	public static void tjKetiNumLeixingGroupByProvinceMUl(TransportClient client) {

		List<String> list = StaticValues.getShenglist();

		long startTime = System.currentTimeMillis();

		BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("records", 0));

		SearchRequestBuilder srb = client.prepareSearch(qlrIndexName).setTypes(qlrTypeName);
		srb.setQuery(boolQueryQueryBuilder);

		SearchResponse sr = srb.execute().actionGet();

		SearchHits qlrhits = sr.getHits();
		int hitsNm = qlrhits.getHits().length;

		Set<String> lxs = new HashSet<String>();
		for (int i = 0; i < hitsNm; i++) {
			lxs.add(String.valueOf(qlrhits.getHits()[i].getSource().get("lx")));
		}

		ExecutorService threadPool = Executors.newFixedThreadPool(34);
		for (String sheng : list) {

			threadPool.submit(() -> {
				System.out.println("省：" + sheng);
				for (String lx : lxs) {
					SearchRequestBuilder lxsrb = client.prepareSearch(qlrIndexName).setTypes(qlrTypeName);

					BoolQueryBuilder lxBQB = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", 0))
							.filter(QueryBuilders.prefixQuery("qx", sheng)).filter(QueryBuilders.prefixQuery("lx", lx));

					SearchResponse lxsr = lxsrb.setQuery(lxBQB).execute().actionGet();

					SearchHits lxhits = lxsr.getHits();
					System.out.println("省：" + sheng + ",lx:" + lx + ",num:" + lxhits.getTotalHits());
				}

			});

		}
		threadPool.shutdown();

		while (true) {
			if (threadPool.isTerminated()) {
				long endTime = System.currentTimeMillis();
				System.out.println("cost time:" + (endTime - startTime) + "ms");
				break;
			}

		}

	}

}
