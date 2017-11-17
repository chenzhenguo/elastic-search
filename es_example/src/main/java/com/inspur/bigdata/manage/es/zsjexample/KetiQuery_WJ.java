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

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
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
		// queryByZl(client);

		// 查询指定的不动产单元号列表，批量查询出权利人相关信息
		// getQyrsByBdcdyhList(client);

		// 根据lx和record查询记录
		// chaXunKtByRecordLx(client,"keti3","keti3",500,"10","0");
		// chaXunKtByRecordLx(client,"keti2","keti2",100,"10","0");

		// 查询权利人通过证件号
		// chaXunQlrByQlrNm(client,"qlr","qlr","530926200511284379");

		// 根据不动产单元号 查keti信息
		// chaXunKetiByBdcdyh(client,"keti3","keti3","150722869801GB95231F358313817");

		// 先插客体100条记录，然后找出不动产单元号，然后根据权利人相关信息
		// getQyrsByBdcdyhList(client, "keti3", "keti3", 100);

		// 根据查询条件查询 权利人
		// BoolQueryBuilder condition =
		// QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("zl", "市中"));
		// getQyrsByCondition(client, "keti3", "keti3", 100, condition);

		// 421182866490GB18165F331396000
		// chaXunKetiByBdcdyh(client, "keti3",
		// "keti3","421182866490GB18165F331396000");
		
		//根据查询条件查询权利人 索引信息   包括权利人不动产数量
		chaXunQlrByCondition(client, "qlr", "qlr","bdcdyh","450804464313GB13560F768258634");
		
		
		client.close();

	}

	/****
	 * 根据不动产单元号 查keti信息
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param ndcdyh
	 */
	private static void chaXunKetiByBdcdyh(TransportClient client, String index, String type, String ndcdyh) {

		SearchRequestBuilder responsebuilder1 = client.prepareSearch(index).setTypes(type);

		long start = System.currentTimeMillis();
		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchQuery("bdcdyh", ndcdyh)).must(QueryBuilders.termQuery("records", "0"));
		SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1).execute().actionGet();
		SearchHits hits1 = response1.getHits();
		
		

		for (int j = 0; j < hits1.getHits().length; j++) {
			// System.out.print("\txm:" +
			// hits1.getHits()[j].getSource().get("xm"));
			// System.out.print("\tzjh:" +
			// hits1.getHits()[j].getSource().get("zjh"));
			System.out.print("\tbdcdyh:" + hits1.getHits()[j].getSource().get("bdcdyh"));
			System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records"));
		}

		long end = System.currentTimeMillis();

		System.out.println("总耗时：" + (end - start) + "ms");

	}

	/****
	 * 根据权利人名称查询权利人信息
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param qlrNm
	 */
	private static void chaXunQlrByCondition(TransportClient client, String index, String type, String column,String columnValue) {

		SearchRequestBuilder responsebuilder1 = client.prepareSearch(index).setTypes(type);

		long start = System.currentTimeMillis();
		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchQuery(column, columnValue)).must(QueryBuilders.termQuery("records", "0"));
		SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1).execute().actionGet();
		SearchHits hits1 = response1.getHits();

		for (int j = 0; j < hits1.getHits().length; j++) {
			System.out.print("\txm:" + hits1.getHits()[j].getSource().get("xm"));
			System.out.print("\tzjh:" + hits1.getHits()[j].getSource().get("zjh"));
			System.out.print("\tbdcdyh:" + hits1.getHits()[j].getSource().get("bdcdyh"));
			System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records"));
		}

		long end = System.currentTimeMillis();

		System.out.println("总耗时：" + (end - start) + "ms");

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
	 * 根据查询条件 查询权利人信息
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param size
	 * @param query
	 * @throws UnknownHostException
	 */
	public static void getQyrsByCondition(TransportClient client, String index, String type, int size,
			BoolQueryBuilder query) throws UnknownHostException {

		long start = System.currentTimeMillis();
		SearchRequestBuilder searchRequestKetiB = null;
		if (size == 0) {
			searchRequestKetiB = client.prepareSearch(index).setTypes(type);
		} else {
			searchRequestKetiB = client.prepareSearch(index).setTypes(type).setSize(size);
		}

		SearchResponse ketiResponse = searchRequestKetiB.setQuery(query).execute().actionGet();
		SearchHits ketiHits = ketiResponse.getHits();

		long endketi = System.currentTimeMillis();

		// 构建权利人的构造
		SearchRequestBuilder qlrSearchRequestBuilder = client.prepareSearch("qlr").setTypes("qlr");

		long qlrCost = 0L;
		for (int i = 0; i < ketiHits.getHits().length; i++) {

			// 以查询到的客体不动产单元号 作为权利人的 查询条件
			String bdcbh = String.valueOf(ketiHits.getHits()[i].getSource().get("bdcdyh"));
			BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
					/* .must(QueryBuilders.termQuery("records", "0")) */.must(
							QueryBuilders.matchQuery("bdcdyh", bdcbh));
			// System.out.println("\t keti bdcdyh:" + bdcbh + "\n");

			long qlrstrt = System.currentTimeMillis();
			SearchResponse response1 = qlrSearchRequestBuilder.setQuery(boolQueryQueryBuilder1).execute().actionGet();
			SearchHits hits1 = response1.getHits();
			for (int j = 0; j < hits1.getHits().length; j++) {
				System.out.print("\txm:" + hits1.getHits()[j].getSource().get("xm"));
				System.out.print("\tbdc:" + hits1.getHits()[j].getSource().get("bdcdyh"));
				System.out.print("\tzjh:" + hits1.getHits()[j].getSource().get("zjh"));
				System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records") + "\n");
			}
			long qlrEnd = System.currentTimeMillis();

			qlrCost += (qlrEnd - qlrstrt);

		}

		long end = System.currentTimeMillis();

		System.out.println("总耗时：" + (end - start) + "ms");

		System.out.println("qlr 耗时:" + qlrCost + "ms");

		System.out.println("keti cost:" + (endketi - start) + "ms");
	}

	/****
	 * 查询指定的不动产单元号列表，批量查询出权利人相关信息
	 * 
	 * @param client
	 * @throws UnknownHostException
	 */
	public static void getQyrsByBdcdyhList(TransportClient client, String index, String type, int size)
			throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息
		SearchResponse response = client.prepareSearch(index).setTypes(type).setSize(size).execute().actionGet();
		SearchHits hits = response.getHits();

		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("records", "0"));
		for (int i = 0; i < hits.getHits().length; i++) {

			String bdcbh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
			boolQueryQueryBuilder1.should(QueryBuilders.matchQuery("bdcdyh", bdcbh));

		}

		SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");
		SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1).execute().actionGet();
		SearchHits hits1 = response1.getHits();
		for (int j = 0; j < hits1.getHits().length; j++) {
			System.out.print("\txm:" + hits1.getHits()[j].getSource().get("xm"));
			System.out.print("\tbdc:" + hits1.getHits()[j].getSource().get("bdcdyh"));
			System.out.print("\tzjh:" + hits1.getHits()[j].getSource().get("zjh"));
			System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records") + "\n");
		}

		long end = System.currentTimeMillis();

		System.out.println("总耗时：" + (end - start) + "ms");
	}

	/****
	 * 查询qlr
	 * 
	 * @param client
	 * @throws UnknownHostException
	 */
	public static void chaXunQlrByKeti100(TransportClient client) throws UnknownHostException {

		SearchResponse response = client.prepareSearch("keti").setTypes("keti").setSize(50).execute().actionGet();
		SearchHits hits = response.getHits();

		SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");
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

	/***
	 * 通过lx和records 查询记录
	 * 
	 * @param client
	 * @param indexNm
	 * @param type
	 * @param count
	 * @param leixing
	 * @param record
	 * @throws UnknownHostException
	 */
	public static void chaXunKtByRecordLx(TransportClient client, String indexNm, String type, int count,
			String leixing, String record) throws UnknownHostException {

		long start = System.currentTimeMillis();
		SearchRequestBuilder responsebuilder = client.prepareSearch(indexNm).setTypes(type);
		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("lx", leixing)).filter(QueryBuilders.termQuery("records", record));
		SearchResponse response = responsebuilder.setQuery(boolQueryQueryBuilder1).setSize(count).execute().actionGet();
		SearchHits hits = response.getHits();
		for (int i = 0; i < hits.getHits().length; i++) {
			System.out.print("\t[bdcdyh]\t :" + hits.getHits()[i].getSource().get("bdcdyh") + "|");
			System.out.print("\t[lx]\t :" + hits.getHits()[i].getSource().get("lx") + "|");
			System.out.print("\t[uuid]\t:" + hits.getHits()[i].getSource().get("uuid") + "|");
			System.out.print("\t[qx]\t:" + hits.getHits()[i].getSource().get("qx") + "|");
			System.out.print("\t[zl]\t:" + hits.getHits()[i].getSource().get("zl") + "|");
			System.out.print("\t[records]\t:" + hits.getHits()[i].getSource().get("records") + "|");
			System.out.print("\t[postDate]\t:" + hits.getHits()[i].getSource().get("postDate") + "|" + "\n");
		}

		long end = System.currentTimeMillis();

		System.out.println("总耗时:" + (end - start) + "ms");
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
