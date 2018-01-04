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

public class QueryBack {
	public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
	// public static String hostname = "10.110.13.179";
	// public static String hostname = "localhost";
	public static String clustername = "es";
	// public static String index = "test001";
	// public static String type = "type001";

	public static void main(String[] args) throws UnknownHostException, InterruptedException {

		TransportClient client = getClient1withNOxpack();

		List<Long> costTimes = new ArrayList<Long>();

		String[] qlrdws = { "猪妙脉劝锈廷叶医寞术恶拖", "屁鲜殿试逞水碴翟际乌庆滇", "勒螺渐羚仿礁桨墅质捏哩喷", "梆焦夷甄迷症留含言犊针撕", "线淹源畴诵墩悟乎兵镣十嚼",
				"棱慌浦典秧劝弗圃感香蜡原", "啤翻禾藻碾玻桔斩沈碌社殖", "鸥亲枕瘴痛较诗痊邓卵剿父", "扎星厄品粹课师雕谦拎彰弄", "牲钱午沉包如旋兔楷惮酱阑" };

		String[] qlrXms = { "闾时进", "夏秋伊", "经保杰", "东凡茗", "郜新言", "荣信以", "督咏", "牧强", "薛馨瑞", "乐亮" };

		String[] qlrzjhs = { "230600198010015006", "640522196211247516", "440115197308022999", "652122198411309639",
				"230506195402259308", "530127198403314086", "140622197803265779", "542425197704155261",
				"460323197507098894", "141029198210295240" };

		String[] bdcdyhs = { "330483421408GB77120F591008209", "330402051645GB43350F523170557",
				"530802557464GB69603F290520285", "654025556740GB81649F582450513", "611024402330GB55349F273907421",
				"610627677378GB74622F583795434", "542421819466GB98305F270018307", "370827355063GB00075F443900854",
				"530122207927GB92318F487692347", "650201882330GB77222F839038732" };

		// for (int i = 0; i < 10; i++) {

		// 根据权利人 查出不动产数量
		// costTimes.add(queryQlrBdcNum(client, "qlr", "qlr", "", qlrzjhs[i]));

		// 场景：根据权利人证件号码查询 统计耗时
		// costTimes.add(chaXunQlrByConditionByFilter(client, "qlr",
		// "qlr", "zjh", qlrzjhs[i]));

		//
		// Thread.sleep(4);
		//
		// }
		//
		// Long allCost = 0L;
		// for (Long cost : costTimes) {
		// allCost += cost;
		// System.out.print(cost + "\t");
		// }
		//
		// System.out.println("平均耗时：" + allCost / 10);

		// 通过坐落查询统计相关信息
		// queryByZl(client,"那曲");

		// 查询指定的不动产单元号列表，批量查询出权利人相关信息
		// getQyrsByBdcdyhListByShold(client, "keti", "keti", 100);
		// getQyrsByBdcdyhListByFilter(client, "keti", "keti", 100);

		// 根据lx和record查询记录
		// chaXunKtByRecordLx(client,"keti3","keti3",500,"10","0");
		// chaXunKtByRecordLx(client,"keti2","keti2",100,"10","0");

		// 查询权利人通过证件号
		// chaXunQlrByConditionByMust(client,"qlr","qlr","zjh","230600198010015006");
		// chaXunQlrByConditionByFilter(client, "qlr", "qlr", "zjh",
		// "230600198010015006");

		// 查询权利人不动产单元号
		// chaXunQlrByConditionByFilter(client, "qlr", "qlr", "bdcdyh",
		// "330400934373GB28301F427478609");
		// chaXunQlrByConditionByMust(client,"qlr","qlr","bdcdyh","330400934373GB28301F427478609");

		// 根据不动产单元号 查keti信息
		// chaXunKetiByBdcdyh(client, "keti", "keti", "");

		// 先插客体100条记录，然后找出不动产单元号，然后根据权利人相关信息
		// getQyrsByBdcdyhList(client, "keti3", "keti3", 100);

		// 根据查询条件查询 权利人
		// BoolQueryBuilder condition =
		// QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("zl", "市中"));
		// getQyrsByCondition(client, "keti3", "keti3", 100, condition);

		// 421182866490GB18165F331396000
		// chaXunKetiByBdcdyh(client, "keti3",
		// "keti3","421182866490GB18165F331396000");

		// 根据权利人证件号 查询权利人不动产数量 以及展示不动产信息
		// queryQlrBdcNum(client, "qlr", "qlr", "zjh", "230600198010015006");

		// 统计场景1
		tjBdcNumByCondition(client);

		// getQyrsByBdcdyh(client,"530122207927GB92318F487692347",100);

		client.close();

	}

	/****
	 * 查询场景：查询出客体前100条记录，条件：不动产单元号，根据每条客体记录的不动产单元号获取权利人 毫秒
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param size
	 * @throws UnknownHostException
	 */
	public static void getQyrsByBdcdyh(TransportClient client, String bdcdyh, int size) throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息

		SearchRequestBuilder ketiSearchRB = client.prepareSearch("keti").setTypes("keti").setSize(size);

		// SearchResponse response =
		// client.prepareSearch(index).setTypes(type).setSize(size).execute().actionGet();

		BoolQueryBuilder ketiBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("records", "0")).filter(QueryBuilders.termsQuery("bdcdyh", bdcdyh));

		SearchResponse ketiResponse = ketiSearchRB.setQuery(ketiBoolQueryQueryBuilder1).execute().actionGet();

		SearchHits ketiHits = ketiResponse.getHits();

		long ketiEnd = System.currentTimeMillis();

		long cost = ketiEnd - start;

		System.out.println(cost);

		for (int i = 0; i < ketiHits.getTotalHits(); i++) {
			String bdcdyhItem = String.valueOf(ketiHits.getHits()[i].getSource().get("bdcdyh"));

			BoolQueryBuilder qlrBoolQueryQueryBuilder1 = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery("records", "0"))
					.filter(QueryBuilders.termsQuery("bdcdyh", bdcdyhItem));

			SearchRequestBuilder qlrRb = client.prepareSearch("qlr").setTypes("qlr");
			SearchResponse response1 = qlrRb.setQuery(qlrBoolQueryQueryBuilder1).execute().actionGet();
			SearchHits hits1 = response1.getHits();
			for (int j = 0; j < hits1.getHits().length; j++) {
				System.out.print("\txm:" + hits1.getHits()[j].getSource().get("xm"));
				System.out.print("\tbdc:" + hits1.getHits()[j].getSource().get("bdcdyh"));
				System.out.print("\tzjh:" + hits1.getHits()[j].getSource().get("zjh"));
				System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records") + "\n");
			}

		}

		long end = System.currentTimeMillis();

		System.out.println("总耗时：" + (end - start) + "ms");
	}

	/***
	 * 场景1
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param ndcdyh
	 * @return
	 */
	private static void tjBdcNumByCondition(TransportClient client) {

		long start = System.currentTimeMillis();
		SearchRequestBuilder srb = client.prepareSearch("qlr_1y").setTypes("qlr_1y");
		TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qxtj").field("lx").size(100);
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
			// System.out.println(gradeBucket.getKey() + "地区有" +
			// gradeBucket.getDocCount() + "条记录。");
			a = a + gradeBucket.getDocCount();
		}

		long end = System.currentTimeMillis();
		System.out.println("总共记录条数：" + String.valueOf(a) + "耗时：" + (end - start) + "ms");

	}

	/****
	 * 根据不动产单元号 查keti信息
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param ndcdyh
	 */
	private static Long chaXunKetiByBdcdyh(TransportClient client, String index, String type, String bdcdyh) {

		SearchRequestBuilder responsebuilder1 = client.prepareSearch(index).setTypes(type).setSize(100);

		long start = System.currentTimeMillis();

		if (StringUtils.isBlank(bdcdyh)) {
			BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
					.filter(QueryBuilders.termQuery("records", "0"));
			SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1).execute().actionGet();
			SearchHits hits1 = response1.getHits();

			for (int j = 0; j < hits1.getHits().length; j++) {
				System.out.print("\tbdcdyh:" + hits1.getHits()[j].getSource().get("bdcdyh"));
				System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records"));
			}

			long end = System.currentTimeMillis();

			System.out.println("总耗时：" + (end - start) + "ms");
			return hits1.getHits().length > 0 ? end - start : 0;

		} else {
			BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
					.filter(QueryBuilders.matchQuery("bdcdyh", bdcdyh)).filter(QueryBuilders.termQuery("records", "0"));
			SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1).execute().actionGet();
			SearchHits hits1 = response1.getHits();

			for (int j = 0; j < hits1.getHits().length; j++) {
				System.out.print("\tbdcdyh:" + hits1.getHits()[j].getSource().get("bdcdyh"));
				System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records"));
			}

			long end = System.currentTimeMillis();

			System.out.println("总耗时：" + (end - start) + "ms");
			return hits1.getHits().length > 0 ? end - start : 0;

		}

	}

	/****
	 * 查询场景：查询权利人不动产数量，根据权利人证件查询
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param column
	 * @param columnValue
	 * @return
	 */
	private static Long queryQlrBdcNum(TransportClient client, String index, String type, String name, String zjhm) {

		SearchRequestBuilder responsebuilder1 = client.prepareSearch(index).setTypes(type).setSize(100);

		long start = System.currentTimeMillis();

		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery();

		if (StringUtils.isBlank(name) && StringUtils.isBlank(zjhm)) {
			System.out.println("查询条件不能为空");
			return null;
		}

		if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(zjhm)) {
			boolQueryQueryBuilder1.must(QueryBuilders.matchPhraseQuery("xm", name))
					.must(QueryBuilders.matchPhraseQuery("zjh", zjhm)).filter(QueryBuilders.termQuery("records", 0));
		} else if (StringUtils.isNotBlank(name)) {
			boolQueryQueryBuilder1.filter(QueryBuilders.matchPhraseQuery("xm", name))
					.filter(QueryBuilders.termQuery("records", 0));
		} else {
			boolQueryQueryBuilder1.filter(QueryBuilders.matchPhraseQuery("zjh", zjhm))
					.filter(QueryBuilders.termQuery("records", 0));
		}

		SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1).execute().actionGet();
		SearchHits hits1 = response1.getHits();

		Set<String> bdcList = new HashSet<String>();
		for (int j = 0; j < hits1.getHits().length; j++) {
			String bdcdyh = String.valueOf(hits1.getHits()[j].getSource().get("bdcdyh"));
			System.out.print("\txm:" + hits1.getHits()[j].getSource().get("xm"));
			System.out.print("\tzjh:" + hits1.getHits()[j].getSource().get("zjh"));
			System.out.print("\tbdcdyh:" + bdcdyh);
			System.out.print("\trecords:" + hits1.getHits()[j].getSource().get("records"));

			bdcList.add(bdcdyh);
		}

		// BoolQueryBuilder ketiBuilder =
		// QueryBuilders.boolQuery().must(QueryBuilders.termQuery("records",
		// "0"))
		// .filter(QueryBuilders.termsQuery("bdcdyh", bdcList));
		// SearchRequestBuilder ketiSearchRb =
		// client.prepareSearch("keti").setTypes("keti");
		//
		// SearchResponse ketiResponse =
		// ketiSearchRb.setQuery(ketiBuilder).execute().actionGet();
		//
		// SearchHits ketiHits = ketiResponse.getHits();
		//
		// Set<String> bdcSet = new HashSet<String>();
		//
		// System.out.println("不动产列表为：");
		// for (int m = 0; m < ketiHits.getTotalHits(); m++) {
		//
		// String bdcdyh =
		// String.valueOf(ketiHits.getHits()[m].getSource().get("bdcdyh"));
		//
		// bdcSet.add(bdcdyh);
		//
		// System.out.print(bdcdyh);
		// }
		//
		// System.out.println("权利人：" + name + "," + zjhm + "\t 有不动产数量：" +
		// bdcSet.size());

		System.out.println("权利人：" + name + "," + zjhm + "\t 有不动产数量：" + bdcList.size());

		long end = System.currentTimeMillis();

		System.out.println("总耗时：" + (end - start) + "ms");

		return (end - start);

	}

	/****
	 * 根据权利人证件号码 查询权利人信息
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param column
	 * @param columnValue
	 */
	private static Long chaXunQlrByConditionByFilter(TransportClient client, String index, String type, String column,
			String columnValue) {

		SearchRequestBuilder responsebuilder1 = client.prepareSearch(index).setTypes(type);

		long start = System.currentTimeMillis();
		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery(column, columnValue)).filter(QueryBuilders.termQuery("records", "0"));
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

		return hits1.getHits().length > 0 ? (end - start) : 0;

	}

	/****
	 * 根据权利人名称查询权利人信息
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param qlrNm
	 */
	private static long chaXunQlrByConditionByMust(TransportClient client, String index, String type, String column,
			String columnValue) {

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
		return hits1.getHits().length > 0 ? (end - start) : 0;

	}

	/***
	 * 查询坐落 模糊查询
	 * 
	 * @param client
	 * @param count
	 * @param leixing
	 * @throws UnknownHostException
	 */
	public static void queryByZl(TransportClient client, String zl) throws UnknownHostException {
		SearchRequestBuilder srb = client.prepareSearch("keti").setTypes("keti").setSize(100);

		MatchPhraseQueryBuilder boolQueryQueryBuilder = QueryBuilders.matchPhraseQuery("zl", zl);

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
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.174"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.175"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.176"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.177"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.178"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.179"), 9300));

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
	public static Long getQyrsByCondition(TransportClient client, String index, String type, int size,
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
					.filter(QueryBuilders.termQuery("records", "0")).filter(QueryBuilders.termQuery("bdcdyh", bdcbh));
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

		return ketiHits.getTotalHits() != 0 ? end - start : 0;
	}

	/***
	 * 根据不动产列表查询权利人
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param size
	 * @throws UnknownHostException
	 */
	public static void getQyrsByBdcdyhListByFilter(TransportClient client, String index, String type, int size)
			throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息
		SearchResponse response = client.prepareSearch(index).setTypes(type).setSize(size).execute().actionGet();
		SearchHits hits = response.getHits();

		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("records", "0"));
		List<String> bdcdyhs = new ArrayList<String>();
		for (int i = 0; i < hits.getHits().length; i++) {
			bdcdyhs.add(String.valueOf(hits.getHits()[i].getSource().get("bdcdyh")));
		}

		boolQueryQueryBuilder1.filter(QueryBuilders.termsQuery("bdcdyh", bdcdyhs));

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
	 * 查询指定的不动产单元号列表，批量查询出权利人相关信息
	 * 
	 * @param client
	 * @throws UnknownHostException
	 */
	public static void getQyrsByBdcdyhListByShold(TransportClient client, String index, String type, int size)
			throws UnknownHostException {

		long start = System.currentTimeMillis();

		// 首先获取课题的100条信息
		SearchResponse response = client.prepareSearch(index).setTypes(type).setSize(size).execute().actionGet();
		SearchHits hits = response.getHits();

		BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
				.must(QueryBuilders.termQuery("records", "0"));
		List<String> bdcdyhs = new ArrayList<String>();
		for (int i = 0; i < hits.getHits().length; i++) {
			bdcdyhs.add(String.valueOf(hits.getHits()[i].getSource().get("bdcdyh")));
		}

		boolQueryQueryBuilder1.should(QueryBuilders.termsQuery("bdcdyh", bdcdyhs));

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
