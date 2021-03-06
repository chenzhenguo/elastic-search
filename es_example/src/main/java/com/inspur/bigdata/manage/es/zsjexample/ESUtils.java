package com.inspur.bigdata.manage.es.zsjexample;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSON;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * elasticsearch operation tool
 * 
 * all operation has two styles
 * 
 * The Java High Level REST,one way oby restfull strictly,use
 * index(update\delete)Request,and client call index(update\delete)method to
 * exec request,just like httclient to exec HttpGet,HttpPost,HttpDelete.such is
 * elasticsearch tell us ,as blow: The Java High Level REST Client works on top
 * of the Java Low Level REST client. Its main goal is to expose API specific
 * methods, that accept request objects as an argument and return response
 * objects, so that request marshalling and response un-marshalling is handled
 * by the client itself.
 * 
 * Each API can be called synchronously or asynchronously. The synchronous
 * methods return a response object, while the asynchronous methods, whose names
 * end with the async suffix, require a listener argument that is notified (on
 * the thread pool managed by the low level client) once a response or an error
 * is received.
 * 
 * The Java High Level REST Client depends on the Elasticsearch core project. It
 * accepts the same request arguments as the TransportClient and returns the
 * same response objects.
 * 
 * 
 * 
 * 
 * 
 * 
 * Another way oby restfull not strictly,use less code like this,
 * client.prepareIndex...(prepareUpdate,prepareDeleter) and return response
 * 
 * 
 * when you write to index,if index not exist,it will create one ;if exist then
 * do as nomal
 * 
 * when you get many request,you should ues bulkProcess or BulkRequestBuilder
 * 
 */

public class ESUtils {

	public static void main(String[] args) throws UnknownHostException, IOException {

		TransportClient client = getClient();
		listIndex(client);
		// createIndex(getClient("es", "10.110.13.176"), "ql3", "ql3", 5, 3);
		// writeDocumentByPrepareIndex(client, "tohdfs", "tohdfs","001");

		// writeDocumentByIndexRequest(client, "tohdfs", "tohdfs","0");

		// UpdateDocumentByPrepareUpdate(client, "tohdfs", "tohdfs","0");

		// deleteDocumentByPrepareDelete(client, "tohdfs", "tohdfs","0");
		// deleteDocumentByDeleteRequest(client, "tohdfs", "tohdfs", "3");
		// useBulkRequestBuilder(client, "tohdfs", "tohdfs");
		// useBulkProcessor(client, "tohdfs", "tohdfs");
		// deleteDocumentByQuery(client, "tohdfs");
	}

	/****
	 * create index
	 * 
	 * @param client
	 * @param indexName
	 * @param type
	 * @param shareds
	 * @param replices
	 * @throws IOException
	 */
	public static void createIndex(TransportClient client, String indexName, String type, int shareds, int replices)
			throws IOException {
		XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("settings")
				.field("index.number_of_shards", shareds).field("number_of_replicas", replices).endObject().endObject();

		CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(indexName).setSource(mapping);
		CreateIndexResponse response = cirb.execute().actionGet();

		if (response.isShardsAcked()) {
			System.out.println("index created");
		} else {
			System.out.println("index create failed");
		}

	}

	public static TransportClient getClient() throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", "es").put("transport.type", "netty4")
				.put("http.type", "netty4").put("client.transport.sniff", true).build();

		// Settings settings = Settings.builder()
		// .put("cluster.name",clusterName)
		// .put("xpack.security.transport.ssl.enabled", false)
		// .put("xpack.security.user", "elastic:changeme")
		// .put("client.transport.sniff", true).build();
		TransportClient client = new PreBuiltTransportClient(settings);
		// client.addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),
		// 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.6"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.7"), 9300));
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.6.8"), 9300));

		return client;
	}

	/**
	 * 索引列表
	 * 
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
	 * create index with default setting
	 * 
	 * @throws UnknownHostException
	 */
	public static void createIndex(TransportClient client, String indexName) throws UnknownHostException {
		CreateIndexRequest cIndexRequest = new CreateIndexRequest(indexName);
		CreateIndexResponse cIndexResponse = client.admin().indices().create(cIndexRequest).actionGet();
		if (cIndexResponse.isAcknowledged()) {
			System.out.println("索引创建成功");
		} else {
			System.out.println("索引创建失败");
		}
	}

	/**
	 * if exist index
	 * 
	 * @throws UnknownHostException
	 */
	public static void existIndex(TransportClient client, String indexName) throws UnknownHostException {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		IndicesExistsResponse response = indicesAdminClient.prepareExists(indexName).get();
		if (response.isExists()) {
			System.out.println("索引存在");
		} else {
			System.out.println("索引不存在");
		}
	}

	public static void listTypes(TransportClient client, String indexName) throws UnknownHostException {
		List<String> typeList = new ArrayList<String>();
		try {
			GetMappingsResponse res = client.admin().indices().getMappings(new GetMappingsRequest().indices(indexName))
					.get();
			ImmutableOpenMap<String, MappingMetaData> mapping = res.mappings().get(indexName);
			for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
				typeList.add(c.key);
			}
		} catch (Exception e) {

		}
		System.out.println("Type List:");
		for (String type : typeList) {
			System.out.println(type);
		}
	}

	/**
	 * write to index with PrepareIndex
	 * 
	 * way 1
	 * 
	 * @throws IOException
	 */
	public static void writeDocumentByPrepareIndex(TransportClient client, String indexName, String typeName, String id)
			throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "510504001004GB00080W00000013");

		String jsondata = JSON.toJSONString(parm);
		jsondata = jsondata.replace("[", "").replace("]", "");

		System.out.println(jsondata);

		IndexResponse response = client.prepareIndex(indexName, typeName, id).setSource(jsondata, XContentType.JSON)
				.get();

		while (response.isFragment()) {
			System.out.println("写入成功");
		}

	}

	/**
	 * write to index with IndexRequest
	 * 
	 * way 2
	 * 
	 * @throws IOException
	 */
	public static void writeDocumentByIndexRequest(TransportClient client, String indexName, String typeName, String id)
			throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "510504001004GB00080W00000000");

		System.out.println(parm);

		IndexResponse response = client.index(new IndexRequest(indexName, typeName, id).source(parm)).actionGet();

		/*
		 * by way one
		 * 
		 * IndexResponse response1 = client.prepareIndex(indexName,
		 * typeName).setId("2") .setSource(parm, XContentType.JSON).get();
		 */

		while (response.isFragment()) {
			System.out.println("写入成功");
		}

	}

	/***
	 * update index by updateRequest
	 * 
	 * @param client
	 * @param indexName
	 * @param typeName
	 * @throws IOException
	 */
	public static void UpdateDocumentByUpdateRequest(TransportClient client, String indexName, String typeName,
			String id) throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "121212121");

		// String jsondata = JSON.toJSONString(parm);

		System.out.println(parm);

		UpdateResponse response = client.update(new UpdateRequest(indexName, typeName, id).doc(parm)).actionGet();

		System.out.println("修改成功,分片信息" + response.getShardInfo().toString());

	}

	/***
	 * update index by updateRequest
	 * 
	 * @param client
	 * @param indexName
	 * @param typeName
	 * @throws IOException
	 */
	public static void UpdateDocumentByPrepareUpdate(TransportClient client, String indexName, String typeName,
			String id) throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "123");

		// String jsondata = JSON.toJSONString(parm);

		System.out.println(parm);

		UpdateResponse response = client.prepareUpdate(indexName, typeName, id).setDoc(parm, XContentType.JSON).get();

		System.out.println("修改成功,分片信息" + response.getShardInfo().toString());

	}

	/***
	 * delete index by PrepareDelete
	 * 
	 * @param client
	 * @param indexName
	 * @param typeName
	 * @throws IOException
	 */
	public static void deleteDocumentByPrepareDelete(TransportClient client, String indexName, String typeName,
			String id) throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "123");

		// String jsondata = JSON.toJSONString(parm);

		System.out.println(parm);

		DeleteResponse response = client.prepareDelete(indexName, typeName, id).get();

		System.out.println("删除成功,分片信息" + response.getShardInfo().toString());

	}

	/**
	 * 查询数据
	 * 
	 * @throws UnknownHostException
	 * @throws JsonProcessingException
	 */
	public static void queryDocument(TransportClient client, String indexName, String typeName)
			throws UnknownHostException, JsonProcessingException {
		SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).execute().actionGet();
		// 获取响应字符串
		System.out.println(response.toString());
		// 遍历查询结果输出相关度分值和文档内容
		SearchHits searchHits = response.getHits();
		for (SearchHit searchHit : searchHits) {
			System.out.println(searchHit.getScore());
			System.out.println(searchHit.getSourceAsString());
		}
	}

	/**
	 * delete by id
	 * 
	 * @throws UnknownHostException
	 */
	public static void deleteDocumentByDeleteRequest(TransportClient client, String indexName, String typeName,
			String documentID) throws UnknownHostException {
		DeleteResponse response = client
				.delete(new org.elasticsearch.action.delete.DeleteRequest(indexName, typeName, documentID)).actionGet();
		System.out.println("delete success,docid is :" + response.getId());
	}

	/**
	 * delete by query
	 * 
	 * @throws UnknownHostException
	 */
	public static void deleteDocumentByQuery(TransportClient client, String indexName) throws UnknownHostException {
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.termQuery("postDate", "xiugaihou")).source(indexName).get();

		long deleted = response.getDeleted();
		System.out.println("delete cost :" + deleted);
	}

	/****
	 * use BulkRequestBuilder batch execs
	 * 
	 * @throws UnknownHostException
	 */
	public static void useBulkRequestBuilder(TransportClient client, String indexName, String typeName)
			throws UnknownHostException {
		// Settings settings = Settings.builder().put("cluster.name",
		// "es").build();
		// TransportClient client = new PreBuiltTransportClient(settings);
		// client.addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("10.10.6.6"),
		// 9300));
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

		Map<String, Object> json = new HashMap();
		json.put("BDCDYH", "1");
		json.put("postDate", "2017-11-12 12:22:23");
		Map<String, Object> json2 = new HashMap();
		json2.put("BDCDYH", "2");
		json2.put("postDate", "xiugaiqian");
		Map<String, Object> json3 = new HashMap();
		json3.put("BDCDYH", "2");
		json3.put("postDate", "xiugaihou");
		IndexRequestBuilder update1 = client.prepareIndex(indexName, typeName, "001").setSource(json);
		IndexRequestBuilder update2 = client.prepareIndex(indexName, typeName, "002").setSource(json2);
		UpdateRequestBuilder update3 = client.prepareUpdate(indexName, typeName, "002").setDocAsUpsert(true)
				.setDoc(json3);
		bulkRequestBuilder.add(update1);
		bulkRequestBuilder.add(update2);
		bulkRequestBuilder.add(update3);
		// bulkRequestBuilder.add(update3);
		BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
		if (bulkItemResponse.hasFailures()) {
			System.out.println(bulkItemResponse.buildFailureMessage());
		}
		client.close();
	}

	/***
	 * if you bach process index and delete request you can use this way,not for
	 * update
	 * 
	 * @param client
	 * @param index
	 * @param type
	 */
	public static void useBulkProcessor(TransportClient client, String index, String type) {
		BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				System.out.println("尝试插入：" + request.numberOfActions() + "条记录");
			}

			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			}
		}).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)).setBulkActions(10000)
				.setFlushInterval(TimeValue.timeValueSeconds(3)).build();

		Map<String, String> parm = new HashMap<String, String>(1);
		parm.put("BDCDYH", "510504001004GB00080W00000013");

		IndexRequest indexRequest = new IndexRequest(index, type, "003").source(parm);
		bulkProcessor.add(indexRequest);

		parm.put("BDCDYH", "510504001004GB00080W00000014");
		IndexRequest indexRequest1 = new IndexRequest(index, type, "004").source(parm);
		bulkProcessor.add(indexRequest1);

		// error : bulkProcessor dont exec updateRequest
		// parm.put("BDCDYH", "510504001004GB00080W00000014");
		// new UpdateRequest(index, type, "001").doc(parm);
		// bulkProcessor.add(UpdateRequest);

	}

	/****
	 * 结构化的统计
	 * 
	 * @param client
	 * @param indexname
	 * @param typename
	 * @param zjh
	 */
	public static void testStructingAggregation(TransportClient client, String indexname, String typename, String zjh) {

		AggregationBuilder structedAggregationBuilder = AggregationBuilders.terms("by_country").field("country")
				.subAggregation(AggregationBuilders.dateHistogram("by_year").field("dateOfBirth")
						.dateHistogramInterval(DateHistogramInterval.YEAR)
						.subAggregation(AggregationBuilders.avg("avg_children").field("children")));

		SearchRequestBuilder srb = client.prepareSearch(indexname).setTypes(typename).setSize(100);
		srb.addAggregation(structedAggregationBuilder);

		SearchResponse sr = srb.execute().actionGet();

		Filters agg = sr.getAggregations().get("qlrtj");

		for (Filters.Bucket entry : agg.getBuckets()) {
			String key = entry.getKeyAsString();
			long docCount = entry.getDocCount();
			System.out.println("zjh" + key + ",count:" + docCount);
		}
	}
	
	/**
	 * 测试组合聚合
	 * @param client
	 * @param indexname
	 * @param typename
	 */
	public static void testSubAgg(TransportClient client, String indexname, String typename) {

		SearchRequestBuilder srb = client.prepareSearch(indexname).setTypes(typename).setSize(100);
		srb.addAggregation(AggregationBuilders.terms("byCountry").field("country")
				.subAggregation(AggregationBuilders.max("maxage").field("age")));

		SearchResponse sr = srb.execute().actionGet();

		Terms agg = sr.getAggregations().get("byCountry");
		for (Terms.Bucket entry : agg.getBuckets()) {
			String key = (String) entry.getKey(); // bucket key
			long docCount = entry.getDocCount(); // Doc count
			System.out.println("key " + key + " doc_count " + docCount);
			
			Max agg1 = entry.getAggregations().get("maxage");
			double value1 = agg1.getValue();
			System.out.println("minage:"+value1);
		}

	}

	/****
	 * 结构化的统计
	 * 
	 * @param client
	 * @param indexname
	 * @param typename
	 * @param zjh
	 */
	public static void testMetricsAgg(TransportClient client, String indexname, String typename) {

		MinAggregationBuilder aggregation1 = AggregationBuilders.min("minage").field("age");

		MaxAggregationBuilder aggregation2 = AggregationBuilders.max("maxAge").field("age");

		StatsAggregationBuilder aggregation3 = AggregationBuilders.stats("allAboutAge").field("age");

		SearchRequestBuilder srb = client.prepareSearch(indexname).setTypes(typename).setSize(100);
		srb.addAggregation(aggregation1);
		srb.addAggregation(aggregation2);
		srb.addAggregation(aggregation3);

		SearchResponse sr = srb.execute().actionGet();

		Min agg1 = sr.getAggregations().get("minage");
		double value1 = agg1.getValue();

		Max agg2 = sr.getAggregations().get("maxAge");
		double value2 = agg2.getValue();
		System.out.println("min :" + value1 + "max :" + value2);

		Stats agg = sr.getAggregations().get("allAboutAge");
		double min = agg.getMin();
		double max = agg.getMax();
		double avg = agg.getAvg();
		double sum = agg.getSum();
		double count = agg.getCount();

		System.out.println(String.format("all status is :[min]:%f,[max]:%f,[avg]:%f,[sum]:%f,[count]:%f", min, max, avg,
				sum, count));

	}

	/****
	 * 测试 桶 统计
	 * 
	 * @param client
	 * @param indexname
	 * @param typename
	 */
	public static void testBucketAgg(TransportClient client, String indexname, String typename) {
		AggregationBuilder aggregation = AggregationBuilders.filters("agg",
				new FiltersAggregator.KeyedFilter("age28", QueryBuilders.termQuery("age", 28)));

		SearchRequestBuilder srb = client.prepareSearch(indexname).setTypes(typename).setSize(100);

		srb.addAggregation(aggregation);

		SearchResponse sr = srb.execute().actionGet();

		Filters agg = sr.getAggregations().get("agg");

		for (Filters.Bucket entry : agg.getBuckets()) {
			String key = entry.getKeyAsString(); // bucket key
			long docCount = entry.getDocCount(); // Doc count
			System.out.println("key [{" + key + "}], doc_count [{" + docCount + "}]");
		}

	}

	/***
	 * 查询结果带高亮
	 * 
	 * @param client
	 */
	public static void queryWithHighLight(TransportClient client, String indexName, String typeName) {

		BoolQueryBuilder boolQ = QueryBuilders.boolQuery().filter(QueryBuilders.matchPhraseQuery("full_name", "wj"));
		SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(typeName).setSize(100);

		HighlightBuilder highlightBuilder = new HighlightBuilder().field("email").requireFieldMatch(false);
		highlightBuilder.preTags("<span style=\"color:red\">");
		highlightBuilder.postTags("</span>");
		srb.highlighter(highlightBuilder);

		SearchResponse response = srb.setQuery(boolQ).execute().actionGet();

		SearchHits result = response.getHits();
		long num = result.getTotalHits();

		SearchHit[] resultHit = result.getHits();
		for (int i = 0; i < num; i++) {
			System.out.println("总的显示：");
			System.out.println(resultHit[i].getSource().toString());

			System.out.println("处理高亮部分...");
			Map<String, HighlightField> highlightFields = resultHit[i].getHighlightFields();
			// 注意这里email在索引映射中必须stored 为true
			HighlightField titleField = highlightFields.get("email");

			Text[] fragments = titleField.fragments();
			StringBuilder builder = new StringBuilder();
			for (Text item : fragments) {
				builder.append(item.toString());
			}
			System.out.println("高亮部分：" + builder.toString());

		}

	}


}
