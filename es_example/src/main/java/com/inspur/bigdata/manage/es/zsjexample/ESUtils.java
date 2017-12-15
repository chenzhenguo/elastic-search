package com.inspur.bigdata.manage.es.zsjexample;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * elasticsearch操作工具类
 * 
 */

public class ESUtils {

	public static void main(String[] args) throws UnknownHostException, IOException {
		// createIndex(getClient("es", "10.110.13.176"), "ql3", "ql3", 5, 3);
		// writeDocumentByPrepareIndex(getClient(), "tohdfs", "tohdfs");

		// writeDocumentByIndexRequest(getClient(), "tohdfs", "tohdfs");

		UpdateDocumentByPrepareUpdate(getClient(), "tohdfs", "tohdfs");
		// writeDocument
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
				.put("http.type", "netty4").build();
		TransportClient client = new PreBuiltTransportClient(settings);
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
	public static void writeDocumentByPrepareIndex(TransportClient client, String indexName, String typeName)
			throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "510504001004GB00080W00000012");

		String jsondata = JSON.toJSONString(parm);
		jsondata = jsondata.replace("[", "").replace("]", "");

		System.out.println(jsondata);

		IndexResponse response = client.prepareIndex(indexName, typeName).setId("3")
				.setSource(jsondata, XContentType.JSON).get();

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
	public static void writeDocumentByIndexRequest(TransportClient client, String indexName, String typeName)
			throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "510504001004GB00080W00000000");

		// String jsondata = JSON.toJSONString(parm);

		System.out.println(parm);

		IndexResponse response = client.index(new IndexRequest(indexName, typeName, "2").source(parm)).actionGet();

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
	public static void UpdateDocumentByUpdateRequest(TransportClient client, String indexName, String typeName)
			throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "121212121");

		// String jsondata = JSON.toJSONString(parm);

		System.out.println(parm);

		UpdateResponse response = client.update(new UpdateRequest(indexName, typeName, "0").doc(parm)).actionGet();

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
	public static void UpdateDocumentByPrepareUpdate(TransportClient client, String indexName, String typeName)
			throws IOException {

		Map<String, String> parm = new HashMap<String, String>(1);

		parm.put("BDCDYH", "123");

		// String jsondata = JSON.toJSONString(parm);

		System.out.println(parm);

		UpdateResponse response = client.prepareUpdate(indexName, typeName, "0").setDoc(parm, XContentType.JSON).get();

		System.out.println("修改成功,分片信息" + response.getShardInfo().toString());

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
	 * 删除数据
	 * 
	 * @throws UnknownHostException
	 */
	public static void deleteDocument(TransportClient client, String indexName, String typeName, String documentID)
			throws UnknownHostException {
		DeleteResponse response = client.prepareDelete(indexName, typeName, documentID).get();
	}
}
