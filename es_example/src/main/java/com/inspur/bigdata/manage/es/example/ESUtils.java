package com.inspur.bigdata.manage.es.example;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * elasticsearch操作工具类
 * 
 */
public class ESUtils {


	public static TransportClient getClient(String clustername,String hostname) throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", clustername).build();
		TransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9300));
		return client;
	}

	/**
	 * 索引列表
	 * @throws UnknownHostException
	 */
	public static void  listIndex(TransportClient client) throws UnknownHostException {
		ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet();
		// 获取所有索引
		String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
		for (String index : indexs) {
			System.out.println(index);
		}
	}

	/**
	 * 创建索引
	 * @throws UnknownHostException
	 */
	public static void createIndex(TransportClient client,String indexName) throws UnknownHostException {
		CreateIndexRequest cIndexRequest = new CreateIndexRequest(indexName);
		CreateIndexResponse cIndexResponse = client.admin().indices().create(cIndexRequest).actionGet();
		if (cIndexResponse.isAcknowledged()) {
			System.out.println("索引创建成功");
		} else {
			System.out.println("索引创建失败");
		}
	}

	/**
	 * 判断索引是否存在
	 * @throws UnknownHostException
	 */
	public static void existIndex(TransportClient client,String indexName) throws UnknownHostException {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		IndicesExistsResponse response = indicesAdminClient.prepareExists(indexName).get();
		if (response.isExists()) {
			System.out.println("索引存在");
		} else {
			System.out.println("索引不存在");
		}
	}

	public static void listTypes(TransportClient client,String indexName) throws UnknownHostException {
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
	 * 写入数据
	 * @throws IOException
	 */
	public static void writeDocument(TransportClient client,String indexName,String typeName) throws IOException {
		XContentBuilder jsonBuild1 = XContentFactory.jsonBuilder();
		XContentBuilder jsonBuild2 = XContentFactory.jsonBuilder();
		XContentBuilder jsonBuild3 = XContentFactory.jsonBuilder();
		XContentBuilder jsonBuild4 = XContentFactory.jsonBuilder();
		XContentBuilder jsonBuild5 = XContentFactory.jsonBuilder();
		ObjectMapper mapper = new ObjectMapper();
		List<String> jsonData = new ArrayList<String>();
		String data1 = jsonBuild1.startObject().field("id", "1").field("title", "git简介").field("date", "2016-06-19")
				.field("message", "SVN与Git最主要的区别").endObject().string();
		String data2 = jsonBuild2.startObject().field("id", "2").field("title", "Java中泛型的介绍与简单使用")
				.field("date", "2016-06-19").field("message", "学习目标掌握泛型的产生意义").endObject().string();

		String data3 = jsonBuild3.startObject().field("id", "3").field("title", "SQL基本操作").field("date", "2016-06-19")
				.field("message", "基本操作：CRUD").endObject().string();

		String data4 = jsonBuild4.startObject().field("id", "4").field("title", "Hibernate框架基础")
				.field("date", "2016-06-19").field("message", "Hibernate框架基础").endObject().string();
		String data5 = jsonBuild5.startObject().field("id", "5").field("title", "Shell基本知识").field("date", "2016-06-19")
				.field("message", "Shell是什么").endObject().string();
		jsonData.add(data1);
		jsonData.add(data2);
		jsonData.add(data3);
		jsonData.add(data4);
		jsonData.add(data5);
		for (int i = 0; i < jsonData.size(); i++) {
			IndexResponse response = client.prepareIndex(indexName, typeName).setId(String.valueOf(i + 1))
					.setSource(jsonData.get(i), XContentType.JSON).get();
		}
		System.out.println("写入成功");
	}

	/**
	 * 查询数据
	 * @throws UnknownHostException
	 * @throws JsonProcessingException
	 */
	public static void queryDocument(TransportClient client,String indexName,String typeName) throws UnknownHostException, JsonProcessingException {
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
	 * @throws UnknownHostException
	 */
	public static void deleteDocument(TransportClient client,String indexName,String typeName,String documentID) throws UnknownHostException {
		DeleteResponse response = client.prepareDelete(indexName, typeName, documentID).get();
	}
}
