package com.inspur.bigdata.manage.es.wj;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import com.fasterxml.jackson.core.JsonProcessingException;

public class Esutil {
	public static void main(String[] params) throws UnknownHostException, JsonProcessingException {
		TransportClient client = new PreBuiltXPackTransportClient(
				Settings.builder().put("cluster.name", "es").put("xpack.security.user", "wj:123456").build())
						.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

		// listIndex(client);

		 queryDocument(client,"tohdfs","study1");
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
	 * 查询数据
	 * 
	 * @throws UnknownHostException
	 * @throws JsonProcessingException
	 */
	public static void queryDocument(TransportClient client, String indexName, String typeName)
			throws UnknownHostException, JsonProcessingException {
		SearchRequestBuilder srb = client.prepareSearch(indexName)
				.setTypes(typeName).setSize(100);

		BoolQueryBuilder boolQ = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("lx", 51));

		SearchResponse response = srb.setQuery(boolQ).execute().actionGet();

		SearchHits result = response.getHits();
		long num=result.getTotalHits();
		
		SearchHit[] resultHit=result.getHits();
		for (int i=0;i<num;i++){
		System.out.println(resultHit[i].getSource().get("name")+":"+resultHit[i].getSource().get("age"));	
		}


	}

}
