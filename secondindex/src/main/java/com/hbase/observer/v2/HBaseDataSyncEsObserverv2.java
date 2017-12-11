package com.hbase.observer.v2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;

public class HBaseDataSyncEsObserverv2 extends BaseRegionObserver {
	private static final Log log = LogFactory.getLog(HBaseDataSyncEsObserverv2.class);

	private static void readConfiguration(CoprocessorEnvironment env) {
		Configuration conf = env.getConfiguration();
		ESClientv2.clusterName = conf.get("es_cluster");
		ESClientv2.nodeHost = conf.get("es_host");
		ESClientv2.nodePort = conf.getInt("es_port", 9200);
		ESClientv2.indexName = conf.get("es_index");
		ESClientv2.typeName = conf.get("es_type");
	}

	public void start(CoprocessorEnvironment e) throws IOException {
		log.error("------start123 ------");

		readConfiguration(e);

		ESClientv2.initEsClient();
		log.error("------observer init EsClient ------" + ESClientv2.getInfo());
		ClusterStateResponse response = (ClusterStateResponse) ESClientv2.client.admin().cluster().prepareState()
				.execute().actionGet();

		String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
		for (String index : indexs) {
			log.error("------index123 ------" + index);
		}
	}

	public void stop(CoprocessorEnvironment e) throws IOException {
		log.error("------stop123 ------");
		ESClientv2.closeEsClient();

		ElasticSearchBulkOperatorv2.shutdownScheduEx();
	}

	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		String indexId = new String(put.getRow());
		log.error("====indexId111 ==" + indexId);
		try {
			log.error("============1");
			NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
			Map<String, Object> infoJson = new HashMap();
			Map<String, Object> json = new HashMap();
			log.error("============2");
			for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
				log.error("============3");
				for (Cell cell : entry.getValue()) {
					String key = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					log.error("==key===" + key);
					log.error("=value====" + value);
					json.put(key, value);
				}
			}
			log.error("======json" + json);
			log.error("============001");

			BulkRequestBuilder bulkRequestBuilder = ESClientv2.client.prepareBulk();
			bulkRequestBuilder.add(ESClientv2.client.prepareUpdate(ESClientv2.indexName, ESClientv2.typeName, indexId)
					.setDocAsUpsert(true).setDoc(json));
			// UpdateRequestBuilder update1 =
			// ESClientv2.client.prepareUpdate(ESClientv2.indexName,ESClientv2.typeName,
			// "008").setDocAsUpsert(true).setDoc(json);
			// bulkRequestBuilder.add(update1);
			BulkResponse bulkItemResponse = bulkRequestBuilder.execute().actionGet();
			// ESClientv2.bulkProcessor.add(new
			// IndexRequest(ESClientv2.indexName, ESClientv2.typeName,
			// indexId).source(json));
			// ElasticSearchBulkOperatorv2.addUpdateBuilderToBulk(ESClient.client.prepareUpdate(ESClient.indexName,
			// ESClient.typeName,
			// indexId).setDocAsUpsert(true).setDoc(infoJson));
			/*
			 * UpdateRequestBuilder updateRequestBuilder =
			 * ESClientv2.client.prepareUpdate(ESClientv2.indexName,
			 * ESClientv2.typeName,
			 * indexId).setDocAsUpsert(true).setDoc(infoJson);
			 * if(updateRequestBuilder == null){ log.error("============0021");
			 * }else{ log.error("============0022"); }
			 * ElasticSearchBulkOperatorv2.addUpdateBuilderToBulk(
			 * updateRequestBuilder);
			 */
			log.error("============0031");
		} catch (Exception ex) {
			log.error("observer put  a doc, index [ " + ESClientv2.indexName + " ]" + "indexId [" + indexId
					+ "] error : " + ex.getMessage());
			String fullStackTrace = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(ex);
			System.out.println("打印异常2:" + fullStackTrace);
		}
	}

	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
			Durability durability) throws IOException {
		String indexId = new String(delete.getRow());
		try {
			ElasticSearchBulkOperatorv2.addDeleteBuilderToBulk(
					ESClientv2.client.prepareDelete(ESClientv2.indexName, ESClientv2.typeName, indexId));
		} catch (Exception ex) {
			log.error(ex);
			log.error("observer delete  a doc, index [ " + ESClientv2.indexName + " ]" + "indexId [" + indexId
					+ "] error : " + ex.getMessage());
		}
	}
}
