package com.hbase.observer;

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
import org.elasticsearch.action.index.IndexRequest;

public class HBaseDataSyncEsObserver extends BaseRegionObserver {
	private static final Log log = LogFactory.getLog(HBaseDataSyncEsObserver.class);

	private static void readConfiguration(CoprocessorEnvironment env) {
		Configuration conf = env.getConfiguration();
		ESClient.clusterName = conf.get("es_cluster");
		ESClient.nodeHost = conf.get("es_host");
		ESClient.nodePort = conf.getInt("es_port", 9200);
		ESClient.indexName = conf.get("es_index");
		ESClient.typeName = conf.get("es_type");
		log.error("++++++123" + ESClient.clusterName);
		log.error("++++++123" + ESClient.nodeHost);
		log.error("++++++123" + ESClient.nodePort);
		log.error("++++++123" + ESClient.indexName);
		log.error("++++++123" + ESClient.typeName);
	}

	public void start(CoprocessorEnvironment e) throws IOException {
		log.error("------start123 ------");

		readConfiguration(e);

		ESClient.initEsClient();
		log.error("------observer init EsClient ------" + ESClient.getInfo());
		ClusterStateResponse response = (ClusterStateResponse) ESClient.client.admin().cluster().prepareState()
				.execute().actionGet();

		String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
		for (String index : indexs) {
			log.error("------index123 ------" + index);
		}
	}

	public void stop(CoprocessorEnvironment e) throws IOException {
		log.error("------stop123 ------");
		ESClient.closeEsClient();

		ElasticSearchBulkOperator.shutdownScheduEx();
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
			log.error("============4");
			ESClient.bulkProcessor.add(new IndexRequest(ESClient.indexName, ESClient.typeName, indexId).source(json));
			log.error("============5");
		} catch (Exception ex) {
			log.error("observer put  a doc, index [ " + ESClient.indexName + " ]" + "indexId [" + indexId + "] error : "
					+ ex.getMessage());
		}
	}

	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
			Durability durability) throws IOException {
		String indexId = new String(delete.getRow());
		try {
			ElasticSearchBulkOperator.addDeleteBuilderToBulk(
					ESClient.client.prepareDelete(ESClient.indexName, ESClient.typeName, indexId));
		} catch (Exception ex) {
			log.error(ex);
			log.error("observer delete  a doc, index [ " + ESClient.indexName + " ]" + "indexId [" + indexId
					+ "] error : " + ex.getMessage());
		}
	}
}
