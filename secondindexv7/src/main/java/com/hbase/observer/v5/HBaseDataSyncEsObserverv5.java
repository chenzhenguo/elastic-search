package com.hbase.observer.v5;

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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.rest.RestStatus;

/**
 * v4的137行在hbase批量提交时会报错java.util.ConcurrentModificationException。
 * hosts用分号分开，不能用逗号
 * 放开ElasticSearchBulkOperatorv5功能。
 */
public class HBaseDataSyncEsObserverv5 extends BaseRegionObserver {
	private static final Log log = LogFactory.getLog(HBaseDataSyncEsObserverv5.class);

	private static final String INDEXNAME = "hbaseindex";
	// private static Map<String,String> columnESName = new
	// HashMap<String,String>();
	// hbase列名，索引名和类型，索引列名。索引的mapping在保存索引配置时就创建了。
	private static Map<String, Map<String, String>> columnESName = new HashMap<String, Map<String, String>>();

	// 索引名和类型,索引的列名和列值
	private static Map<String, Map<String, Object>> columnESValue = new HashMap<String, Map<String, Object>>();
	private static BulkRequestBuilder bulkRequestBuilder = null;

	private static void readConfiguration(CoprocessorEnvironment env) {
		String tableName = "";
		if (env instanceof RegionCoprocessorEnvironment) {
			RegionCoprocessorEnvironment envregion = (RegionCoprocessorEnvironment) env;
			tableName = envregion.getRegionInfo().getTable().getNameAsString();
		}
		Configuration conf = env.getConfiguration();
		ESClientv5.clusterName = conf.get("es_cluster");
		ESClientv5.nodeHost = conf.get("es_host");
		ESClientv5.nodePort = conf.getInt("es_port", 9300);
		try {
			Table table = env.getTable(TableName.valueOf(INDEXNAME));
			Scan scan = new Scan();
			ResultScanner rs = null;
			rs = table.getScanner(scan);
			for (Result r : rs) {
				String rowkey = Bytes.toString(r.getRow());
				String[] tabatt = rowkey.split(",");
				if (tableName.equals(tabatt[0])) {
					for (Cell cell : r.rawCells()) {
						String columnNameTemp = Bytes.toString(CellUtil.cloneQualifier(cell));
						if (columnESName.get(columnNameTemp) == null) {
							columnESName.put(columnNameTemp, new HashMap<String, String>());
						}
						if (columnESValue.get(tabatt[1].concat(",").concat(tabatt[2])) == null) {
							columnESValue.put(tabatt[1].concat(",").concat(tabatt[2]), new HashMap<String, Object>());
						}
						String columnValueTemp = Bytes.toString(CellUtil.cloneValue(cell));

						columnESName.get(columnNameTemp).put(tabatt[1].concat(",").concat(tabatt[2]),
								(columnValueTemp.split(",")[0]));
					}
				}
			}
		} catch (IOException e) {
			String fullStackTrace = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e);
			log.error(fullStackTrace);
		}
	}

	/**
	 * 这个方法会在regionserver打开region时候执行
	 */
	public void start(CoprocessorEnvironment e) throws IOException {
		readConfiguration(e);
		ESClientv5.initEsClient();
		bulkRequestBuilder = ESClientv5.client.prepareBulk();
	}

	public void stop(CoprocessorEnvironment e) throws IOException {
		ESClientv5.closeEsClient();

		// ElasticSearchBulkOperatorv5.shutdownScheduEx();
	}

	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		String indexId = new String(put.getRow());
		try {
			NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
			for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
				for (Cell cell : entry.getValue()) {
					String family = Bytes.toString(CellUtil.cloneFamily(cell));
					String key = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					Map<String, String> t1 = columnESName.get(family.concat(":").concat(key));
					if (t1 != null) {
						// key1是索引名和类型
						for (String key1 : t1.keySet()) {
							columnESValue.get(key1).put(t1.get(key1), value);
						}
					}
				}
			}
			//拼request
			for (String key2 : columnESValue.keySet()) {
				String[] key2temp = key2.split(",");
//				bulkRequestBuilder.add(ESClientv5.client.prepareUpdate(key2temp[0], key2temp[1], indexId)
//						.setDocAsUpsert(true).setDoc(columnESValue.get(key2)));
				ElasticSearchBulkOperatorv5.addUpdateBuilderToBulk
				(ESClientv5.client.prepareUpdate(key2temp[0], key2temp[1], indexId).setDocAsUpsert(true).setDoc(columnESValue.get(key2)));
			}
			//清空数值
			for (String key3 : columnESValue.keySet()) {
				columnESValue.put(key3, new HashMap<String, Object>());
			}
			
		} catch (Exception ex) {
			String fullStackTrace = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(ex);
			log.error(fullStackTrace);
		}
	}

	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
			Durability durability) throws IOException {
		String indexId = new String(delete.getRow());
		try {
			for (String key : columnESValue.keySet()) {
				String[] keytemp = key.split(",");
//				DeleteResponse response = ESClientv5.client.prepareDelete(keytemp[0], keytemp[1], indexId).get();
//				if(!response.status().equals(RestStatus.FOUND)){
//					log.error("删除索引数据报错:index:"+keytemp[0]+",type:"+keytemp[1]+",id:"+indexId+",status:"+response.status());
//				}
				ElasticSearchBulkOperatorv5.addDeleteBuilderToBulk(ESClientv5.client.prepareDelete(keytemp[0],keytemp[1], indexId));
			}
		} catch (Exception ex) {
			log.error(ex);
		}
	}
}
