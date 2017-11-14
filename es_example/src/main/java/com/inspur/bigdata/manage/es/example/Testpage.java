package com.inspur.bigdata.manage.es.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Testpage
{
  public static Logger logger = LoggerFactory.getLogger(Testpage.class);
  public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
  public static String hostname = "10.110.18.47";
  public static String clustername = "es";
  public static String index001 = "test001";
  public static String type001 = "type001";
  public static ObjectMapper mapper = new ObjectMapper();
  
  public static void bulkWriteMethod(TransportClient client)
    throws IOException
  {
    BulkProcessor bulkProcessor = BulkProcessor.builder(
      client, 
      new BulkProcessor.Listener()
      {
        public void beforeBulk(long executionId, BulkRequest request) {}
        
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}
        
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {}
      }).setBulkActions(1000)
      .setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB))
      .setFlushInterval(TimeValue.timeValueSeconds(5L))
      .setConcurrentRequests(1)
      .setBackoffPolicy(
      BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100L), 3))
      .build();
    for (int i = 1; i < 10001; i++) {
      bulkProcessor.add(new IndexRequest("pageindex", "tweet", String.valueOf(i))
        .source(XContentFactory.jsonBuilder()
        .startObject()
        .field("idstr", String.valueOf(i))
        .field("user", "kimchy".concat(String.valueOf(i)))
        .field("postDate", new Date())
        .endObject()));
    }
    bulkProcessor.close();
  }
  
  public static TransportClient getClient1withNOxpack()
    throws UnknownHostException
  {
    Settings settings = Settings.builder()
      .put("cluster.name", clustername).put("transport.type", "netty4")
      .put("http.type", "netty4").build();
    TransportClient client = new PreBuiltTransportClient(settings, new Class[0]);
    

    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9300));
    return client;
  }
  
  public static void listIndex(TransportClient client)
    throws UnknownHostException
  {
    ClusterStateResponse response = 
    
      (ClusterStateResponse)client.admin().cluster().prepareState().execute().actionGet();
    
    String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
    for (String index : indexs) {
      System.out.println(index);
    }
  }
  
  public static void bulkWriteOneByOneMethod(TransportClient client)
    throws IOException
  {
    XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    System.out.println("aaa" + formatDate.format(new Date()));
    for (int i = 1; i < 100000000; i++) {
      bulkRequest.add(client.prepareIndex("pageindex", "tweet", String.valueOf(i))
        .setSource(XContentFactory.jsonBuilder()
        .startObject()
        .field("idstr", String.valueOf(i))
        .field("user", "kimchy".concat(String.valueOf(i)))
        .field("postDate", new Date())
        .endObject()));
    }
    BulkResponse bulkResponse = (BulkResponse)bulkRequest.get();
    System.out.println("bbb" + formatDate.format(new Date()));
    if (bulkResponse.hasFailures()) {
      System.out.println(bulkResponse.toString());
    }
  }
  
  public static void main(String[] args)
  {
    try
    {
      SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
      TransportClient client = getClient1withNOxpack();
      System.out.println(formatDate.format(new Date()));
      bulkWriteMethod(client);
      

      client.close();
      System.out.println(formatDate.format(new Date()));
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
