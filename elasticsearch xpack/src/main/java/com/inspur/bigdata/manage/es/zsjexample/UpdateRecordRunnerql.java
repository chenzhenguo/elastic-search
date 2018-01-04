package com.inspur.bigdata.manage.es.zsjexample;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class UpdateRecordRunnerql extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(UpdateRecordRunnerql.class);
    CountDownLatch latch;
    TransportClient client;
    String key;
    Map<String, String> value;
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");

    public UpdateRecordRunnerql(TransportClient client, Map<String, String> value, String key, CountDownLatch latch) {
        this.latch = latch;
        this.key = key;
        this.value = value;
        this.client = client;
    }

    public void run() {
//        Settings settings = Settings.builder()
//                .put("cluster.name", "es").build();
//        TransportClient client1 = new PreBuiltTransportClient(settings);
////        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
////                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
//        try {
//            client1.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.177"), 9300))
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.176"), 9300))
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.175"), 9300));
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
        System.out.println(key + "开始" + sdf.format(new Date()));
        long a1 = Calendar.getInstance().getTimeInMillis();
        SearchRequestBuilder responsebuilder = client.prepareSearch("ql10").setTypes("ql10");
        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", 0))
                .filter(QueryBuilders.prefixQuery("qx", key));
        TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qllxtj").field("qllx").size(30);
        responsebuilder.addAggregation(gradeTermsBuilder);
        responsebuilder.setQuery(boolQueryQueryBuilder);

        SearchResponse sr = responsebuilder.execute().actionGet();
        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        StringTerms gradeTerms = (StringTerms) aggMap.get("qllxtj");
        Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
        long a = 0;
        while (gradeBucketIt.hasNext()) {
            StringTerms.Bucket gradeBucket = gradeBucketIt.next();
            System.out.println(key+"    "+gradeBucket.getKey() + "类型有" + gradeBucket.getDocCount() + "条记录。");
            a = a + gradeBucket.getDocCount();
        }
        long a2 = Calendar.getInstance().getTimeInMillis();
        System.out.println(key+"总共记录条数：" + String.valueOf(a));
        System.out.println(key + "结束" + sdf.format(new Date()));
        System.out.println(key + "花费:" + (a2 - a1) + "ms");
        value.put(key, String.valueOf(a));
        latch.countDown();
//        client1.close();
    }
}
