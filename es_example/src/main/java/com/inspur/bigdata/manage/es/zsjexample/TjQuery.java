package com.inspur.bigdata.manage.es.zsjexample;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TjQuery {
    public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    public static String hostname = "10.110.13.177";
    //    public static String hostname = "localhost";
    public static String clustername = "es";
    public static String index = "test001";
    public static String type = "type001";
    public static ObjectMapper mapper = new ObjectMapper();

    public static TransportClient getClient1withNOxpack() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", clustername).build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.177"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.178"), 9300));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.13.176"), 9300));
        return client;
    }

    /**
     * 客体和权利人
     * 不动产单元号 行政区划
     * 展示包含权利人、证件号
     *
     * @param client
     * @throws UnknownHostException
     */
    public static void conditionQueryWithoutMethodKetiqlr(TransportClient client) throws UnknownHostException {
        long a1 = Calendar.getInstance().getTimeInMillis();
        SearchRequestBuilder responsebuilder = client.prepareSearch("keti").setTypes("keti");
        SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");

        //term查不出来，估计是分词了
//        TermQueryBuilder boolQueryQueryBuilder = QueryBuilders.termQuery("bdcdyh", "620522865057GB22282F729085473");
        MatchQueryBuilder boolQueryQueryBuilder = QueryBuilders.matchQuery("bdcdyh", "130209109640GB04190F344126562");
        SearchResponse response = responsebuilder.setQuery(boolQueryQueryBuilder)
                .execute().actionGet();
        SearchHits hits = response.getHits();
//        System.out.println("客体："+hits.getTotalHits());
        int temp = 0;
        for (int i = 0; i < hits.getHits().length; i++) {
            String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("bdcdyh:" + hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("\tlx:"
                    + hits.getHits()[i].getSource().get("lx"));
            System.out.print("\tuuid:"
                    + hits.getHits()[i].getSource().get("uuid"));
            System.out.print("\tqx:"
                    + hits.getHits()[i].getSource().get("qx"));
            System.out.print("\tzl:"
                    + hits.getHits()[i].getSource().get("zl"));
            System.out.print("\trecords:"
                    + hits.getHits()[i].getSource().get("records"));
            System.out.print("\tpostDate:"
                    + hits.getHits()[i].getSource().get("postDate") + "\n");
           /* BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("bdcdyh", bdcdyh))
                    .should(QueryBuilders.termQuery("records", "0"));*/
            BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("bdcdyh", bdcdyh))
                    .must(QueryBuilders.termQuery("records", "0"));
            SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1)
                    .execute().actionGet();
            SearchHits hits1 = response1.getHits();
//            System.out.println("权利人："+hits1.getTotalHits());
            for (int j = 0; j < hits1.getHits().length; j++) {
                System.out.print("\txm:"
                        + hits1.getHits()[j].getSource().get("xm"));
                System.out.print("\tzjh:"
                        + hits1.getHits()[j].getSource().get("zjh"));
                System.out.print("\trecords:"
                        + hits1.getHits()[j].getSource().get("records"));
            }
            System.out.println("");
        }
        long a2 = Calendar.getInstance().getTimeInMillis();
        System.out.println("    2:" + formatDate.format(new Date()));
    }

    /**
     * 客体无条件查询，先查出客体，再查询权利人
     * 客体和权利人
     * 不动产单元号 行政区划
     * 展示包含权利人、证件号
     *
     * @param client
     * @throws UnknownHostException
     */
    public static void noconditionQueryKetiqlrnoSystem(TransportClient client) throws UnknownHostException {
       /* SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr1").setTypes("qlr1");
        SearchResponse response = client.prepareSearch("keti1").setTypes("keti1").setSize(100).execute().actionGet();
        SearchHits hits = response.getHits();
        for (int i = 0; i < hits.getHits().length; i++) {
            String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
           *//* BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("bdcdyh", bdcdyh))
                    .must(QueryBuilders.termQuery("records", "0"));
            SearchResponse  response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1)
                    .execute().actionGet();
            SearchHits hits1 = response1.getHits();*//*
            BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("bdcdyh", bdcdyh))
                    .filter(QueryBuilders.termQuery("records", "0"));
            SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1)
                    .execute().actionGet();
            SearchHits hits1 = response1.getHits();
            for (int j = 0; j < hits1.getHits().length; j++) {
            }
        }*/
        SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");
        SearchResponse response = client.prepareSearch("keti").setTypes("keti").setSize(100).execute().actionGet();
        SearchHits hits = response.getHits();
        List<String> list = new ArrayList<String>();

        for (int i = 0; i < hits.getHits().length; i++) {
            String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
            list.add(bdcdyh);
        }
//        TermsQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.termsQuery("bdcdyh", list);
        BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("bdcdyh", list))
                .filter(QueryBuilders.termQuery("records", "0"));
        SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1)
                .execute().actionGet();
        SearchHits hits1 = response1.getHits();
        for (int j = 0; j < hits1.getHits().length; j++) {
        }
    }

    public static void noconditionQueryKetiqlrWithSystem(TransportClient client) throws UnknownHostException {
        SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");
        SearchResponse response = client.prepareSearch("keti").setTypes("keti").setSize(50).execute().actionGet();
        SearchHits hits = response.getHits();
        for (int i = 0; i < hits.getHits().length; i++) {
            String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("bdcdyh:" + hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("\tlx:"
                    + hits.getHits()[i].getSource().get("lx"));
            System.out.print("\tuuid:"
                    + hits.getHits()[i].getSource().get("uuid"));
            System.out.print("\tqx:"
                    + hits.getHits()[i].getSource().get("qx"));
            System.out.print("\tzl:"
                    + hits.getHits()[i].getSource().get("zl"));
            System.out.print("\trecords:"
                    + hits.getHits()[i].getSource().get("records"));
            System.out.print("\tpostDate:"
                    + hits.getHits()[i].getSource().get("postDate") + "\n");
            BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("bdcdyh", bdcdyh))
                    .must(QueryBuilders.termQuery("records", "0"));
            SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1)
                    .execute().actionGet();
            SearchHits hits1 = response1.getHits();
            for (int j = 0; j < hits1.getHits().length; j++) {
                System.out.print("\txm:"
                        + hits1.getHits()[j].getSource().get("xm"));
                System.out.print("\tzjh:"
                        + hits1.getHits()[j].getSource().get("zjh"));
                System.out.print("\trecords:"
                        + hits1.getHits()[j].getSource().get("records"));
            }
            System.out.println("");
        }
    }

    //最简单没条件查询，查询某个索引下的某个类型
    public static void simpleQueryWithoutMethodqlr(TransportClient client) throws UnknownHostException {
//        long a1 = Calendar.getInstance().getTimeInMillis();
        SearchResponse response = client.prepareSearch("qlr").setTypes("qlr").setSize(100).execute().actionGet();
        SearchHits hits = response.getHits();
        System.out.println("获取记录：" + hits.getTotalHits() + "条");
        int temp = 0;
        for (int i = 0; i < hits.getHits().length; i++) {
            System.out.print(hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("zjh"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("uuid"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("xm"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("dw"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("records"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("postDate") + "\n");
        }
//        long a2 = Calendar.getInstance().getTimeInMillis();
//        System.out.println(String.valueOf(a2-a1).concat("毫秒"));
//        System.out.println("---------------------------------------------------------");
    }

    //最简单没条件查询，查询某个索引下的某个类型
    public static void simpleQueryWithoutMethodKeti(TransportClient client) throws UnknownHostException {
//        long a1 = Calendar.getInstance().getTimeInMillis();
        SearchResponse response = client.prepareSearch("keti1").setTypes("keti1").setSize(100).execute().actionGet();
        SearchHits hits = response.getHits();
        System.out.println("获取记录：" + hits.getTotalHits() + "条");
        int temp = 0;
        for (int i = 0; i < hits.getHits().length; i++) {
            System.out.print(hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("lx"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("uuid"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("qx"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("zl"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("records"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("postDate") + "\n");
        }
//        long a2 = Calendar.getInstance().getTimeInMillis();
//        System.out.println(String.valueOf(a2-a1).concat("毫秒"));
//        System.out.println("---------------------------------------------------------");
    }

    /**
     * 查询某类型的客体的前100条
     *
     * @param client
     * @param count
     * @param leixing
     * @throws UnknownHostException
     */
    public static void queryKetiWithLeixing(TransportClient client, int count, String leixing) throws UnknownHostException {
        SearchRequestBuilder responsebuilder = client.prepareSearch("keti1").setTypes("keti1");
        BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("lx", leixing))
                .filter(QueryBuilders.termQuery("records", "0"));
        SearchResponse response = responsebuilder.setQuery(boolQueryQueryBuilder1).setSize(count)
                .execute().actionGet();
        SearchHits hits = response.getHits();
        for (int i = 0; i < hits.getHits().length; i++) {
            System.out.print(hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("lx"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("uuid"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("qx"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("zl"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("records"));
            System.out.print("\t"
                    + hits.getHits()[i].getSource().get("postDate") + "\n");
        }
    }

    public static void queryKetiCountWithLeixing(TransportClient client, int count, String leixing) throws UnknownHostException {
        SearchRequestBuilder responsebuilder = client.prepareSearch("keti").setTypes("keti");
        BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("lx", leixing))
                .filter(QueryBuilders.termQuery("records", "0"));
        SearchResponse response = responsebuilder.setQuery(boolQueryQueryBuilder1).setSize(count)
                .execute().actionGet();
        SearchHits hits = response.getHits();
        //
//        System.out.print( hits.getHits().length + "\n");
        long length = response.getHits().getTotalHits();
        System.out.print("总共记录:".concat(String.valueOf(length) + "条\n"));

    }

    /**
     * 按地区统计客体数量
     *
     * @param client
     * @throws UnknownHostException
     */
    public static void andiqutongjiketishuliang(TransportClient client) throws UnknownHostException {
//        SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1").setSize(100);
        SearchRequestBuilder srb = client.prepareSearch("keti").setTypes("keti");
        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", "0"));
        TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qllxtj").field("qx").size(4000);
//        ValueCountAggregationBuilder gradeTermsBuilder = AggregationBuilders.count("gradeAgg").field("qx");
        srb.setQuery(boolQueryQueryBuilder);
        srb.addAggregation(gradeTermsBuilder);

        SearchResponse sr = srb.execute().actionGet();
        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        StringTerms gradeTerms = (StringTerms) aggMap.get("qllxtj");
        Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
        long a = 0;
        while (gradeBucketIt.hasNext()) {
            StringTerms.Bucket gradeBucket = gradeBucketIt.next();
            System.out.println(gradeBucket.getKey() + "类型有" + gradeBucket.getDocCount() + "条记录。");
            a = a + gradeBucket.getDocCount();
        }
        System.out.println("总共记录条数：" + String.valueOf(a));
    }

    /**
     * 按省统计客体数量，30多个省市并发查询,省里面的客体类型需要聚合
     * @param client
     * @throws UnknownHostException
     */
    public static void anshengtongjiketishuliang(TransportClient client) throws Exception {
        CountDownLatch latch = new CountDownLatch(34);
        Map<String, String> value = new HashMap<String,String>();
        List<String> list = StaticValues.getShenglist();
        for(String sheng:list) {
            new UpdateRecordRunnerKeti(client, value,sheng, latch).start();
        }
        latch.await();
        System.out.println("---------------");
        int all = 0;
       for(String sheng:StaticValues.shenglist) {
            System.out.println(sheng+":"+value.get(sheng));
           all = all+ Integer.valueOf(value.get(sheng));
        }
        System.out.println("总共："+all);
    }
    /**
     * 按省统计权利数量，30多个省市并发查询,省里面的权利类型需要聚合
     * @param client
     * @throws UnknownHostException
     */
    public static void anshengtongjiqlshuliang(TransportClient client) throws Exception {
        CountDownLatch latch = new CountDownLatch(34);
        Map<String, String> value = new HashMap<String,String>();
        List<String> list = StaticValues.getShenglist();
        for(String sheng:list) {
            new UpdateRecordRunnerql(client, value,sheng, latch).start();
        }
        latch.await();
        System.out.println("---------------");
        int all = 0;
        for(String sheng:StaticValues.shenglist) {
            System.out.println(sheng+":"+value.get(sheng));
            all = all+ Integer.valueOf(value.get(sheng));
        }
        System.out.println("总共："+all);
    }
    /**
     * 按权利类型统计权利数量
     *
     * @param client
     * @throws UnknownHostException
     */
    public static void anquanlileixingshuliang(TransportClient client) throws UnknownHostException {
//        SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1").setSize(100);
        SearchRequestBuilder srb = client.prepareSearch("ql1").setTypes("ql1");
        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", "0"));
        TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qllxtj").field("qllx").size(30);
//        ValueCountAggregationBuilder gradeTermsBuilder = AggregationBuilders.count("gradeAgg").field("qx");
        srb.setQuery(boolQueryQueryBuilder);
        srb.addAggregation(gradeTermsBuilder);

        SearchResponse sr = srb.execute().actionGet();
        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        StringTerms gradeTerms = (StringTerms) aggMap.get("qllxtj");
        Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
        long a = 0;
        while (gradeBucketIt.hasNext()) {
            StringTerms.Bucket gradeBucket = gradeBucketIt.next();
            System.out.println(gradeBucket.getKey() + "类型有" + gradeBucket.getDocCount() + "条记录。");
            a = a + gradeBucket.getDocCount();
        }
        System.out.println("总共记录条数：" + String.valueOf(a));

    }

    /**
     * 按权利人类型统计权利人数量
     *
     * @param client
     * @throws UnknownHostException
     */
    public static void anquanlirenleixingshuliang(TransportClient client) throws UnknownHostException {
//        SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1").setSize(100);
        SearchRequestBuilder srb = client.prepareSearch("qlr1").setTypes("qlr1");
        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", "0"));
        TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qlrlxtj").field("lx").size(30);
//        ValueCountAggregationBuilder gradeTermsBuilder = AggregationBuilders.count("gradeAgg").field("qx");
        srb.setQuery(boolQueryQueryBuilder);
        srb.addAggregation(gradeTermsBuilder);

        SearchResponse sr = srb.execute().actionGet();
        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        StringTerms gradeTerms = (StringTerms) aggMap.get("qlrlxtj");
        Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
        long a = 0;
        while (gradeBucketIt.hasNext()) {
            StringTerms.Bucket gradeBucket = gradeBucketIt.next();
            System.out.println(gradeBucket.getKey() + "类型有" + gradeBucket.getDocCount() + "条记录。");
            a = a + gradeBucket.getDocCount();
        }
        System.out.println("总共记录条数：" + String.valueOf(a));

    }
    public static void anbudongchanleixing(TransportClient client) throws UnknownHostException {
//        SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1").setSize(100);
        SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1");
        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", "0"));
        TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qlrlxtj").field("lx").size(30);
//        ValueCountAggregationBuilder gradeTermsBuilder = AggregationBuilders.count("gradeAgg").field("qx");
        srb.setQuery(boolQueryQueryBuilder);
        srb.addAggregation(gradeTermsBuilder);

        SearchResponse sr = srb.execute().actionGet();
        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        StringTerms gradeTerms = (StringTerms) aggMap.get("qlrlxtj");
        Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
        long a = 0;
        while (gradeBucketIt.hasNext()) {
            StringTerms.Bucket gradeBucket = gradeBucketIt.next();
            System.out.println(gradeBucket.getKey() + "类型有" + gradeBucket.getDocCount() + "条记录。");
            a = a + gradeBucket.getDocCount();
        }
        System.out.println("总共记录条数：" + String.valueOf(a));

    }

    public static void quanli_quxian(TransportClient client) throws UnknownHostException {
//        SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1").setSize(100);
        SearchRequestBuilder srb = client.prepareSearch("ql1").setTypes("ql1");
        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", "0"));
        TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qlrlxtj").field("qx").size(4000);
//        ValueCountAggregationBuilder gradeTermsBuilder = AggregationBuilders.count("gradeAgg").field("qx");
        srb.setQuery(boolQueryQueryBuilder);
        srb.addAggregation(gradeTermsBuilder);

        SearchResponse sr = srb.execute().actionGet();
        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        StringTerms gradeTerms = (StringTerms) aggMap.get("qlrlxtj");
        Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
        long a = 0;
        while (gradeBucketIt.hasNext()) {
            StringTerms.Bucket gradeBucket = gradeBucketIt.next();
            System.out.println(gradeBucket.getKey() + "类型有" + gradeBucket.getDocCount() + "条记录。");
            a = a + gradeBucket.getDocCount();
        }
        System.out.println("总共记录条数：" + String.valueOf(a));

    }

    public static void noconditionQueryKetiqlrWithSystemList(TransportClient client) throws UnknownHostException {
        SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");
        SearchResponse response = client.prepareSearch("keti").setTypes("keti").setSize(100).execute().actionGet();
        SearchHits hits = response.getHits();
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < hits.getHits().length; i++) {
            String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
            list.add(bdcdyh);
            System.out.print("bdcdyh:" + hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("\tlx:"
                    + hits.getHits()[i].getSource().get("lx"));
            System.out.print("\tuuid:"
                    + hits.getHits()[i].getSource().get("uuid"));
            System.out.print("\tqx:"
                    + hits.getHits()[i].getSource().get("qx"));
            System.out.print("\tzl:"
                    + hits.getHits()[i].getSource().get("zl"));
            System.out.print("\trecords:"
                    + hits.getHits()[i].getSource().get("records"));
            System.out.print("\tpostDate:"
                    + hits.getHits()[i].getSource().get("postDate") + "\n");
        }
       BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("bdcdyh", list));
//        BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("bdcdyh", list))
//                .filter(QueryBuilders.termQuery("records", 0));
        SearchResponse response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1)
                .execute().actionGet();
        SearchHits hits1 = response1.getHits();
        for (int j = 0; j < hits1.getHits().length; j++) {
            System.out.print("\txm:"
                    + hits1.getHits()[j].getSource().get("xm"));
            System.out.print("\tzjh:"
                    + hits1.getHits()[j].getSource().get("zjh"));
            System.out.print("\trecords:"
                    + hits1.getHits()[j].getSource().get("records")+ "\n");
        }
        System.out.println("");
    }
    public static void multiSearchQueryKetiqlr(TransportClient client) throws UnknownHostException {
        SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");
        List<SearchRequestBuilder>  listaa = new ArrayList<SearchRequestBuilder>();
        MultiSearchRequestBuilder aa =  client.prepareMultiSearch();
        SearchResponse response = client.prepareSearch("keti").setTypes("keti").setSize(50).execute().actionGet();
        SearchHits hits = response.getHits();
        for (int i = 0; i < hits.getHits().length; i++) {
            String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
            BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("bdcdyh", bdcdyh));
//            listaa.add( client.prepareSearch("qlr").setTypes("qlr").setQuery(boolQueryQueryBuilder1));
            aa.add( client.prepareSearch("qlr").setTypes("qlr").setQuery(boolQueryQueryBuilder1));
            System.out.print("bdcdyh:" + hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("\tlx:"
                    + hits.getHits()[i].getSource().get("lx"));
            System.out.print("\tuuid:"
                    + hits.getHits()[i].getSource().get("uuid"));
            System.out.print("\tqx:"
                    + hits.getHits()[i].getSource().get("qx"));
            System.out.print("\tzl:"
                    + hits.getHits()[i].getSource().get("zl"));
            System.out.print("\trecords:"
                    + hits.getHits()[i].getSource().get("records"));
            System.out.print("\tpostDate:"
                    + hits.getHits()[i].getSource().get("postDate") + "\n");
        }
       MultiSearchResponse sr = aa.get();
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response11 = item.getResponse();
            SearchHits hits11 = response11.getHits();
            for (int j = 0; j < hits11.getHits().length; j++) {
                System.out.print("\txm:"
                        + hits11.getHits()[j].getSource().get("xm"));
                System.out.print("\tzjh:"
                        + hits11.getHits()[j].getSource().get("zjh"));
                System.out.print("\trecords:"
                        + hits11.getHits()[j].getSource().get("records")+ "\n");
            }
        }

    }

    /**
     * 权利索引按权利类型统计
     * @param client
     * @throws UnknownHostException
     */
    public static void anquanlileixtongji(TransportClient client) throws UnknownHostException {
//        SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1").setSize(100);
        SearchRequestBuilder srb = client.prepareSearch("ql_1y").setTypes("ql_1y");
        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", 0));
        TermsAggregationBuilder gradeTermsBuilder = AggregationBuilders.terms("qllxtj").field("lx").size(4000);
//        ValueCountAggregationBuilder gradeTermsBuilder = AggregationBuilders.count("gradeAgg").field("qx");
        srb.setQuery(boolQueryQueryBuilder);
        srb.addAggregation(gradeTermsBuilder);

        SearchResponse sr = srb.execute().actionGet();
        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        StringTerms gradeTerms = (StringTerms) aggMap.get("qllxtj");
        Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
        long a = 0;
        while (gradeBucketIt.hasNext()) {
            StringTerms.Bucket gradeBucket = gradeBucketIt.next();
            System.out.println(gradeBucket.getKey() + "类型有" + gradeBucket.getDocCount() + "条记录。");
            a = a + gradeBucket.getDocCount();
        }
        System.out.println("总共记录条数：" + String.valueOf(a));
    }

    public static void test(TransportClient client) throws UnknownHostException {
        long a1 = Calendar.getInstance().getTimeInMillis();
        SearchRequestBuilder responsebuilder = client.prepareSearch("keti").setTypes("keti").setSize(100);
        SearchResponse response = responsebuilder.execute().actionGet();
        SearchHits hits = response.getHits();
        System.out.println("客体111：" + hits.getTotalHits());
    }

    /**
     * distinct 去重取前50条
     * @param client
     * @throws UnknownHostException
     */
    public static void testcardinality(TransportClient client) throws UnknownHostException {
        SearchRequestBuilder srb = client.prepareSearch("qlr").setTypes("qlr").setSize(50);
        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", 0));
        CardinalityAggregationBuilder gradeTermsBuilder = AggregationBuilders.cardinality("abc").field("zjh");
        srb.setQuery(boolQueryQueryBuilder);
        srb.addAggregation(gradeTermsBuilder);
        SearchResponse sr = srb.execute().actionGet();
        Cardinality agg = sr.getAggregations().get("abc");
        long value = agg.getValue();
        SearchHit[]  hits= sr.getHits().getHits();
        for(SearchHit hit : hits) {
            String zjh = hit.getSource().get("zjh").toString();
            System.out.println(zjh);
        }
    }
    public static void testcardinalityBAK(TransportClient client1) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "es").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.110.18.47"), 9300));
        //        SearchRequestBuilder srb = client.prepareSearch("keti1").setTypes("keti1").setSize(100);
        SearchRequestBuilder srb = client.prepareSearch("cars").setTypes("transactions").setSize(2);
//        BoolQueryBuilder boolQueryQueryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("records", "0"));
        CardinalityAggregationBuilder gradeTermsBuilder = AggregationBuilders.cardinality("abc").field("color");
//        ValueCountAggregationBuilder gradeTermsBuilder = AggregationBuilders.count("gradeAgg").field("qx");
//        srb.setQuery(boolQueryQueryBuilder);
        srb.addAggregation(gradeTermsBuilder);

        SearchResponse sr = srb.execute().actionGet();
        Cardinality agg = sr.getAggregations().get("abc");
        long value = agg.getValue();
        SearchHit[]  hits= sr.getHits().getHits();
        for(SearchHit hit : hits) {
            String color = hit.getSource().get("color").toString();
            System.out.println(color);
        }
        client.close();
    }
    public static void main(String[] args) throws Exception {
        long all = 0;
        int count = 1;
        TransportClient client = getClient1withNOxpack();
//        System.out.println(formatDate.format(new Date()));
        for (int i = 0; i < count; i++) {
            long a1 = Calendar.getInstance().getTimeInMillis();
//            testcardinality(client);
//            simpleQueryWithoutMethodKeti(client);
//            conditionQueryWithoutMethodKetiqlr(client);
//            noconditionQueryKetiqlrWithSystem(client);
//            noconditionQueryKetiqlrnoSystem(client);
//            simpleQueryWithoutMethodqlr(client);
//            queryKetiWithLeixing(client,100,"15");
//            queryKetiCountWithLeixing(client,0,"16");
//            noconditionQueryKetiqlrWithSystemList(client);
//              multiSearchQueryKetiqlr(client);
//            andiqutongjiketishuliang(client);
//            anquanlileixingshuliang(client);
//            anquanlirenleixingshuliang(client);
//            anbudongchanleixing(client);
//            quanli_quxian(client);
//            test(client);
              //anshengtongjiketishuliang(client);
            anshengtongjiqlshuliang(client);
            //anquanlileixtongji(client);
            long a2 = Calendar.getInstance().getTimeInMillis();
            System.out.println(String.valueOf(a2 - a1).concat("毫秒"));
            all = all + a2 - a1;
        }
        System.out.println("查询" + count + "次，平均" + String.valueOf(all / count).concat("毫秒"));
//        conditionQueryWithoutMethodKetiqlr(client);
        client.close();
//        System.out.println(formatDate.format(new Date()));
    }
}
