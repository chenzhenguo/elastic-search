package forbdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class KetiQuery {
    public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    public static String hostname = "10.110.13.176";
    //    public static String hostname = "localhost";
    public static String clustername = "es";
    public static String index = "test001";
    public static String type = "type001";
    public static ObjectMapper mapper = new ObjectMapper();
    public static TransportClient getClient1withNOxpack() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", clustername).build();
        TransportClient client = new PreBuiltTransportClient(settings);
//        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9300));
        return client;
    }

    /**
     * 客体和权利人
     * 不动产单元号 行政区划
     展示包含权利人、证件号
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
        SearchResponse  response = responsebuilder.setQuery(boolQueryQueryBuilder)
                .execute().actionGet();
        SearchHits hits = response.getHits();
//        System.out.println("客体："+hits.getTotalHits());
        int temp = 0;
        for (int i = 0; i < hits.getHits().length; i++) {
            String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("bdcdyh:"+hits.getHits()[i].getSource().get("bdcdyh"));
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
                    + hits.getHits()[i].getSource().get("postDate")+"\n");
           /* BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("bdcdyh", bdcdyh))
                    .should(QueryBuilders.termQuery("records", "0"));*/
            BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("bdcdyh", bdcdyh))
                    .must(QueryBuilders.termQuery("records", "0"));
            SearchResponse  response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1)
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
        System.out.println("    2:"+formatDate.format(new Date()));
    }

    /**
     * 客体无条件查询，先查出客体，再查询权利人
     * 客体和权利人
     * 不动产单元号 行政区划
     展示包含权利人、证件号
     * @param client
     * @throws UnknownHostException
     */
    public static void noconditionQueryKetiqlr(TransportClient client) throws UnknownHostException {
        long a1 = Calendar.getInstance().getTimeInMillis();
        SearchRequestBuilder responsebuilder = client.prepareSearch("keti").setTypes("keti");
        SearchRequestBuilder responsebuilder1 = client.prepareSearch("qlr").setTypes("qlr");

        //term查不出来，估计是分词了
//        TermQueryBuilder boolQueryQueryBuilder = QueryBuilders.termQuery("bdcdyh", "620522865057GB22282F729085473");
        MatchQueryBuilder boolQueryQueryBuilder = QueryBuilders.matchQuery("bdcdyh", "130209109640GB04190F344126562");
        SearchResponse  response = responsebuilder.setQuery(boolQueryQueryBuilder)
                .execute().actionGet();
        SearchHits hits = response.getHits();
//        System.out.println("客体："+hits.getTotalHits());
        int temp = 0;
        for (int i = 0; i < hits.getHits().length; i++) {
            String bdcdyh = String.valueOf(hits.getHits()[i].getSource().get("bdcdyh"));
            System.out.print("bdcdyh:"+hits.getHits()[i].getSource().get("bdcdyh"));
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
                    + hits.getHits()[i].getSource().get("postDate")+"\n");
           /* BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("bdcdyh", bdcdyh))
                    .should(QueryBuilders.termQuery("records", "0"));*/
            BoolQueryBuilder boolQueryQueryBuilder1 = QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("bdcdyh", bdcdyh))
                    .must(QueryBuilders.termQuery("records", "0"));
            SearchResponse  response1 = responsebuilder1.setQuery(boolQueryQueryBuilder1)
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
        System.out.println("    2:"+formatDate.format(new Date()));
    }
    //最简单没条件查询，查询某个索引下的某个类型
    public static void simpleQueryWithoutMethodqlr(TransportClient client) throws UnknownHostException {
//        long a1 = Calendar.getInstance().getTimeInMillis();
        SearchResponse response = client.prepareSearch("qlr").setTypes("qlr").setSize(100).execute().actionGet();
        SearchHits hits = response.getHits();
        System.out.println("获取记录："+hits.getTotalHits()+"条");
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
                    + hits.getHits()[i].getSource().get("postDate")+"\n");
        }
//        long a2 = Calendar.getInstance().getTimeInMillis();
//        System.out.println(String.valueOf(a2-a1).concat("毫秒"));
//        System.out.println("---------------------------------------------------------");
    }

    //最简单没条件查询，查询某个索引下的某个类型
    public static void simpleQueryWithoutMethodKeti(TransportClient client) throws UnknownHostException {
//        long a1 = Calendar.getInstance().getTimeInMillis();
        SearchResponse response = client.prepareSearch("keti").setTypes("keti").setSize(100).execute().actionGet();
        SearchHits hits = response.getHits();
        System.out.println("获取记录："+hits.getTotalHits()+"条");
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
                    + hits.getHits()[i].getSource().get("postDate")+"\n");
        }
//        long a2 = Calendar.getInstance().getTimeInMillis();
//        System.out.println(String.valueOf(a2-a1).concat("毫秒"));
//        System.out.println("---------------------------------------------------------");
    }
    public static void main(String[] args) throws UnknownHostException {
        long all = 0;
        int count = 10;
        TransportClient client = getClient1withNOxpack();
//        System.out.println(formatDate.format(new Date()));
        for(int i=0;i< count;i++) {
            long a1 = Calendar.getInstance().getTimeInMillis();
//            simpleQueryWithoutMethodKeti(client);
            conditionQueryWithoutMethodKetiqlr(client);
//            simpleQueryWithoutMethodqlr(client);
            long a2 = Calendar.getInstance().getTimeInMillis();
            System.out.println(String.valueOf(a2 - a1).concat("毫秒"));
            all = all+a2-a1;
        }
        System.out.println("查询"+count+"次，平均"+String.valueOf(all/count).concat("毫秒"));
//        conditionQueryWithoutMethodKetiqlr(client);
        client.close();
//        System.out.println(formatDate.format(new Date()));
    }
}
