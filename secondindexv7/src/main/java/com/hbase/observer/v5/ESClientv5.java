package com.hbase.observer.v5;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ESClientv5 {
	private static final Log log = LogFactory.getLog(ESClientv5.class);
    // ElasticSearch的集群名称
    public static String clusterName;
    // ElasticSearch的host
    public static String nodeHost;
    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
    public static int nodePort;
    public static TransportClient client;

    /**
     * get Es config
     *
     * @return
     */
    public static String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : ESClientv5.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(null));
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
        return StringUtils.join(fields, ", ");
    }

    /**
     * init ES client
     */
    public static void initEsClient() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", ESClientv5.clusterName)
                .put("client.transport.sniff","true")
                .put("transport.type", "netty3")
                .put("http.type", "netty3")
                .build();
        client = new PreBuiltTransportClient(settings);
        String[] hosts = ESClientv5.nodeHost.split(";");
        for(String host:hosts){
        	client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), ESClientv5.nodePort));
        }
}

    /**
     * Close ES client
     */
    public static void closeEsClient() {
        client.close();
    }
}
