package com.hbase.observer.v4;

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

public class ESClientv4 {
	private static final Log log = LogFactory.getLog(ESClientv4.class);
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
            for (Field f : ESClientv4.class.getDeclaredFields()) {
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
                .put("cluster.name", ESClientv4.clusterName)
                .put("transport.type", "netty3")
                .put("http.type", "netty3")
                .build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ESClientv4.nodeHost), ESClientv4.nodePort));


}

    /**
     * Close ES client
     */
    public static void closeEsClient() {
        client.close();
    }
}
