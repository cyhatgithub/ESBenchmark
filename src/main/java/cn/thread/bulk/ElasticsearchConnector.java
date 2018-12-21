package cn.thread.bulk;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Created by chenyinghao on 2018/9/26.
 */
public class ElasticsearchConnector {

    public final static String HIGHLIGHT_TAG_PRE = "<span class='highlight'>";
    public final static String HIGHLIGHT_TAG_POST = "</span>";

    public final static String PATTERN_ALL = "*";

    protected TransportClient client;

    protected String esHosts;
    protected int esPort;
    protected String clusterName;

//    protected static TransportClient getInstance(String esHosts, int esPort, String clusterName) {
//        if(client == null) {
//            init(esHosts, esPort, clusterName);
//        }
//        return client;
//    }

    public ElasticsearchConnector(String esHosts, int esPort, String clusterName) {
        if((esHosts == null || esHosts.equals("")) || (clusterName == null || clusterName.equals(""))) {
            throw new RuntimeException("configuration 'esHosts' or 'clusterName' can not be null.");
        }
        this.esHosts = esHosts;
        this.esPort = esPort;
        this.clusterName = clusterName;
        this.client = init();
    }


    protected TransportClient init() {
        long msStart = System.currentTimeMillis();

        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .put("client.transport.ping_timeout", "30s")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            String[] hosts = esHosts.split(",");
            if (hosts.length == 0) {
                throw new RuntimeException("can not initial elastic client, cause configure 'tcp.esHosts' is null");
            }
            // 这里是配置多个集群,但是我们一般只有一个,这里的配置多个是防止单个节点宕掉
            List<InetSocketTransportAddress> esAddresses = new ArrayList<InetSocketTransportAddress>();
            for (String host : hosts) {
                esAddresses.add(
                        new InetSocketTransportAddress(new InetSocketAddress(host.trim(), esPort))
                );
            }
            InetSocketTransportAddress[] esAddressList = esAddresses.toArray(new InetSocketTransportAddress[esAddresses.size()]);
            client.addTransportAddresses(esAddressList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    protected void reconnect() {

        System.out.println("Reconnect to Elasticsearch...");

        try {
            this.client.close();
            this.client = null;
            this.client = init();
        }
        catch (Exception ex) {
            System.out.println("Failed to close existing connection:" + ex);
        }
    }
//    private static void init(String esHosts, int esPort, String clusterName) {
//        long msStart = System.currentTimeMillis();
//
//        Settings settings = Settings.builder()
//                .put("cluster.name", clusterName)
//                .put("client.transport.sniff", true)
//                .put("client.transport.ping_timeout", "30s")
//                .build();
//        client = new PreBuiltTransportClient(settings);
//        try {
//            String[] hosts = esHosts.split(",");
//            if(hosts.length == 0) {
//                throw new RuntimeException("can not initial elastic client, cause configure 'tcp.esHosts' is null");
//            }
//            // 这里是配置多个集群,但是我们一般只有一个,这里的配置多个是防止单个节点宕掉
//            List<InetSocketTransportAddress> esAddresses = new ArrayList<InetSocketTransportAddress>();
//            for(String host : hosts) {
//                esAddresses.add(
//                        new InetSocketTransportAddress(new InetSocketAddress(host.trim(), esPort))
//                );
//            }
//            InetSocketTransportAddress []esAddressList = esAddresses.toArray(new InetSocketTransportAddress[esAddresses.size()]);
//            client.addTransportAddresses(esAddressList);
//        }catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    protected void closeClient() {
//
//        try {
//            if(client != null) {
//                this.client.close();
//                this.client = null;
//            }
//        }catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }

}
