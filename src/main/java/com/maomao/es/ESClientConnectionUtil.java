package com.maomao.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

//创建连接工具类
public class ESClientConnectionUtil {
    public static TransportClient client=null;
    public final static String HOST = "192.168.200.211"; //服务器部署
    public final static Integer PORT = 9301; //端口
public static TransportClient  getESClientConnection(){
    if (client == null) {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
            try {
                //设置集群名称
                Settings settings = Settings.builder().put("cluster.name", "es5").put("client.transport.sniff", true).build();
                //创建client
                client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println(ex.getMessage());
        }
    }
    return client;
}

}