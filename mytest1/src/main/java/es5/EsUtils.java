package es5;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * @author yuan.li
 */

public class EsUtils {
	public static TransportClient client;

	@SuppressWarnings("unchecked")
	// @PostConstruct
	public void init() {
		if (client == null) {
			try {
				 Settings settings = Settings.builder()
						 .put("cluster.name", "es5-cluster")
						 .put("client.transport.sniff", true) //自动嗅探整个集群的状态
						 .build();
				client = new PreBuiltTransportClient(settings);
				InetAddress serverAddr = InetAddress.getByName("10.198.198.115");
				client.addTransportAddress(new InetSocketTransportAddress(serverAddr, 9300));
				
			} catch (UnknownHostException e) {
				System.out.println(e);
			}
		}
	}

	// 取得实例
	public static TransportClient getClient() {
		return client;
	}

	public void close() {
		if (client != null) {
			client.close();
		}
	}
	/**
	 * 
	 * public static RestClient restClient;
	 * 
	 * // 取得实例 public static void init() { if (restClient == null) { HttpHost[] hosts = { new HttpHost("10.198.198.115",
	 * 9200, "http") }; restClient = RestClient.builder(hosts).build();
	 * 
	 * } }
	 * 
	 * public static void destroy() { if (restClient != null) { try { restClient.close(); } catch (IOException e) {
	 * e.printStackTrace(); } } } *
	 */
}
