package es5;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;

/**
 * @author yuan.li
 */
public class ElasticDelete {
	// 刪除文档//done
	public static void prepareDelete(String indexname, String type, String id) {
		DeleteRequestBuilder drb = EsUtils.client.prepareDelete(indexname, type, id);
		DeleteResponse response = drb.execute().actionGet();
		System.out.println(response.getResult());
	}
}
