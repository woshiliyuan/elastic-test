package es5;

import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;

/**
 * @author yuan.li
 */
public class ElasticInsert {
	// 增加文档//done
	public static void prepareIndex(String indexname, String type, String jsonStr) throws Exception {
		IndexRequestBuilder irb = EsUtils.client.prepareIndex(indexname, type).setSource(jsonStr);
		IndexResponse response = irb.execute().actionGet();
		System.out.println("index:" + response.getIndex() + " insert doc id:" + response.getId() + " result:"
				+ response.getResult());
	}

	// 增加文档//批量//done
	public static void prepareBulkIndex(String index, String type, List<String> jsonList) {

		if (null == jsonList || 0 == jsonList.size()) {
			return;
		}

		BulkRequestBuilder bulkRequest = EsUtils.client.prepareBulk();

		for (String json : jsonList) {
			IndexRequestBuilder irb = EsUtils.client.prepareIndex(index, type).setSource(json);
			bulkRequest.add(irb);
		}

		BulkResponse response = bulkRequest.execute().actionGet();
		if (response.hasFailures()) {
			System.out.println("prepareBulkIndex fail,message: " + response.buildFailureMessage());
		}

		System.out.println(response);
	}
}
