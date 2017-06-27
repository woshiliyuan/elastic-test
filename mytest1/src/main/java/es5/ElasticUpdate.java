package es5;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author yuan.li
 */
public class ElasticUpdate {
	// 更新文档1//done
	public static void update(String indexname, String type, String id)
			throws IOException, InterruptedException, ExecutionException {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(indexname);
		updateRequest.type(type);
		updateRequest.id(id);
		updateRequest.doc(XContentFactory.jsonBuilder().startObject().field("name", "惊天地").field("addr", "泣鬼神").endObject());
		ActionFuture<UpdateResponse> af = EsUtils.client.update(updateRequest);
		UpdateResponse response = af.actionGet();
		System.out.println(response.getResult());
	}

	// 更新文档2//done
	public static void prepareUpdate(String indexname, String type, String id)
			throws IOException, InterruptedException, ExecutionException {
		XContentBuilder xcb = XContentFactory.jsonBuilder().startObject().field("name", "惊天地xcb")
				.field("addr", "泣鬼神xcb").endObject();
		UpdateRequestBuilder af = EsUtils.client.prepareUpdate(indexname, type, id).setDoc(xcb);
		UpdateResponse response = af.execute().actionGet();
		System.out.println(response.getResult());
	}

	// 更新文档3//done
	public static void prepareUpdate(String indexname, String type, String id, String json)
			throws IOException, InterruptedException, ExecutionException {
		UpdateRequestBuilder af = EsUtils.client.prepareUpdate(indexname, type, id).setDoc(json);
		UpdateResponse response = af.execute().actionGet();
		System.out.println(response.getResult());
	}

	// 更新文档4//done
	public static void prepareUpsert(String indexname, String type, String id)
			throws IOException, InterruptedException, ExecutionException {
		IndexRequest indexRequest = new IndexRequest(indexname, type, id)
				.source(XContentFactory.jsonBuilder()
				.startObject()
				.field("name", "惊天地update2")
				.field("addr", "泣鬼神update2")
				.endObject());
		UpdateRequest updateRequest = new UpdateRequest(indexname, type, id)
				.doc(XContentFactory.jsonBuilder()// 如果id存在，则更新以下字段
				.startObject()
				.field("name", "惊天地update2")
				.endObject())
				.upsert(indexRequest);// 如果id不存在，则新增文档indexRequest
		ActionFuture<UpdateResponse> af = EsUtils.client.update(updateRequest);
		UpdateResponse response = af.actionGet();
		System.out.println(response.getResult());
	}
}
