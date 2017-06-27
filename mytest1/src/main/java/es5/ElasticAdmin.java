package es5;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * @author yuan.li
 */
public class ElasticAdmin {
	// 创建索引//add mapping//done
	public static void createIndex(String indexname, String type,XContentBuilder source) {

		CreateIndexRequestBuilder cirb = EsUtils.client.admin().indices().prepareCreate(indexname).setSource(source);

		CreateIndexResponse response = cirb.execute().actionGet();
		if (response.isAcknowledged()) {
			System.out.println("index created.");
		} else {
			System.err.println("index creation failed.");
		}
	}

	// 刪除索引//done
	public static void indicesPrepareDelete(String indexname) {
		DeleteIndexRequestBuilder dirb = EsUtils.client.admin().indices().prepareDelete(indexname);
		DeleteIndexResponse response = dirb.execute().actionGet();
		System.out.println(response);
	}
	
	// 创建索引//no mapping//done
	public static void createIndex(String indexname) {
		 Settings settings = Settings.builder()
				 .put("number_of_shards", 5)
				 .put("number_of_replicas", 1) 
				 .build();
		CreateIndexRequestBuilder cirb = EsUtils.client.admin().indices().prepareCreate(indexname).setSettings(settings);

		CreateIndexResponse response = cirb.execute().actionGet();
		if (response.isAcknowledged()) {
			System.out.println("index created.");
		} else {
			System.err.println("index creation failed.");
		}
	}
	
	// 更新索引//add mapping//done
	public static void setMapping(String indexname,String type, XContentBuilder source) throws IOException {
		
		PutMappingRequestBuilder pmrb = EsUtils.client.admin().indices().preparePutMapping(indexname).setType(type).setSource(source);

		PutMappingResponse response = pmrb.execute().actionGet();
		
		if (response.isAcknowledged()) {
			System.out.println("mapping created.");
		} else {
			System.err.println("mapping creation failed.");
		}
	}
}
