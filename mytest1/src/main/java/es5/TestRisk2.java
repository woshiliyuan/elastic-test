package es5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;

/**
 * @author yuan.li
 */
public class TestRisk2 {

	private String indexName = "vip2";
	private String typeName = "risk2";
	private static EsUtils esClient;

	@Before
	public void setUp() throws Exception {
		esClient = new EsUtils();
		esClient.init();
	}

	@After
	public void tearDown() throws Exception {
		// esClient.close();
	}

	@Test
	public void createIndex() throws Exception {

		ElasticAdmin.createIndex(indexName);
	}

	@Test
	public void setMapping() throws Exception {
		XContentBuilder source = null;
		try {
			source = XContentFactory.jsonBuilder().startObject().startObject("properties") // 下面是设置文档列属性。
					.startObject("name").field("type", "string").field("store", "yes").endObject().startObject("age")
					.field("type", "integer").endObject().startObject("addr").field("type", "string")
					.field("index", "no").field("store", "yes").endObject().startObject("job").field("type", "string")
					.field("index", "not_analyzed").field("store", "yes").endObject().startObject("phone")
					.field("type", "string").field("index", "analyzed").field("store", "yes").endObject()
					.startObject("birth").field("type", "date")
					.field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis").field("store", "yes").endObject()
					.endObject().endObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ElasticAdmin.setMapping(indexName, typeName, source);
	}

	@Test
	public void prepareBulkIndex() throws Exception {
		Long startTime = System.currentTimeMillis();
		for (int j = 0; j < 1; j++) {
			List<String> jsonList = new ArrayList<String>();
			for (int i = j * 100; i < (j + 1) * 100; i++) {
				Risk user = new Risk();
				user.setName("liyuan" + i);
				user.setAge(i);
				user.setAddr("四行天地" + i);
				user.setJob("工作" + i);
				user.setPhone("156666" + i);
				user.setEmail("156@qq.com" + i);
				user.setCompany("唯品会" + i);
				user.setBirth(new Date());
				jsonList.add(JSON.toJSONString(user));
			}
			ElasticInsert.prepareBulkIndex(indexName, typeName, jsonList);
			System.out.println(j + "---" + "time cost(ms):" + (System.currentTimeMillis() - startTime));
		}
	}
}
