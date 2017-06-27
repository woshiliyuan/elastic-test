package es5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;

/**
 * @author yuan.li
 */
public class TestRisk6 {
	private String indexName = "vip4";
	private String typeName = "risk4";
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
			source = XContentFactory.jsonBuilder().startObject()

					.startObject("_all")
					.field("analyzer", "ik_max_word")
					.field("term_vector", "no")
					.field("store", "false")
					.endObject()
					.startObject("properties") // 下面是设置文档列属性。
					.startObject("name").field("type", "string").field("index", "not_analyzed").endObject()
					.startObject("age").field("type", "integer").field("store", "yes").endObject()
					.startObject("addr").field("type", "string").field("store", "no").field("analyzer", "ik_max_word").endObject()
					.startObject("job").field("type", "string").field("store", "yes").field("fielddata", true).endObject()
					.startObject("phone").field("type", "string").field("index", "no").field("store", "yes").endObject()
					.startObject("birth").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis").field("store", "yes").endObject()
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
			for (int i = j * 10; i < (j + 1) * 10; i++) {
				Risk user = new Risk();
				user.setName("Liyuan" + i);
				user.setAge(i);
				user.setAddr("中华人民共和国" + i);
				user.setJob("job" + i);
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
	
	@Test
	public void search3() throws Exception {
		List<QueryConditions> queryConditions = new ArrayList<QueryConditions>();
		QueryConditions q1 = new QueryConditions();
		q1.setQueryType(QueryType.TERM);
		q1.setName("name");
		q1.setValue("Liyuan1");
		queryConditions.add(q1);

		QueryConditions q2 = new QueryConditions();
		q2.setQueryType(QueryType.TERM);
		q2.setName("job");
		q2.setValue("job1");
		queryConditions.add(q2);

		SearchResponse response = ElasticQuery.search3(indexName, typeName, 0, 100, queryConditions);
		System.out.println("response:" + response);
		for (int i = 0; i < response.getHits().hits().length; i++) {
			SearchHit hit = response.getHits().getAt(i);
			System.out.println(hit.sourceAsString());
		}

	}
	
	@Test // 中文分词查询
	public void search() throws Exception {
		List<QueryConditions> queryConditions = creat();
		SearchResponse response = ElasticQuery.search(indexName, typeName, 0, 100, queryConditions);
		System.out.println("response:" + response);
		for (int i = 0; i < response.getHits().hits().length; i++) {
			SearchHit hit = response.getHits().getAt(i);
			System.out.println(hit.sourceAsString());
		}
	}

	private List<QueryConditions> creat() {
		List<QueryConditions> queryConditions = new ArrayList<QueryConditions>();

		QueryConditions q1 = new QueryConditions();
		q1.setQueryType(QueryType.TERM);
		q1.setName("name");
		q1.setValue("Liyuan1");
		queryConditions.add(q1);

		QueryConditions q2 = new QueryConditions();
		q2.setQueryType(QueryType.TERM);
		q2.setName("addr");
		q2.setValue("中华");
		queryConditions.add(q2);

		QueryConditions q4 = new QueryConditions();
		q4.setQueryType(QueryType.RANGE);
		q4.setName("age");
		q4.setFromValue("0");
		q4.setToValue("10000");
		queryConditions.add(q4);
		;
		return queryConditions;
	}
}
