package es5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;

/**
 * @author yuan.li
 */
public class TestRisk {
	private String indexName = "vip";
	private String typeName = "risk";
	private static EsUtils esClient;

	@Before
	public void setUp() throws Exception {
		esClient = new EsUtils();
		esClient.init();
	}

	@After
	public void tearDown() throws Exception {
//		esClient.close();
	}

	@Test
	public void createIndexx() throws Exception {
		XContentBuilder source = null;
		try {
			source = XContentFactory.jsonBuilder()
					.startObject()
					.startObject("settings")
					.field("number_of_shards", 10)// 设置分片数量
					.field("number_of_replicas", 2)// 设置副本数量
					.endObject()
					.endObject()
					.startObject()
					.startObject(typeName)// type名称
					.startObject("properties") // 下面是设置文档列属性。
					.startObject("name").field("type", "string").field("store", "yes").endObject()
					.startObject("age").field("type", "integer").field("store", "yes").endObject()
					.startObject("addr").field("type", "string").field("store", "yes").endObject()
					.startObject("job").field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject()
					.startObject("phone").field("type", "string").field("store", "yes").endObject()
					.startObject("birth").field("type", "long").field("store", "yes").endObject()
					.endObject()
					.endObject()
					.endObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ElasticAdmin.createIndex(indexName, typeName, source);
	}

	@Test
	public void prepareIndex() throws Exception {
		Risk user = new Risk();
		user.setName("liyuan");
		user.setAge(19);
		user.setAddr("四行天地");
		user.setJob("工作");
		user.setPhone("1566666666");
		user.setEmail("156@qq.com");
		user.setCompany("唯品会");
		user.setBirth(new Date());
		ElasticInsert.prepareIndex(indexName, typeName, JSON.toJSONString(user));
	}

	@Test
	public void prepareBulkIndex() throws Exception {
		Long startTime = System.currentTimeMillis();
		for (int j = 2632; j < 5000; j++) {
			List<String> jsonList = new ArrayList<String>();
			for (int i = j * 10000; i < (j + 1) * 10000; i++) {
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

	@Test
	public void prepareGet() {
		ElasticQuery.prepareGet(indexName, typeName, "AVrWi5EK8e3lHWYLiL42");
	}

	@Test
	public void update() throws IOException, InterruptedException, ExecutionException {
		ElasticUpdate.update(indexName, typeName, "AVrVRqCApUwn2uzu9i2G");
	}

	@Test
	public void prepareUpdate() throws IOException, InterruptedException, ExecutionException {
		ElasticUpdate.prepareUpdate(indexName, typeName, "AVrVRqCApUwn2uzu9i2G");
	}

	@Test
	public void prepareUpdate2() throws IOException, InterruptedException, ExecutionException {
		Risk user = new Risk();
		user.setName("yuanli");
		user.setAge(188);
		user.setAddr("天地四行");
		user.setJob("job");
		user.setPhone("1566666666");
		user.setEmail("156@qq.com");
		user.setCompany("唯品会");
		ElasticUpdate.prepareUpdate(indexName, typeName, "AVrVRqCApUwn2uzu9i2G", JSON.toJSONString(user));
	}

	@Test
	public void prepareUpdate3() throws IOException, InterruptedException, ExecutionException {
		ElasticUpdate.prepareUpsert(indexName, typeName, "AVrVRqCApUwn2uzu9i2G1");
	}

	@Test
	public void prepareDelete() throws IOException, InterruptedException, ExecutionException {
		ElasticDelete.prepareDelete(indexName, typeName, "AVrVRqCBpUwn2uzu9i23");
	}

	@Test
	public void indicesPrepareDelete() throws IOException, InterruptedException, ExecutionException {
		ElasticAdmin.indicesPrepareDelete("hwd");
	}

	@Test
	public void search() throws Exception {
		List<QueryConditions> queryConditions = creat();
		SearchResponse response = ElasticQuery.search(indexName, typeName, 0, 100, queryConditions);
		System.out.println("response:" + response);
		for (int i = 0; i < response.getHits().hits().length; i++) {
			SearchHit hit = response.getHits().getAt(i);
			System.out.println(hit.sourceAsString());
		}
		InternalMin min = response.getAggregations().get("min");
		System.out.println("min:"+min.getValue());
		InternalMax max = response.getAggregations().get("max");
		System.out.println("max:"+max.getValue());
		InternalAvg avg = response.getAggregations().get("avg");
		System.out.println("avg:"+avg.getValue());
		InternalSum sum = response.getAggregations().get("sum1111");
		System.out.println("sum:"+sum.getValue());
		InternalValueCount count = response.getAggregations().get("count");
		System.out.println("count:"+count.getValue());
	}
	@Test
	public void search2() throws Exception {
		List<QueryConditions> queryConditions = creat();
	
		SearchResponse response = ElasticQuery.search2(indexName, typeName, 0, 100, queryConditions);
		System.out.println("response:" + response);
		for (int i = 0; i < response.getHits().hits().length; i++) {
			SearchHit hit = response.getHits().getAt(i);
			System.out.println(hit.sourceAsString());
		}
		InternalMin min = response.getAggregations().get("min");
		System.out.println("min:"+min.getValue());
		InternalMax max = response.getAggregations().get("max");
		System.out.println("max:"+max.getValue());
		InternalAvg avg = response.getAggregations().get("avg");
		System.out.println("avg:"+avg.getValue());
		InternalSum sum = response.getAggregations().get("sum1111");
		System.out.println("sum:"+sum.getValue());
		InternalValueCount count = response.getAggregations().get("count");
		System.out.println("count:"+count.getValue());
//		InternalTerms<InternalTerms<A,B>, Bucket<B>> terms = response.getAggregations().get("terms");
//		System.out.println("terms:"+terms.getValue());
		
	}
	
	/**
	 * @return
	 */
	private List<QueryConditions> creat() {
		List<QueryConditions> queryConditions = new ArrayList<QueryConditions>();

		QueryConditions q1 = new QueryConditions();
		q1.setQueryType(QueryType.TERM);
		q1.setName("name");
		q1.setValue("liyuan");
		queryConditions.add(q1);
//
//		QueryConditions q2 = new QueryConditions();
//		q2.setSearchType(SearchType.TERM);
//		q2.setName("phone");
//		q2.setValue("1566666666");
//		queryConditions.add(q2);

//		QueryConditions q3 = new QueryConditions();
//		q3.setSearchType(SearchType.WILDCARD);
//		q3.setName("job");
//		q3.setValue("工作");
//		queryConditions.add(q3);
		
		QueryConditions q4 = new QueryConditions();
		q4.setQueryType(QueryType.RANGE);
		q4.setName("age");
		q4.setFromValue("0");
		q4.setToValue("10000");
		queryConditions.add(q4);
		
		// QueryConditions q5 = new QueryConditions();
		// q5.setSearchType(SearchType.GREATER);
		// q5.setName("age");
		// q5.setValue("11");
		// queryConditions.add(q5);
		//
		// QueryConditions q6 = new QueryConditions();
		// q6.setSearchType(SearchType.LESS);
		// q6.setName("age");
		// q6.setValue("100");
		// queryConditions.add(q6);
		//
		// QueryConditions q7 = new QueryConditions();
		// q7.setSearchType(SearchType.MIN);
		// q7.setName("age");
		// queryConditions.add(q7);
		return queryConditions;
	}
	@Test
	public void aggregationMin() throws IOException, InterruptedException, ExecutionException {
		ElasticQuery.aggregationMin(indexName, typeName);
	}
	@Test
	public void aggregationMax() throws IOException, InterruptedException, ExecutionException {
		ElasticQuery.aggregationMax(indexName, typeName);
	}
}
