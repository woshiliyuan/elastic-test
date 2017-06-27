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
public class TestRisk3 {
	private String indexName = "vip3";
	private String typeName = "iikk";
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

					.startObject("_all").field("analyzer", "ik_max_word")
					// .field("search_analyzer", "ik_max_word")
					.field("term_vector", "no").field("store", "false").endObject()
					/**
					 * index: no(不对该字段进行索引，无法搜索，throw：Cannot search on field [phone] since it is not indexed.)，
					 * analyzied(分词后索引)， not_analyzed(以单个关键词进行索 引，不分词，以便精确搜索)
					 * 
					 * store:store.yes，store.no 默认情况下，字段值进行索引，使其可搜索，但不存储。 这意味着可以查询该字段，但不能检索原始字段值。 通常这并不重要。
					 * 字段值已经是_source字段的一部分，它默认存储。 如果您只想检索单个字段或几个字段的值，而不是整个_source，则可以使用源过滤来实现。 在某些情况下，存储一个字段是有意义的。
					 * 例如，如果您有一个包含标题，日期和非常大的内容字段的文档，则可能需要仅检索标题和日期，而无需从大的_source字段中提取这些字段：
					 * 
					 * analyzer:指定分析器，ik_max_word，中文分词搜索 
					 * standard   liyuaN->liyuan li yuaN->li,yuan
					 * simple     
					 * whitespace liyuaN->liyuaN li yuaN->li,yuaN
					 * stop      
					 * keyword    
					 * pattern    
					 * language   
					 * snowball   
					 * 
					 * search_analyzer:默认情况下，查询将使用字段映射中定义的分析器，但可以使用search_analyzer设置来覆盖
					 * 
					 * fielddata：用于group by 默认情况下，Fielddata在文本字段上禁用。 在[your_field_name]上设置fielddata = true，
					 * 以便通过反转反向索引来加载内存中的fielddata。 请注意，这可能会使用显着的内存。
					 * 
					 * date:utc时间，并且受时区影响，建议直接用Long。 @see ValueType
					 */
					.startObject("properties") // 下面是设置文档列属性。
					.startObject("name").field("type", "string").field("store", "yes").field("fielddata", true)
					.endObject().startObject("age").field("type", "integer").field("store", "yes").endObject()
					.startObject("addr").field("type", "string").field("store", "no").field("analyzer", "ik_max_word")
					.endObject().startObject("job").field("type", "string").field("store", "yes")
					.field("fielddata", true).endObject().startObject("phone").field("type", "string")
					.field("index", "no").field("store", "yes").endObject().startObject("birth").field("type", "date")
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
		q1.setValue("liyuan0");
		queryConditions.add(q1);

		QueryConditions q2 = new QueryConditions();
		q2.setQueryType(QueryType.MATCH);
		q2.setName("addr");
		q2.setValue("中华国");
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

	@Test
	public void scroll() throws Exception {
		List<QueryConditions> queryConditions = new ArrayList<QueryConditions>();

		QueryConditions q1 = new QueryConditions();
		q1.setQueryType(QueryType.TERM);
		q1.setName("name");
		q1.setValue("liyuan1");
		queryConditions.add(q1);

		QueryConditions q2 = new QueryConditions();
		q2.setQueryType(QueryType.MATCH);
		q2.setName("addr");
		q2.setValue("中华");
		queryConditions.add(q2);

		QueryConditions q4 = new QueryConditions();
		q4.setQueryType(QueryType.RANGE);
		q4.setName("age");
		q4.setFromValue("0");
		q4.setToValue("10000");
		queryConditions.add(q4);

		ElasticQuery.scroll(indexName, typeName, queryConditions);

	}

	@Test
	public void search3() throws Exception {
		List<QueryConditions> queryConditions = new ArrayList<QueryConditions>();
		QueryConditions q1 = new QueryConditions();
		q1.setQueryType(QueryType.TERM);
		q1.setName("name");
		q1.setValue("liyuan1");
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

	@Test
	public void search4() throws Exception {
		List<QueryConditions> queryConditions = new ArrayList<QueryConditions>();
		QueryConditions q1 = new QueryConditions();
		q1.setQueryType(QueryType.NOT_TERM);
		q1.setName("name");
		q1.setValue("liyuan1");
		queryConditions.add(q1);

		QueryConditions q2 = new QueryConditions();
		q2.setQueryType(QueryType.TERM);
		q2.setName("job");
		q2.setValue("job2");
		queryConditions.add(q2);

		SearchResponse response = ElasticQuery.search3(indexName, typeName, 0, 100, queryConditions);
		System.out.println("response:" + response);
		for (int i = 0; i < response.getHits().hits().length; i++) {
			SearchHit hit = response.getHits().getAt(i);
			System.out.println(hit.sourceAsString());
		}

	}
}
