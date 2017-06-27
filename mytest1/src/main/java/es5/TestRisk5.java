package es5;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yuan.li
 */
public class TestRisk5 {
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
}
