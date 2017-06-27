package es5;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author yuan.li
 */
public class ElasticQuery {
	// 查询文档//done
	public static void prepareGet(String indexname, String type, String id) {
		Long startTime = System.currentTimeMillis();

		GetRequestBuilder grb = EsUtils.client.prepareGet(indexname, type, id);
		GetResponse response = grb.execute().actionGet();
		String source = response.getSourceAsString();
		System.out.println(source);

		System.out.println("time cost(ms):" + (System.currentTimeMillis() - startTime));
	}

	// 组合查询//done
	public static SearchResponse search(String indexname, String type, int nowPage, int pageSize,
			List<QueryConditions> queryConditions) throws Exception {
		Long startTime = System.currentTimeMillis();

		SearchResponse response = searchResponse(indexname, type, nowPage, pageSize, queryConditions);

		System.out.println("time cost(ms):" + (System.currentTimeMillis() - startTime));

		return response;

		// HttpEntity entity = new StringEntity(queryBuilder.toString(), ContentType.APPLICATION_JSON);
		// Response indexResponse = EsClient.restClient.performRequest("POST", "/" + indexname + "/" + type + "/",
		// Collections.<String, String> emptyMap(), entity);
		// System.out.println(indexResponse);
	}

	// 组合查询//不返回文档，只返回聚合信息//done
	public static SearchResponse search2(String indexname, String type, int nowPage, int pageSize,
			List<QueryConditions> queryConditions) throws Exception {
		Long startTime = System.currentTimeMillis();
		pageSize = 0;// 不返回文档，只返回聚合信息
		SearchResponse response = searchResponse(indexname, type, nowPage, pageSize, queryConditions);

		System.out.println("time cost(ms):" + (System.currentTimeMillis() - startTime));

		return response;
	}

	// 组合查询//group by//done
	public static SearchResponse search3(String indexname, String type, int nowPage, int pageSize,
			List<QueryConditions> queryConditions) throws Exception {
		Long startTime = System.currentTimeMillis();
		SearchResponse response = null;

		try {
			BoolQueryBuilder queryBuilder = boolQueryBuilder(queryConditions);

			SearchRequestBuilder searchRequestBuilder = EsUtils.client.prepareSearch(indexname.toLowerCase());
			searchRequestBuilder.setQuery(queryBuilder);
			searchRequestBuilder.setTypes(type.toLowerCase());
			// 类似count(1) group by name,// terms.field非数组，因此只能group by一个字段
			//searchRequestBuilder.addAggregation(AggregationBuilders.terms("nameAgg").field("name"));
			// 类似count(1),sum(age) group by name
			searchRequestBuilder.addAggregation(AggregationBuilders.terms("nameAgg").field("name").subAggregation(AggregationBuilders.sum("ageAgg").field("age")));
			response = searchRequestBuilder.execute().actionGet();
		} catch (Exception e) {
			throw e;
		}

		System.out.println("time cost(ms):" + (System.currentTimeMillis() - startTime));

		return response;
	}

	// scroll查询//分页查询，比下标分页性能更优//done
	public static SearchResponse scroll(String indexname, String type, List<QueryConditions> queryConditions)
			throws Exception {
		Long startTime = System.currentTimeMillis();

		SearchResponse response = null;

		try {
			BoolQueryBuilder queryBuilder = boolQueryBuilder(queryConditions);

			SearchRequestBuilder searchRequestBuilder = EsUtils.client.prepareSearch(indexname.toLowerCase());

			searchRequestBuilder.setTypes(type.toLowerCase());

			searchRequestBuilder.setSearchType(org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH);
			searchRequestBuilder.setSize(2);
			searchRequestBuilder.addSort("age", SortOrder.ASC);
			searchRequestBuilder.setScroll(TimeValue.timeValueMinutes(10));
			searchRequestBuilder.setQuery(queryBuilder);

			response = searchRequestBuilder.execute().actionGet();

			long totalCount = response.getHits().getTotalHits();
			// 处理第1页数据
			long count0 = response.getHits().hits().length;
			for (int i = 0; i < response.getHits().hits().length; i++) {
				SearchHit hit = response.getHits().getAt(i);
				System.out.println(hit.sourceAsString());
			}
			// 处理第2...n页数据
			for (int sum = 0; sum < totalCount - count0;) {
				response = EsUtils.client.prepareSearchScroll(response.getScrollId())
						.setScroll(TimeValue.timeValueMinutes(10)).execute().actionGet();
				sum += response.getHits().hits().length;
				System.out.println("总量" + totalCount + " 已经查到" + sum);
				for (int i = 0; i < response.getHits().hits().length; i++) {
					SearchHit hit = response.getHits().getAt(i);
					System.out.println(hit.sourceAsString());
				}
			}

		} catch (Exception e) {
			throw e;
		}

		System.out.println("time cost(ms):" + (System.currentTimeMillis() - startTime));
		return response;
	}

	private static SearchResponse searchResponse(String indexname, String type, int nowPage, int pageSize,
			List<QueryConditions> queryConditions) throws Exception {
		SearchResponse response = null;

		try {
			BoolQueryBuilder queryBuilder = boolQueryBuilder(queryConditions);

			SearchRequestBuilder searchRequestBuilder = EsUtils.client.prepareSearch(indexname.toLowerCase());

			searchRequestBuilder.setTypes(type.toLowerCase());

			searchRequestBuilder.setSearchType(org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH);
			if (pageSize != 0) {
				searchRequestBuilder.setFrom(nowPage * pageSize);
			}
			// 设置查询结果集的返回数，0：不返回文档，可变相只返回聚合信息
			searchRequestBuilder.setSize(pageSize);
			searchRequestBuilder.addSort("birth", SortOrder.DESC);
			// 设置是否按查询匹配度排序
			searchRequestBuilder.setExplain(true);
			searchRequestBuilder.setQuery(queryBuilder);

			// done
			searchRequestBuilder.addAggregation(AggregationBuilders.min("min").field("age"));
			// done
			searchRequestBuilder.addAggregation(AggregationBuilders.max("max").field("age"));
			// done
			searchRequestBuilder.addAggregation(AggregationBuilders.avg("avg").field("age"));
			// done
			searchRequestBuilder.addAggregation(AggregationBuilders.sum("sum1111").field("age"));
			// done
			searchRequestBuilder.addAggregation(AggregationBuilders.count("count").field("age"));

			response = searchRequestBuilder.execute().actionGet();
		} catch (Exception e) {
			throw e;
		}
		return response;
	}

	private static BoolQueryBuilder boolQueryBuilder(List<QueryConditions> queryConditions) {
		BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
		for (QueryConditions queryCondition : queryConditions) {
			QueryBuilder qb = null;
			if (QueryType.TERM.equals(queryCondition.getQueryType())) {// done
				if (StringUtils.isEmpty(queryCondition.getValue())) {
					continue;
				}
				queryBuilder.must(QueryBuilders.termQuery(queryCondition.getName(), queryCondition.getValue()));
			} else if (QueryType.MATCH.equals(queryCondition.getQueryType())) {// done
				if (StringUtils.isEmpty(queryCondition.getValue())) {
					continue;
				}
				queryBuilder.must(QueryBuilders.matchQuery(queryCondition.getName(), queryCondition.getValue()));
			} else if (QueryType.WILDCARD.equals(queryCondition.getQueryType())) {// none
				if (StringUtils.isEmpty(queryCondition.getValue())) {
					continue;
				}
				queryBuilder.must(QueryBuilders.wildcardQuery(queryCondition.getName(), "*" + queryCondition.getValue() + "*"));
			} else if (QueryType.RANGE.equals(queryCondition.getQueryType())) {// done
				queryBuilder.must(QueryBuilders.rangeQuery(queryCondition.getName()).from(queryCondition.getFromValue())
						.to(queryCondition.getToValue()));
			} else if (QueryType.GREATER.equals(queryCondition.getQueryType())) {// done
				if (StringUtils.isEmpty(queryCondition.getValue())) {
					continue;
				}
				queryBuilder.must(QueryBuilders.rangeQuery(queryCondition.getName()).gt(queryCondition.getValue()));
			} else if (QueryType.LESS.equals(queryCondition.getQueryType())) {// done
				if (StringUtils.isEmpty(queryCondition.getValue())) {
					continue;
				}
				queryBuilder.must(QueryBuilders.rangeQuery(queryCondition.getName()).lt(queryCondition.getValue()));
			}
			if (QueryType.NOT_TERM.equals(queryCondition.getQueryType())) {// done
				if (StringUtils.isEmpty(queryCondition.getValue())) {
					continue;
				}
				qb = QueryBuilders.termQuery(queryCondition.getName(), queryCondition.getValue());
				queryBuilder.mustNot(qb);
			}
//			if (qb != null) {
//				queryBuilder.must(qb);
//			}
		}
		return queryBuilder;
	}

	// min//done
	public static void aggregationMin(String indexname, String type) {
		Long startTime = System.currentTimeMillis();

		QueryBuilder queryBuilder = QueryBuilders.rangeQuery("age").from(18).to(100);
		InternalMin min = EsUtils.client.prepareSearch(indexname).setQuery(queryBuilder)
				.addAggregation(AggregationBuilders.min("min").field("age")).get().getAggregations().get("min");
		// response 返回查询结果全量， 计算应该放在客户端？--已解決
		System.out.println(min.getValue());

		System.out.println("time cost(ms):" + (System.currentTimeMillis() - startTime));
	}

	// max//done
	public static void aggregationMax(String indexname, String type) {
		Long startTime = System.currentTimeMillis();

		QueryBuilder queryBuilder = null;
		SearchResponse response = EsUtils.client.prepareSearch(indexname).setQuery(null).setQuery(queryBuilder)
				.addAggregation(AggregationBuilders.max("max").field("age")).get();

		InternalMax max = response.getAggregations().get("max");
		System.out.println(max.getValue());

		System.out.println("time cost(ms):" + (System.currentTimeMillis() - startTime));
	}

}
