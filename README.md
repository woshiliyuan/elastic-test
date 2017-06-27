https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html

prepare**：构建builder

execute:执行构建的builder
 
get,actionGet:用于获取操作结果，用于获取Future线程执行结果，可以不加

http://10.*.*.115:9200
http://10.*.*.116:9200
http://10.*.*.117:9200

Aggregation

term:是代表完全匹配，即不进行分词器分析，文档中必须包含整个搜索的词汇
match:查询匹配就会进行分词

/**
 * Short version of execute().actionGet().
*/
public Response get() {
    return execute().actionGet();
}


##############################

Elasticsearch：
Elasticsearch是一个实时的分布式搜索和分析引擎。
ElasticSearch是一个基于Lucene的搜索服务器。
   
Lucene： 
 文档（document）：索引和搜索时使用的主要数据载体，包含一个或多个存有数据的字段。
 字段（field）：文档的一部分，包含名称和值两部分。
 词（term）：一个搜索单元，表示文本中的一个词。
 标记（token）：表示在字段文本中出现的词，由这个词的文本、开始和结束偏移量以及类型组成。

倒排索引

分布式部署：
设置集群名称
设置集群hosts
设置最小集群机器个数（防止脑裂）
gateway.recover_after_nodes:
gateway.recover_after_time:

elasticsearch-head插件：
基于node.js的一款界面化的集群操作和管理工具
集群状态，集群管理，数据操作

分片：
主副1：N，主副不位于同一个节点
分片丢失自动重建
分片设置不可更改（可重建索引更改）

数据操作：
索引创建，字段新增，字段修改（不可以，可重建索引更改）
增删改查
聚合（桶）：
一个聚合就是一些桶和指标的组合。一个聚合可以只有一个桶，或者一个指标，或者每样一个。
在桶中甚至可以有多个嵌套的桶。比如，我们可以将文档按照其所属国家进行分桶，然后对每个桶计算其平均薪资(一个指标)。

中文分词插件：
elasticsearch-analysis-ik
http://10.*.*.115:9200/_analyze?analyzer=ik_max_word&pretty=true&text=中华人民共和国
#############################
