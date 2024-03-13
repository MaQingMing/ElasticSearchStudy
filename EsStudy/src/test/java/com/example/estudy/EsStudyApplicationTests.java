package com.example.estudy;

import com.example.estudy.entity.Product;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Query;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
class EsStudyApplicationTests {
    @Autowired
    private ElasticsearchOperations elasticsearchOperations;
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void testCreate() throws IOException {
        Product product = new Product();
        product.setId(1); //存在id指定id  不存在id自动生成id
        product.setTitle("怡宝矿泉水");
        product.setPrice(129.11);
        product.setDescription("我们喜欢喝矿泉水....");
        elasticsearchOperations.save(product);
    }

    @Test
    public void testDelete() {
        Product product = new Product();
        product.setId(1);
        String delete = elasticsearchOperations.delete(product);
        System.out.println(delete);
    }

    @Test
    public void testGet() {
        Product product = elasticsearchOperations.get("1", Product.class);
        System.out.println(product);
    }

    @Test
    public void testUpdate() {
        Product product = new Product();
        product.setId(1);
        product.setTitle("怡宝矿泉水");
        product.setPrice(129.11);
        product.setDescription("我们喜欢喝矿泉水,你们喜欢吗....");
        elasticsearchOperations.save(product);//不存在添加,存在更新
    }

    @Test
    public void testDeleteAll() {
        elasticsearchOperations.delete(Query.findAll(), Product.class);
    }

    @Test
    public void testFindAll() {
        SearchHits<Product> productSearchHits = elasticsearchOperations.search(Query.findAll(), Product.class);
        productSearchHits.forEach(productSearchHit -> {
            System.out.println("id: " + productSearchHit.getId());
            System.out.println("score: " + productSearchHit.getScore());
            Product product = productSearchHit.getContent();
            System.out.println("product: " + product);
        });
    }

    // 创建索引映射

    @Test
    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("fruit");
        createIndexRequest.mapping("{\n" +
                "    \"properties\": {\n" +
                "      \"title\":{\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"price\":{\n" +
                "        \"type\": \"double\"\n" +
                "      },\n" +
                "      \"created_at\":{\n" +
                "        \"type\": \"date\"\n" +
                "      },\n" +
                "      \"description\":{\n" +
                "        \"type\": \"text\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" , XContentType.JSON);
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.isAcknowledged());
        restHighLevelClient.close();
    }

        // 索引文档
        @Test
        public void testIndex() throws IOException {
            IndexRequest indexRequest = new IndexRequest("fruit");
            indexRequest.source("{\n" +
                    "          \"id\" : 1,\n" +
                    "          \"title\" : \"蓝月亮\",\n" +
                    "          \"price\" : 123.23,\n" +
                    "          \"description\" : \"这个洗衣液非常不错哦！\"\n" +
                    "        }",XContentType.JSON);
            IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println(index.status());
        }

        //更新文档
        @Test
        public void testUpdateDOC() throws IOException {
            UpdateRequest updateRequest = new UpdateRequest("fruit","qJ0R9XwBD3J1IW494-Om");
            updateRequest.doc("{\"title\":\"好月亮\"}",XContentType.JSON);
            UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            System.out.println(update.status());
        }

        //删除文档
        @Test
        public void testDeleteDOC() throws IOException {
            DeleteRequest deleteRequest = new DeleteRequest("fruit","1");
            DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println(delete.status());
        }

        //基于 id 查询文档
        @Test
        public void testGetDOC() throws IOException {
            GetRequest getRequest = new GetRequest("fruit","1");
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            System.out.println(getResponse.getSourceAsString());
        }

        //查询所有
        @Test
        public void testSearch() throws IOException {
            SearchRequest searchRequest = new SearchRequest("fruit");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            SearchSourceBuilder query = sourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //System.out.println(searchResponse.getHits().getTotalHits().value);
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                System.out.println(hit.getSourceAsString());
            }
        }

        //综合查询
        @Test
        public void testSearchDOC() throws IOException {
            SearchRequest searchRequest = new SearchRequest("fruit");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder
                    .from(0)
                    .size(2)
                    .sort("price", SortOrder.DESC)
                    .fetchSource(new String[]{"title"},new String[]{})
                    .highlighter(new HighlightBuilder().field("description").requireFieldMatch(false).preTags("<span style='color:red;'>").postTags("</span>"))
                    .query(QueryBuilders.termQuery("description","错"));
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println("总条数: "+searchResponse.getHits().getTotalHits().value);
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                System.out.println(hit.getSourceAsString());
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                highlightFields.forEach((k,v)-> System.out.println("key: "+k + " value: "+v.fragments()[0]));
            }
        }

    /**
     * 聚合查询
     */
    // 求不同价格的数量
    @Test
    public void testAggsPrice() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fruit");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.terms("group_price").field("price"));
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        ParsedDoubleTerms terms = aggregations.get("group_price");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey() + ", "+ bucket.getDocCount());
        }
    }

    // 求不同名称的数量
    @Test
    public void testAggsTitle() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fruit");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.terms("group_title").field("title"));
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        ParsedStringTerms terms = aggregations.get("group_title");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey() + ", "+ bucket.getDocCount());
        }
    }


    // 求和
    @Test
    public void testAggsSum() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fruit");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.sum("sum_price").field("price"));
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ParsedSum parsedSum = searchResponse.getAggregations().get("sum_price");
        System.out.println(parsedSum.getValue());
    }

}
