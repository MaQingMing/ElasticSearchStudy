package com.example.estudy;

import com.example.estudy.entity.Product;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Query;

import java.io.IOException;

@SpringBootTest
class EsStudyApplicationTests {
    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

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

}
