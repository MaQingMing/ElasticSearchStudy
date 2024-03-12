package com.example.estudy.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * @author mqm
 * @version 1.0
 * @date 2024/3/12 23:12
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Document(indexName = "products", createIndex = true)
public class Product {
    @Id
    private Integer id;
    @Field(type = FieldType.Keyword)
    private String title;
    @Field(type = FieldType.Float)
    private Double price;
    @Field(type = FieldType.Text)
    private String description;
    //get set ...
}

//1. @Document(indexName = "products", createIndex = true) 用在类上 作用:代表一个对象为一个文档
//		-- indexName属性: 创建索引的名称
//                -- createIndex属性: 是否创建索引
////2. @Id 用在属性上  作用:将对象id字段与ES中文档的_id对应
////3. @Field(type = FieldType.Keyword) 用在属性上 作用:用来描述属性在ES中存储类型以及分词情况
//                -- type: 用来指定字段类型