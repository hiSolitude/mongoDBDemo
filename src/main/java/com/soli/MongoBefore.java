package com.soli;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Test;

/**
 * Created by Solitude on 2017/6/12.
 */
public class MongoBefore {

    //测试本地数据库连接
    @Test
    public void JDBCTest(){
        try {
            //连接mongodb服务
            MongoClient mongoClient = new MongoClient("localhost",27017);

            // 连接到数据库
            DB db = mongoClient.getDB("foobar");
            System.out.println("Connect to database successfully!");

        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    //创建集合
    //创建集合需要使用 MongoDatabase 类的 createCollection() 方法。
    @Test
    public void createCol(){
        try {

            MongoClient mongoClient = new MongoClient("localhost",27017);

            MongoDatabase db = mongoClient.getDatabase("foobar");

            //创建Collection
            db.createCollection("myCollection");
            System.out.println("Collection created successfully");

            //删除Collection
//            db.getCollection("myCollection").drop();

        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    //获取/选择一个集合
    //MongoDatabase 中 getCollection方法
    @Test
    public void TestChooseCol(){
        MongoClient mongoClient = new MongoClient("localhost",27017);

        MongoDatabase db = mongoClient.getDatabase("foobar");

        MongoCollection<Document> col =  db.getCollection("myCollection");

        MongoNamespace name = col.getNamespace();

        String dbname = name.getDatabaseName();

        System.out.println(dbname);
    }
}
