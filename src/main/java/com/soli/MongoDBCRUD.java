package com.soli;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Solitude on 2017/6/9.
 *
 * 连接本机数据库
 */
public class MongoDBCRUD {
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
            //连接mongodb服务
            MongoClient mongoClient = new MongoClient("localhost",27017);

            // 连接到数据库
            MongoDatabase db = mongoClient.getDatabase("foobar");
            System.out.println("Connect to database successfully!");

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

    //插入文档
    //MongoCollection  中  insert() 方法
    @Test
    public void TestInsert(){
        MongoClient mongoClient = new MongoClient("localhost",27017);

        MongoDatabase db = mongoClient.getDatabase("foobar");

        MongoCollection<Document>  col = db.getCollection("myCollection");
        //插入文档
        /**
         * 1. 创建文档 org.bson.Document 参数为key-value的格式
         * 2. 创建文档集合List<Document>
         * 3. 将文档集合插入数据库集合中 mongoCollection.insertMany(List<Document>) 插入单个文档可以用 mongoCollection.insertOne(Document)
         * */
        Document document = new Document("title","mongodb")
                .append("description","database")
                .append("likes","100")
                .append("by","soli");
        List<Document> documents = new ArrayList<Document>();
        documents.add(document);
        col.insertMany(documents);
        System.out.println("文档1插入成功");

        Document doc = new Document("name","soli");
        col.insertOne(doc);
        System.out.println("文档2插入成功");
    }

    //检索所有文档
    //MongoCollection   find()
    @Test
    public void TestFind(){
        MongoClient mongoClient = new MongoClient("localhost",27017);
        MongoDatabase db = mongoClient.getDatabase("foobar");
        MongoCollection<Document> col = db.getCollection("myCollection");

        //检索所有文档
        /**
         * 1. 获取迭代器FindIterable<Document>
         * 2. 获取游标MongoCursor<Document>
         * 3. 通过游标遍历检索出的文档集合
         * */
        FindIterable<Document> findIterable = col.find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()){
            System.out.println(mongoCursor.next());
        }
    }

    //更新文档
    //MongoCollection   update()
    @Test
    public void TestUpdate(){
        MongoClient mongoClient = new MongoClient("localhost",27017);
        MongoDatabase db = mongoClient.getDatabase("foobar");
        MongoCollection<Document> col = db.getCollection("myCollection");

        //更新文档   将文档中likes=100的文档修改为likes=200
        //参数对应的值必须加双引号
        UpdateResult re = col.updateMany(Filters.eq("likes", "200"), new Document("$set",new Document("likes","100")));


        //检索查看结果
        FindIterable<Document> findIterable = col.find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while(mongoCursor.hasNext()){
            System.out.println(mongoCursor.next());
        }
    }

    //删除第一个文档
    //MongoCollection   remove ()
    @Test
    public void TestDel(){
        MongoClient mongoClient = new MongoClient("localhost",27017);
        MongoDatabase db = mongoClient.getDatabase("foobar");
        MongoCollection<Document> col = db.getCollection("myCollection");

        //删除符合条件的一个文档
        //col.deleteOne(Filters.eq("likes","100"));
        //删除符合条件的所有文档
        col.deleteMany(Filters.eq("name","soli"));

        //检查结果
        FindIterable<Document> findIterable = col.find();
        MongoCursor mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()){
            System.out.println(mongoCursor.next());
        }

    }
}
