package com.soli;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.Test;

import javax.print.Doc;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Solitude on 2017/6/9.
 *
 * 连接本机数据库
 *
 *MongoClient实例实际上代表与数据库的连接池;
 * MongoClient即使使用多个线程，只需要一个类的实例 。
 *  通常，您只MongoClient为给定的数据库集群创建一个实例，并在整个应用程序中使用它。创建多个实例时：
    1、所有资源使用限制（最大连接等）适用于每个 MongoClient实例
    2、要处理实例，请确保调用MongoClient.close() 清理资源
 */
public class MongoDBCRUD {

    //连接mongodb服务
    MongoClient mongoClient = new MongoClient("localhost",27017);

    // 连接到数据库
    MongoDatabase db = mongoClient.getDatabase("foobar");

    //获取/选择一个集合
    MongoCollection<Document> col = db.getCollection("myCollection");


    //插入文档
    //MongoCollection  中  insert() 方法
    @Test
    public void TestInsert(){
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


    //插入多个文档
    //MongoCollection  中  insertMany() 方法
    @Test
    public void TestInsertMany(){

        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i<100; i++){
            documents.add(new Document("i",i));
        }

        col.insertMany(documents);

        FindIterable<Document>  findIterable = col.find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()){
            System.out.println(mongoCursor.next());
        }

    }

    //检索所有文档
    //MongoCollection   find()
    @Test
    public void TestFind(){

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

    //查找单个
    @Test
    public void TestFindOne(){
        Document d = col.find().first();
        System.out.println(d.toJson());
    }

    //查询具有过滤的单个文档
    @Test
    public void TestFindOneByCon(){
        Document document = col.find(eq("i",71)).first();
        System.out.println(document.toJson());
    }

    //查询具有过滤的多个文档
    @Test
    public void TestFindByCon(){
        Block<Document> block = new Block<Document>() {
            public void apply(Document document) {
                System.out.println(document.toJson());
            }
        };
        col.find(gt("i",50)).forEach(block);

        col.find(and(gt("i", 50), lte("i", 100))).forEach(block);
    }

    //更新文档
    //MongoCollection   update()
    @Test
    public void TestUpdate(){

        //更新文档   将文档中likes=100的文档修改为likes=200
        //参数对应的值必须加双引号
        UpdateResult re = col.updateMany(eq("likes", "200"), new Document("$set",new Document("likes","100")));

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

        //删除符合条件的一个文档
        //col.deleteOne(eq("likes","100"));
        //删除符合条件的所有文档
        col.deleteMany(eq("name","soli"));

        //检查结果
        FindIterable<Document> findIterable = col.find();
        MongoCursor mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()){
            System.out.println(mongoCursor.next());
        }
    }

    //删除多个匹配文档
    //MongoCollection   delete()
    //filters  的正则，gte表示大于等于50
    @Test
    public void TestDelMany(){
        DeleteResult deleteResult = col.deleteMany(gte("i",50));
        System.out.println(deleteResult.getDeletedCount()+"-----------------");
        FindIterable<Document> findIterable = col.find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()){
            System.out.println(mongoCursor.next());
        }
    }

    //计数
    @Test
    public void TestCount(){
        System.out.println(col.count());
    }

    //排序
    //exists表示查找为i的元素
    //sort中参数为 BasicDbObject
    // -1代表倒序，1代表正序
    @Test
    public void TestSort(){
//        FindIterable<Document> documents= col.find(exists("i")).sort(new BasicDBObject("i",-1));
        FindIterable<Document> documents= col.find(exists("i")).sort(descending("i"));
        MongoCursor<Document> mongoCursor  = documents.iterator();
        while (mongoCursor.hasNext()){
            System.out.println(mongoCursor.next());
        }
    }


}
