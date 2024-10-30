package com.thjvjpxx.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.thjvjpxx.config.MongoConfig;

public class MongoDBRecordReader extends RecordReader<LongWritable, Text> {
    private MongoClient mongoClient;
    private MongoCursor<Document> cursor;
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private long pos;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        mongoClient = MongoClients.create(MongoConfig.getClientSettings());
        cursor = mongoClient
                .getDatabase(MongoConfig.DATABASE)
                .getCollection(MongoConfig.COLLECTION)
                .find()
                .iterator();
    }

    @Override
    public boolean nextKeyValue() {
        if (cursor.hasNext()) {
            Document doc = cursor.next();
            key.set(pos++);
            // Chuyển Document thành chuỗi CSV
            value.set(doc.getString("invoiceNo") + "," +
                    doc.getInteger("quantity") + "," +
                    doc.getDouble("unitPrice") + "," +
                    doc.getInteger("customerId"));
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return 0.0f;
    }

    @Override
    public void close() {
        if (cursor != null)
            cursor.close();
        if (mongoClient != null)
            mongoClient.close();
    }
}