package com.thjvjpxx.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.thjvjpxx.config.CommonConfig;
import com.thjvjpxx.config.MongoConfig;

public class MongoImportService extends AbstractImportService {
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;
    private List<Document> batch;

    @Override
    public void initialize() {
        try {
            mongoClient = MongoClients.create(MongoConfig.getClientSettings());

            MongoDatabase database = mongoClient.getDatabase(MongoConfig.DATABASE);
            collection = database.getCollection(MongoConfig.COLLECTION);

            collection.drop();

            batch = new ArrayList<>();
        } catch (Exception e) {
            throw new RuntimeException("Không thể kết nối đến MongoDB", e);
        }
    }

    @Override
    public long importFromCSV(String csvFile) throws IOException {
        System.out.println("Đang import vào MongoDB....");
        long recordCount = 0;
        long errorCount = 0;

        try (BufferedReader reader = new BufferedReader(
                new FileReader(csvFile), CommonConfig.BUFFER_SIZE)) {

            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    String[] data = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    if (removeNullOrEmpty(data[0])
                            || removeNullOrEmpty(data[3])
                            || removeNullOrEmpty(data[5])
                            || removeNullOrEmpty(data[6])
                            || Integer.parseInt(data[3]) <= 0
                            || Double.parseDouble(data[5]) <= 0.01) {
                        continue;
                    }
                    addToBatch(data);
                    recordCount++;

                    if (batch.size() >= CommonConfig.BATCH_SIZE) {
                        executeBatch();
                    }
                } catch (Exception e) {
                    errorCount++;
                    System.err.println("Lỗi xử lý dòng: " + line);
                    e.printStackTrace();
                }
            }

            if (!batch.isEmpty()) {
                executeBatch();
            }

            System.out.printf("Hoàn thành! Tổng số dòng: %d, Lỗi: %d%n", recordCount, errorCount);
        }

        return recordCount;
    }

    private void addToBatch(String[] data) {
        try {
            Document doc = processData(data);
            batch.add(doc);
        } catch (Exception e) {
            handleBatchError(data, e);
        }
    }

    private Document processData(String[] data) throws ParseException {
        Document doc = new Document();
        Date invoiceDate = CommonConfig.dateFormat.parse(data[4].trim());
        doc.append("invoiceNo", data[0].trim())
                .append("stockCode", data[1].trim())
                .append("description", data[2].trim())
                .append("quantity", Integer.parseInt(data[3].trim()))
                .append("invoiceDate", invoiceDate)
                .append("unitPrice", Double.parseDouble(data[5].trim()))
                .append("customerId", Integer.parseInt(data[6].trim()));

        return doc;
    }

    @Override
    protected void executeBatch() {
        if (!batch.isEmpty()) {
            collection.insertMany(batch);
            batch.clear();
        }
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
