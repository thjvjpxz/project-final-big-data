package com.thjvjpxx.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.thjvjpxx.config.MongoConfig;
import com.thjvjpxx.hadoop.mapper.FeatureMapper;
import com.thjvjpxx.hadoop.mapper.KMapper;
import com.thjvjpxx.hadoop.reducer.FeatureReducer;
import com.thjvjpxx.hadoop.reducer.KReducer;

public class CustomerClusteringRun {
    private Configuration conf;
    private int iterations;
    private double threshold;
    private int k;
    private String outputPath;
    // private Map<Integer, CustomerFeature> finalCentroids;

    // Constructor
    public CustomerClusteringRun(String outputPath, int maxIterations, double threshold, int k) {
        this.conf = new Configuration();
        this.iterations = maxIterations;
        this.threshold = threshold;
        this.k = k;
        this.outputPath = outputPath;
    }

    private void initializeCentroids(Path featurePath) throws IOException {
        featurePath = new Path(featurePath + "/part-r-00000");
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(featurePath)));

        List<CustomerFeature> allPoints = new ArrayList<>();
        String line;

        while ((line = reader.readLine()) != null) {
            String[] parts = line.toString().split("\t");
            CustomerFeature feature = CustomerFeature.fromString(parts[1]);
            allPoints.add(feature);
        }
        reader.close();

        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        boolean first = true;

        Set<Integer> selectedIndexes = new HashSet<>();
        while (selectedIndexes.size() < k) {
            int index = rand.nextInt(allPoints.size());
            if (selectedIndexes.add(index)) {
                if (!first) {
                    sb.append(";");
                }
                CustomerFeature centroid = allPoints.get(index);
                sb.append(String.format("%.2f,%d,%.2f",
                        centroid.totalSpending,
                        centroid.purchaseFrequency,
                        centroid.avgItemsPerOrder));
                first = false;
            }
        }

        conf.set("centroids", sb.toString());
        System.out.println("Initial centroids: " + sb.toString());
    }

    // Main processing method
    public Map<Integer, CustomerFeature> processData() throws Exception {
        // Job 1: Feature Extraction và chuẩn hóa
        Job job1 = Job.getInstance(conf, "customer feature extraction");
        job1.setJarByClass(CustomerClusteringRun.class);

        job1.setMapperClass(FeatureMapper.class);
        job1.setReducerClass(FeatureReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(CustomerFeature.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(CustomerFeature.class);

        job1.setInputFormatClass(MongoDBInputFormat.class);

        Path featurePath = new Path(outputPath + "_features");
        FileOutputFormat.setOutputPath(job1, featurePath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(featurePath)) {
            fs.delete(featurePath, true);
        }

        if (!job1.waitForCompletion(true)) {
            throw new Exception("Job 1 failed!");
        }

        // Khởi tạo centroids
        initializeCentroids(featurePath);

        // Job 2: K-means Clustering
        Map<Integer, CustomerFeature> currentCentroids = null;
        Path iterationPath = new Path(outputPath + "_iteration");

        for (int i = 0; i < iterations; i++) {
            System.out.printf("---------------------%dth iteration---------------------\n", i);
            Job job2 = Job.getInstance(conf, "k-means iteration " + i);
            job2.setJarByClass(CustomerClusteringRun.class);

            job2.setMapperClass(KMapper.class);
            job2.setReducerClass(KReducer.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(CustomerFeature.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(CustomerFeature.class);

            FileInputFormat.addInputPath(job2, featurePath);
            if (fs.exists(iterationPath)) {
                fs.delete(iterationPath, true);
            }
            FileOutputFormat.setOutputPath(job2, iterationPath);

            if (!job2.waitForCompletion(true)) {
                throw new Exception("Job 2 failed at iteration " + i);
            }

            Map<Integer, CustomerFeature> newCentroids = readCentroids(
                    new Path(iterationPath + "/part-r-00000"));

            if (i > 0 && hasConverged(currentCentroids, newCentroids)) {
                // writeFinalCentroids(newCentroids,
                // new Path(outputPath + "_final_centroids.txt").toString());
                if (fs.exists(iterationPath)) {
                    fs.delete(iterationPath, true);
                }
                if (fs.exists(featurePath)) {
                    fs.delete(featurePath, true);
                }
                saveClustersToMongoDB(newCentroids);
                return newCentroids;
            }

            updateCentroidsInConfig(newCentroids);
            currentCentroids = newCentroids;
        }

        // Cleanup
        if (fs.exists(iterationPath)) {
            fs.delete(iterationPath, true);
        }
        if (fs.exists(featurePath)) {
            fs.delete(featurePath, true);
        }
        return null;
    }

    private Map<Integer, CustomerFeature> readCentroids(Path path) throws IOException {
        Map<Integer, CustomerFeature> centroids = new HashMap<>();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;

        while ((line = reader.readLine()) != null) {
            try {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    int clusterId = Integer.parseInt(parts[0]);
                    CustomerFeature cf = CustomerFeature.fromString(parts[1]);
                    centroids.put(clusterId, cf);
                }
            } catch (Exception e) {
                System.err.println("Error parsing line: " + line);
            }
        }
        reader.close();
        return centroids;
    }

    private boolean hasConverged(Map<Integer, CustomerFeature> oldCentroids,
            Map<Integer, CustomerFeature> newCentroids) {
        if (oldCentroids == null || newCentroids == null) {
            return false;
        }

        double maxMovement = 0.0;
        for (Integer clusterId : oldCentroids.keySet()) {
            CustomerFeature oldCentroid = oldCentroids.get(clusterId);
            CustomerFeature newCentroid = newCentroids.get(clusterId);
            if (oldCentroid != null && newCentroid != null) {
                double movement = oldCentroid.distance(newCentroid);
                maxMovement = Math.max(maxMovement, movement);
            }
        }
        // System.out.println("Max centroid movement: " + maxMovement);
        return maxMovement < threshold;
    }

    private void updateCentroidsInConfig(Map<Integer, CustomerFeature> centroids) {
        StringBuilder centroidsStr = new StringBuilder();
        boolean first = true;

        for (Map.Entry<Integer, CustomerFeature> entry : centroids.entrySet()) {
            if (!first)
                centroidsStr.append(";");
            CustomerFeature cf = entry.getValue();
            centroidsStr.append(String.format("%.2f,%d,%.2f",
                    cf.totalSpending, cf.purchaseFrequency, cf.avgItemsPerOrder));
            first = false;
        }

        conf.set("centroids", centroidsStr.toString());
        System.out.println("Updated centroids: " + centroidsStr.toString());
    }

    // private void writeFinalCentroids(Map<Integer, CustomerFeature> centroids,
    // String path)
    // throws IOException {
    // System.out.println("Final Centroids:");
    // try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
    // List<Integer> sortedKeys = new ArrayList<>(centroids.keySet());
    // Collections.sort(sortedKeys);

    // for (Integer clusterId : sortedKeys) {
    // CustomerFeature centroid = centroids.get(clusterId);
    // String output = String.format(
    // "Cluster %d: [totalSpending=%.2f, purchaseFrequency=%d,
    // avgItemsPerOrder=%.2f]",
    // clusterId,
    // centroid.totalSpending,
    // centroid.purchaseFrequency,
    // centroid.avgItemsPerOrder);
    // System.out.println(output);
    // writer.write(output);
    // writer.newLine();
    // }
    // }
    // }

    private void saveClustersToMongoDB(Map<Integer, CustomerFeature> centroids) {
        try {
            // Tạo kết nối MongoDB với cấu hình từ MongoConfig
            MongoClient mongoClient = MongoClients.create(MongoConfig.getClientSettings());
            MongoDatabase database = mongoClient.getDatabase(MongoConfig.DATABASE);
            MongoCollection<Document> collection = database.getCollection(MongoConfig.COLLECTION + "_cluster");

            // Xóa dữ liệu cũ trong collection (nếu cần)
            collection.deleteMany(new Document());

            // Chuyển đổi và lưu từng cluster
            List<Integer> sortedKeys = new ArrayList<>(centroids.keySet());
            Collections.sort(sortedKeys);

            List<Document> documents = new ArrayList<>();
            for (Integer clusterId : sortedKeys) {
                CustomerFeature centroid = centroids.get(clusterId);
                Document clusterDoc = new Document()
                        .append("cluster_id", clusterId)
                        .append("totalSpending", centroid.totalSpending)
                        .append("purchaseFrequency", centroid.purchaseFrequency)
                        .append("avgItemsPerOrder", centroid.avgItemsPerOrder);

                documents.add(clusterDoc);
            }

            // Thực hiện insert
            collection.insertMany(documents);
            System.out.println("Đã lưu " + documents.size() + " clusters vào MongoDB");

            // Đóng kết nối
            mongoClient.close();
        } catch (Exception e) {
            System.err.println("Lỗi khi lưu clusters vào MongoDB: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
