package com.thjvjpxx.config;

import java.util.Arrays;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongoConfig {
    public static final String HOST = System.getenv().getOrDefault("MONGO_HOST", "localhost");
    public static final int PORT = Integer.parseInt(System.getenv().getOrDefault("MONGO_PORT", "27017"));
    public static final String USERNAME = "admin";
    public static final String PASSWORD = "123";
    public static final String AUTH_SOURCE = "admin";
    public static final String DATABASE = "big_data_test";
    public static final String COLLECTION = "sales";

    /**
     * Tạo cấu hình MongoDB client với xác thực
     */
    public static MongoClientSettings getClientSettings() {
        // Tạo credentials
        MongoCredential credential = MongoCredential.createCredential(
                USERNAME,
                AUTH_SOURCE,
                PASSWORD.toCharArray());

        // Cấu hình client settings
        return MongoClientSettings.builder()
                .credential(credential)
                .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress(HOST, PORT))))
                .build();
    }
}
