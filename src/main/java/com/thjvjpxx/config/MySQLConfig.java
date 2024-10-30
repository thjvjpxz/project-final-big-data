package com.thjvjpxx.config;

public class MySQLConfig {
    public static final String HOST = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
    public static final String PORT = System.getenv().getOrDefault("MYSQL_PORT", "3306");
    public static final String URL = String.format("jdbc:mysql://%s:%s/big_data_test?rewriteBatchedStatements=true", 
                                                 HOST, PORT);
    public static final String USER = "root";
    public static final String PASSWORD = "123";
}
