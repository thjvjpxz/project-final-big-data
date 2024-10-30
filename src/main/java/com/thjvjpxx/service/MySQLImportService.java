package com.thjvjpxx.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;

import com.thjvjpxx.config.CommonConfig;
import com.thjvjpxx.config.MySQLConfig;

public class MySQLImportService extends AbstractImportService {
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void initialize() throws SQLException {
        connection = DriverManager.getConnection(
                MySQLConfig.URL,
                MySQLConfig.USER,
                MySQLConfig.PASSWORD);

        connection.setAutoCommit(false);

        createTable();

        prepareStatement();

    }

    private void createTable() throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS sales (" +
                "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                "InvoiceNo VARCHAR(30), " +
                "StockCode VARCHAR(30), " +
                "Description TEXT, " +
                "Quantity INT, " +
                "InvoiceDate DATETIME, " +
                "UnitPrice DECIMAL(10,2), " +
                "CustomerID INT, " +
                "Country VARCHAR(20)" +
                ")";
        String deleteTableSQL = "DROP TABLE IF EXISTS sales";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(deleteTableSQL);
            stmt.execute(createTableSQL);
        }
    }

    private void prepareStatement() throws SQLException {
        String insertSQL = "INSERT INTO sales (" +
                "InvoiceNo, StockCode, Description, " +
                "Quantity, InvoiceDate, UnitPrice, CustomerID, Country" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        preparedStatement = connection.prepareStatement(insertSQL);
    }

    private void addBatch(String[] data) throws SQLException, ParseException {
        validateData(data);
        processData(data);
        preparedStatement.addBatch();
    }

    private void processData(String[] data) throws SQLException, ParseException {
        preparedStatement.setString(1, data[0].trim()); // InvoiceNo
        preparedStatement.setString(2, data[1].trim()); // StockCode
        preparedStatement.setString(3, data[2].trim()); // Description
        preparedStatement.setInt(4, Integer.parseInt(data[3].trim())); // Quantity
        preparedStatement.setTimestamp(5, new Timestamp(CommonConfig.dateFormat.parse(data[4].trim()).getTime())); // InvoiceDate
        preparedStatement.setDouble(6, Double.parseDouble(data[5].trim())); // UnitPrice
        preparedStatement.setInt(7, Integer.parseInt(data[6].trim())); // CustomerID
        preparedStatement.setString(8, data[7].trim()); // Country
    }

    @Override
    public long importFromCSV(String csvFile) throws IOException, SQLException {
        System.out.println("Đang import vào mysql....");
        long recordCount = 0;
        long errorCount = 0;

        try (BufferedReader reader = new BufferedReader(
                new FileReader(csvFile), CommonConfig.BUFFER_SIZE)) {

            reader.readLine();

            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    if (line.trim().isEmpty()) {
                        continue; // Bỏ qua dòng trống
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

                    try {
                        addBatch(data);
                        recordCount++;
                    } catch (Exception e) {
                        errorCount++;
                        System.err.println("Dòng bị lỗi: " + line);
                        e.printStackTrace();
                        continue; // Tiếp tục với dòng tiếp theo
                    }

                    if (recordCount % CommonConfig.BATCH_SIZE == 0) {
                        executeBatch();
                    }
                } catch (Exception e) {
                    errorCount++;
                    System.err.println("Lỗi xử lý dòng: " + line);
                    e.printStackTrace();
                }
            }

            if (recordCount % CommonConfig.BATCH_SIZE != 0) {
                executeBatch();
            }

            System.out.printf("Hoàn thành! Tổng số dòng: %d, Lỗi: %d%n", recordCount, errorCount);
        }

        return recordCount;
    }

    @Override
    protected void executeBatch() {
        try {
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            System.err.println("Lỗi thực thi batch");
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws SQLException {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}