package com.thjvjpxx.service;

import java.io.IOException;
import java.sql.SQLException;

public interface ImportService extends AutoCloseable {
    void initialize() throws SQLException;

    long importFromCSV(String csvPath) throws IOException, SQLException;
}
