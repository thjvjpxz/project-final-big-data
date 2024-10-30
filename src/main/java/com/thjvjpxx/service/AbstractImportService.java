package com.thjvjpxx.service;

public abstract class AbstractImportService implements ImportService {

    protected void validateData(String[] data) {
        if (data.length < 8) {
            throw new IllegalArgumentException(
                    "Thiếu dữ liệu: cần 8 cột, nhận được " + data.length);
        }
    }

    protected boolean removeNullOrEmpty(String value) {
        if (value == null || value.trim().isEmpty())
            return true;
        return false;
    }

    protected void handleBatchError(String[] data, Exception e) {
        String rowData = String.join(", ", data);
        System.err.println("Lỗi xử lý dòng dữ liệu: " + rowData);
        System.err.println("Chi tiết lỗi: " + e.getMessage());
        throw new RuntimeException(e);
    }

    protected abstract void executeBatch();
}
