package com.thjvjpxx.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CustomerFeature implements Writable {
    public double totalSpending;
    public int purchaseFrequency;
    public double avgItemsPerOrder;

    public CustomerFeature() {
        this.totalSpending = 0.0;
        this.purchaseFrequency = 0;
        this.avgItemsPerOrder = 0.0;
    }

    public CustomerFeature(double spending, int frequency, double avgItems) {
        totalSpending = spending;
        purchaseFrequency = frequency;
        avgItemsPerOrder = avgItems;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(totalSpending);
        out.writeInt(purchaseFrequency);
        out.writeDouble(avgItemsPerOrder);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalSpending = in.readDouble();
        purchaseFrequency = in.readInt();
        avgItemsPerOrder = in.readDouble();
    }

    /**
     * Tính khoảng cách Euclid giữa đối tượng hiện tại và đối tượng khác
     * 
     * @param other
     * @return Khoảng cách giữa 2 đôi tượng
     */
    public double distance(CustomerFeature other) {
        double spendingDiff = this.totalSpending - other.totalSpending;
        double frequencyDiff = this.purchaseFrequency - other.purchaseFrequency;
        double avgItemsDiff = this.avgItemsPerOrder - other.avgItemsPerOrder;

        return Math.sqrt(spendingDiff * spendingDiff +
                frequencyDiff * frequencyDiff +
                avgItemsDiff * avgItemsDiff);
    }

    @Override
    public String toString() {
        return String.format("%.2f,%d,%.2f",
                totalSpending, purchaseFrequency, avgItemsPerOrder);
    }

    public static CustomerFeature fromString(String str) {
        String[] parts = str.split(",");
        return new CustomerFeature(
                Double.parseDouble(parts[0]),
                Integer.parseInt(parts[1]),
                Double.parseDouble(parts[2]));
    }
}
