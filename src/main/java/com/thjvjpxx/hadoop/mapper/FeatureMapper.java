package com.thjvjpxx.hadoop.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.thjvjpxx.hadoop.CustomerFeature;

public class FeatureMapper extends Mapper<LongWritable, Text, Text, CustomerFeature> {

    private Map<String, CustomerFeature> customerFeatures;

    @Override
    protected void setup(Context context) {
        customerFeatures = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String customerId = fields[3];
        double quantity = Double.parseDouble(fields[1]);
        double unitPrice = Double.parseDouble(fields[2]);
        double amount = quantity * unitPrice;

        CustomerFeature profile = customerFeatures.getOrDefault(customerId, new CustomerFeature());
        profile.totalSpending += amount;
        profile.purchaseFrequency++;
        profile.avgItemsPerOrder = (profile.avgItemsPerOrder * (profile.purchaseFrequency - 1) + quantity)
                / profile.purchaseFrequency;

        customerFeatures.put(customerId, profile);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, CustomerFeature> entry : customerFeatures.entrySet()) {
            if (entry.getValue() != null) {
                context.write(new Text(entry.getKey()), entry.getValue());
            }
        }
    }
}
