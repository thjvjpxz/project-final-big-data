package com.thjvjpxx.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.thjvjpxx.hadoop.CustomerFeature;

public class FeatureReducer extends Reducer<Text, CustomerFeature, Text, CustomerFeature> {

    @Override
    protected void reduce(Text key, Iterable<CustomerFeature> values, Context context)
            throws IOException, InterruptedException {
        CustomerFeature finalProfile = new CustomerFeature();
        for (CustomerFeature profile : values) {
            finalProfile.totalSpending += profile.totalSpending;
            finalProfile.purchaseFrequency += profile.purchaseFrequency;
            finalProfile.avgItemsPerOrder += profile.avgItemsPerOrder;
        }

        context.write(key, finalProfile);
    }
}
