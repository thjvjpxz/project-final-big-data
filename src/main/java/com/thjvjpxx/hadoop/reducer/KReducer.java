package com.thjvjpxx.hadoop.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.thjvjpxx.hadoop.CustomerFeature;

public class KReducer extends Reducer<Text, CustomerFeature, Text, CustomerFeature> {

    @Override
    protected void reduce(Text key, Iterable<CustomerFeature> values, Context context)
            throws IOException, InterruptedException {
        CustomerFeature newCentroid = new CustomerFeature();
        int count = 0;
        List<CustomerFeature> profiles = new ArrayList<>();

        // Copy values vì Iterable chỉ có thể duyệt một lần
        for (CustomerFeature profile : values) {
            CustomerFeature copy = new CustomerFeature();
            copy.totalSpending = profile.totalSpending;
            copy.purchaseFrequency = profile.purchaseFrequency;
            copy.avgItemsPerOrder = profile.avgItemsPerOrder;
            profiles.add(copy);
        }

        // Tính tổng
        for (CustomerFeature profile : profiles) {
            newCentroid.totalSpending += profile.totalSpending;
            newCentroid.purchaseFrequency += profile.purchaseFrequency;
            newCentroid.avgItemsPerOrder += profile.avgItemsPerOrder;
            count++;
        }

        // Tính trung bình cho centroid mới
        if (count > 0) {
            newCentroid.totalSpending /= count;
            newCentroid.purchaseFrequency /= count;
            newCentroid.avgItemsPerOrder /= count;
        }

        // Output centroid mới
        context.write(key, newCentroid);
    }
}