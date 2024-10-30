package com.thjvjpxx.hadoop.mapper;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.thjvjpxx.hadoop.CustomerFeature;

public class KMapper extends Mapper<LongWritable, Text, Text, CustomerFeature> {
    private CustomerFeature[] centroids;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String centroidsStr = conf.get("centroids");
        String[] centroidPoints = centroidsStr.split(";");
        centroids = new CustomerFeature[centroidPoints.length];

        for (int i = 0; i < centroidPoints.length; i++) {
            String[] values = centroidPoints[i].split(",");
            centroids[i] = new CustomerFeature();
            centroids[i].totalSpending = Double.parseDouble(values[0]);
            centroids[i].purchaseFrequency = Integer.parseInt(values[1]);
            centroids[i].avgItemsPerOrder = Double.parseDouble(values[2]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        CustomerFeature profile = CustomerFeature.fromString(parts[1]);

        // Tìm centroid gần nhất
        int nearestCentroid = 0;
        double minDistance = Double.MAX_VALUE;

        for (int i = 0; i < centroids.length; i++) {
            double distance = profile.distance(centroids[i]);
            if (distance < minDistance) {
                minDistance = distance;
                nearestCentroid = i;
            }
        }

        // Output: clusterId -> CustomerFeature
        context.write(new Text(String.valueOf(nearestCentroid)), profile);
    }
}