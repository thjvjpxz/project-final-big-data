package com.thjvjpxx.hadoop;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MongoDBInputFormat extends InputFormat<LongWritable, Text> {
    @Override
    public List<InputSplit> getSplits(JobContext context) {
        return Collections.singletonList(new MongoDBInputSplit());
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new MongoDBRecordReader();
    }
}