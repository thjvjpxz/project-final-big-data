package com.thjvjpxx.hadoop;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class MongoDBInputSplit extends InputSplit implements Writable {
    @Override
    public void write(DataOutput out) {
    }

    @Override
    public void readFields(DataInput in) {
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public String[] getLocations() {
        return new String[] {};
    }
}
