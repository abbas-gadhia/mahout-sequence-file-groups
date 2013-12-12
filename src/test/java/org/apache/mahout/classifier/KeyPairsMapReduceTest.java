package org.apache.mahout.classifier;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.mahout.math.VectorWritable;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * @author : Abbas Gadhia
 * @since : 4/12/13
 */

public class KeyPairsMapReduceTest {
    /**
     * Um. Need to figure out how to create a proper VectorWritable
     */
    @Ignore
    @Test
    public void map() {
        new MapDriver<Text, VectorWritable, Text, VectorWritable>()
                .withMapper(new CategoryMapper())
                .withInput(new Text("/232/64334"), fromString("/232/64334:Some vector 1"))
                .withOutput(new Text("232"), fromString("/232/64334:Some vector 1"))
                .runTest();
    }

    /**
     * Ah shucks. MRUnit doesn't support multiple outputs yet
     * https://issues.apache.org/jira/browse/MRUNIT-13
     */
    @Test
    @Ignore
    public void reduce() {
        new ReduceDriver<Text, VectorWritable, Text, VectorWritable>()
                .withReducer(new CategoryReducer())
                .withInputKey(new Text("232"))
                .withInputValues(Arrays.asList(fromString("/232/64334:Some vector 1"), fromString("/232/64335:Some vector 2")))
                .withOutput(new Text("/232/64334"), fromString("/232/64334:Some vector 1"))
                .withOutput(new Text("/232/64335"), fromString("/232/64335:Some vector 2"))
                .runTest();

    }

    private VectorWritable fromString(String val) {
        VectorWritable v = new VectorWritable();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(val.getBytes(Charset.forName("UTF-8"))));
        try {
            v.readFields(dis);
        } catch (IOException e) {
        }
        return v;
    }
}
