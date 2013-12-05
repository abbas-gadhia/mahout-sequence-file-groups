package org.apache.mahout.classifier;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author : Abbas Gadhia
 * @since : 4/12/13
 */

public class KeyPairsMapReduceTest {
    @Test
    public void map() {
        new MapDriver<Text, Text, LongWritable, Text>()
                .withMapper(new CategoryMapper())
                .withInput(new Text("/232/64334"), new Text("Some vector 1"))
                .withOutput(new LongWritable(232l), new Text("/64334/Some vector 1"))
                .runTest();
    }

    /**
     * Ah shucks. MRUnit doesn't support multiple outputs yet
     * https://issues.apache.org/jira/browse/MRUNIT-13
     */
    @Ignore
    @Test
    public void reduce() {
        new ReduceDriver<LongWritable, Text, Text, Text>()
                .withReducer(new CategoryReducer())
                .withInputKey(new LongWritable(232l))
                .withInputValues(Arrays.asList(new Text("/64334/Some vector 1"), new Text("/64335/Some vector 2")))
                .withOutput(new Text("/232/64334"), new Text("Some vector 1"))
                .withOutput(new Text("/232/64335"), new Text("Some vector 2"))
                .runTest();

    }
}
