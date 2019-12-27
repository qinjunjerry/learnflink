package com.ververica.learnflink.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class StringTokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
        // normalize and split the line
        String[] tokens = line.toLowerCase().split("\\W+");

        // emit the pairs of (word,1)
        for (String token : tokens ) {
            if (token.length() > 0) {
                out.collect(new Tuple2<>(token, 1));
            }
        }

    }
}