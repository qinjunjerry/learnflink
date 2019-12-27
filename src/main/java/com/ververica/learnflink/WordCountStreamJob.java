package com.ververica.learnflink;

import com.ververica.learnflink.function.StringTokenizer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreamJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new StringTokenizer())
                .uid("word_count_flatmap")
                .keyBy( t -> t.f0 )
                .sum(1)
                // Use sum(1) like the line above or a custom KeyedProcesssFunction like the line below.
                // (In the latter case, change WordCountBootstrapJob to use WordCountCustomBootstrapper as well)
                //.process(new WordCountCustomProcessFunction())
                .uid("word_count_sum");

        dataStream.print();

        env.execute("Word Count Stream Job");
    }
}