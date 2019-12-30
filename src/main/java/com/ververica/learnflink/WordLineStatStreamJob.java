package com.ververica.learnflink;

import com.ververica.learnflink.function.LineStatMapFunction;
import com.ververica.learnflink.function.StringTokenizer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordLineStatStreamJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> textStream = env
                .socketTextStream("localhost", 9999);

        DataStream<String> lineStream = textStream
                .map(new LineStatMapFunction())
                .uid("line_stat_map");

        DataStream<Tuple2<String,Integer>> wordStream = textStream
                .flatMap(new StringTokenizer())
                .uid("word_count_flatmap")

                .keyBy( t -> t.f0 )
                .sum(1)
                .uid("word_count_sum");

        lineStream.print();
        wordStream.print();

        env.execute("Word Line Stat Stream Job");
    }
}