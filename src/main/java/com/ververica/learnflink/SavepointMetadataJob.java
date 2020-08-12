package com.ververica.learnflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SavepointMetadataJob {

    private static final String VALUE = "aaadefghijklmnopqrstuvwzzz";

    static class StringTokenizer implements FlatMapFunction<String, Tuple2<String, String>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, String>> out) throws Exception {
            // normalize and split the line
            String[] tokens = line.toLowerCase().split("\\W+");

            // emit the pairs of (word,VALUE)
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, VALUE));
                }
            }
        }
    }

    static class WordCountProcessFunction extends
            KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>>  {
        private ValueState<Tuple2<String, String>> state;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Tuple2<String, String>> descriptor = new ValueStateDescriptor<>(
                    "WordState",
                    TypeInformation.of(new TypeHint<Tuple2<String, String>>(){})
            );
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> event, Context context, Collector<Tuple2<String, String>> out) throws Exception {
            Tuple2<String, String> newValue;
            if (state.value() == null) {
                newValue = new Tuple2<>(event.f0, event.f1 + "+");
            } else {
                newValue = new Tuple2<>(event.f0, state.value().f1 + "+");
            }
            state.update(newValue);
            out.collect(newValue);
        }

    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new StringTokenizer())
                .keyBy( t -> t.f0 )
                .process(new WordCountProcessFunction())
                .uid("CountOperator2");

        dataStream.print();

        env.execute("Word Count Stream Job");


    }
}
