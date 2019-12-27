package com.ververica.learnflink.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

public class WordCountStateReadFunction extends KeyedStateReaderFunction<String, Tuple2<String,Integer>> {

    ValueState<Integer> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("custom", Types.INT);
        state = getRuntimeContext().getState(stateDescriptor);

    }

    @Override
    public void readKey(
            String key,
            Context ctx,
            Collector<Tuple2<String,Integer>> out) throws Exception {

        Tuple2<String,Integer> data = new Tuple2<String,Integer>();
        data.f0 = key;
        data.f1 = state.value();
        out.collect(data);
    }
}