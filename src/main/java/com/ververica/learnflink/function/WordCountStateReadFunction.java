package com.ververica.learnflink.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

public class WordCountStateReadFunction extends KeyedStateReaderFunction<String, Tuple2<String,Integer>> {

    ValueState<Tuple2<String, Integer>> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Tuple2<String,Integer>> stateDescriptor = new ValueStateDescriptor<>("_op_state",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){})
        );
        state = getRuntimeContext().getState(stateDescriptor);

    }

    @Override
    public void readKey(
            String key,
            Context ctx,
            Collector<Tuple2<String,Integer>> out) throws Exception {

        out.collect(state.value());
    }
}