package com.ververica.learnflink.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

public class WordCountBootstrapper extends KeyedStateBootstrapFunction<String, Tuple2<String,Integer>> {
    private ValueState<Tuple2<String,Integer>> state;

    @Override
    public void open(Configuration parameters) {
        // The WordCount example is implemented with sum(1) of Tuple2<String, Integer>, where sum() is implemented
        // with {@link StreamGroupedReduce} which saves the state (type: Tuple2<String, Integer>) identified by the
        // name StreamGroupedReduce.STATE_NAME, i.e., "_op_state". This is why the ValueStateDescriptor is created
        // this way.
        ValueStateDescriptor<Tuple2<String,Integer>> descriptor = new ValueStateDescriptor<>(
                "_op_state",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){})
        );
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Tuple2<String,Integer> value, Context ctx) throws Exception {
        if (state.value() == null) {
            // Is it necessary to create a copy?
            state.update( value );
        } else {
            state.update( new Tuple2<>(value.f0, state.value().f1 + 1) );
        }
    }

}
