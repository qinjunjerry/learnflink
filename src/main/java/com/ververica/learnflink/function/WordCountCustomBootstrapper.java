package com.ververica.learnflink.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

public class WordCountCustomBootstrapper extends KeyedStateBootstrapFunction<String, Tuple2<String,Integer>> {
    private ValueState<Integer> state;

    @Override
    public void open(Configuration parameters) {
        // The WordCountCustom example is implemented with a custom KeyedProcessFunction which saves the state
        // (type: Integer) identified by the name "custom". This is why the ValueStateDescriptor is created this way.
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                "custom",
                TypeInformation.of(new TypeHint<Integer>(){})
        );
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Tuple2<String,Integer> value, Context ctx) throws Exception {
        if (state.value() == null) {
            state.update( 1 );
        } else {
            state.update( state.value() + 1 );
        }
    }

}
