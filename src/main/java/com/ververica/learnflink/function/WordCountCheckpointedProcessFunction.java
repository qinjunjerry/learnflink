package com.ververica.learnflink.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class WordCountCheckpointedProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>
        implements CheckpointedFunction {
    private ValueState<Integer> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("custom", Integer.class);
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        if (state.value() == null ) {
            state.update(1);
        } else {
            state.update(state.value() + 1);
        }
        out.collect( new Tuple2<>(value.f0, state.value()));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        throw new Exception("snapshotState() for unknown reasons");

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}