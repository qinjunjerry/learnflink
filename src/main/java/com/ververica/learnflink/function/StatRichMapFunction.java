package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.RunningStats;
import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;


public class StatRichMapFunction extends RichMapFunction<Transaction, Tuple6<Long, Double, Double, Double, Long, Double>> {
    private ValueState<RunningStats> statState;

    @Override
    public void open(Configuration conf) {
        ValueStateDescriptor<RunningStats> descriptor = new ValueStateDescriptor<>("stats", RunningStats.class);
        statState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Tuple6<Long, Double, Double, Double, Long, Double> map(Transaction value) throws Exception {

        RunningStats stat = statState.value();

        if ( stat == null ) {
            stat = new RunningStats();
        }
        stat.add(value.getAmount());

        statState.update(stat);

        return new Tuple6<>(value.getAccountId(), stat.getMin(), stat.getMax(), stat.average(), stat.getNum(), stat.getTotal());
    }

}
