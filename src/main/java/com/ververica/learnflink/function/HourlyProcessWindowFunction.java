package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyProcessWindowFunction extends ProcessWindowFunction<Transaction, Tuple3<Long, Long, Double>, Long, TimeWindow> {

    @Override
    public void process(Long aLong, Context context, Iterable<Transaction> elements, Collector<Tuple3<Long, Long, Double>> out) throws Exception {

        out.collect(new Tuple3<>(aLong, context.window().getEnd(), elements.iterator().next().getAmount()));

    }
}
