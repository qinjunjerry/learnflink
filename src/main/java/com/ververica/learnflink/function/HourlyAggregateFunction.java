package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;

public class HourlyAggregateFunction implements AggregateFunction<Transaction, Double, Double> {

    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(Transaction transaction, Double aDouble) {
        return Math.max(aDouble, transaction.getAmount());
    }

    @Override
    public Double getResult(Double aDouble) {
        return aDouble;
    }

    @Override
    public Double merge(Double aDouble, Double acc1) {
        return Math.max(aDouble, acc1);
    }
}
