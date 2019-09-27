package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.common.functions.ReduceFunction;

public class HourlyReduceFunction implements ReduceFunction<Transaction> {
    @Override
    public Transaction reduce(Transaction transaction, Transaction t1) throws Exception {
        return transaction.getAmount() > t1.getAmount() ?  transaction : t1;
    }
}
