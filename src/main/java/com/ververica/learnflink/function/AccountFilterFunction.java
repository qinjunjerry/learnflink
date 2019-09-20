package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.common.functions.FilterFunction;

public class AccountFilterFunction implements FilterFunction<Transaction> {
    @Override
    public boolean filter(Transaction transaction) throws Exception {
        return transaction.getAccountId() != 5;
    }
}
