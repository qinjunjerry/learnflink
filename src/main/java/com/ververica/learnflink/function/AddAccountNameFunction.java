package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.EnrichedTransaction;
import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.common.functions.MapFunction;

public class AddAccountNameFunction implements MapFunction<Transaction, EnrichedTransaction> {
    @Override
    public EnrichedTransaction map(Transaction value) {
        return new EnrichedTransaction(value);
    }
}
