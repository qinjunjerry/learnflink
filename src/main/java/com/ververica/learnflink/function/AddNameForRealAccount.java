package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.EnrichedTransaction;
import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class AddNameForRealAccount implements FlatMapFunction<Transaction, EnrichedTransaction> {

    @Override
    public void flatMap(Transaction value, Collector<EnrichedTransaction> out) {
        if (value.getAccountId() != 99 ) {
            out.collect( new EnrichedTransaction(value) );
        }
    }
}
