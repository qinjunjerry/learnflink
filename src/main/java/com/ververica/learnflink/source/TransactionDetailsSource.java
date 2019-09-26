package com.ververica.learnflink.source;

import com.ververica.learnflink.entity.TransactionDetails;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

public class TransactionDetailsSource extends FromIteratorFunction<TransactionDetails> {

    public TransactionDetailsSource() {
        super(new TransactionDetailsIterator());
    }
}