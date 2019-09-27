package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.FullTransaction;
import com.ververica.learnflink.entity.Transaction;
import com.ververica.learnflink.entity.TransactionDetails;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class TransactionDetailCoFlatMapFunction extends RichCoFlatMapFunction<Transaction, TransactionDetails, FullTransaction> {

    private ValueState<Transaction> transactionState;
    private ValueState<TransactionDetails> detailsState;

    @Override
    public void open(Configuration config) {
        RuntimeContext rc = getRuntimeContext();
        transactionState = rc.getState(new ValueStateDescriptor<>("transaction", Transaction.class));
        detailsState = rc.getState(new ValueStateDescriptor<>("details", TransactionDetails.class));
    }


    @Override
    public void flatMap1(Transaction value, Collector<FullTransaction> out) throws Exception {
        TransactionDetails details = detailsState.value();
        if ( details != null ) {
            out.collect(new FullTransaction(value, details.getCategory()));

            detailsState.clear();
            // TODO: needed?
            if (transactionState.value() != null ) transactionState.clear();

        } else {
            transactionState.update(value);
        }
    }

    @Override
    public void flatMap2(TransactionDetails value, Collector<FullTransaction> out) throws Exception {
        Transaction transaction = transactionState.value();

        if (transaction != null) {
            out.collect(new FullTransaction(transaction, value.getCategory()));

            transactionState.clear();
            // TODO: needed?
            if (detailsState.value() != null) detailsState.clear();
        } else {
            detailsState.update(value);
        }
    }
}
