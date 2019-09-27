package com.ververica.learnflink.function;

import com.ververica.learnflink.StreamingJob;
import com.ververica.learnflink.entity.FullTransaction;
import com.ververica.learnflink.entity.Transaction;
import com.ververica.learnflink.entity.TransactionDetails;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class EnrichedTransactionWithTimeoutFunction extends KeyedCoProcessFunction<Long, Transaction, TransactionDetails, FullTransaction> {


    private ValueState<Transaction> transactionState;
    private ValueState<TransactionDetails> detailsState;


    @Override
    public void open(Configuration config) {
        RuntimeContext rc = getRuntimeContext();
        transactionState = rc.getState(new ValueStateDescriptor<>("transaction", Transaction.class));
        detailsState = rc.getState(new ValueStateDescriptor<>("details", TransactionDetails.class));
    }


    @Override
    public void processElement1(Transaction value, Context ctx, Collector<FullTransaction> out) throws Exception {
        TransactionDetails details = detailsState.value();
        if ( details != null ) {

            detailsState.clear();
            ctx.timerService().deleteEventTimeTimer(details.getTimestamp());

            out.collect(new FullTransaction(value, details.getCategory()));

        } else {
            transactionState.update(value);
            ctx.timerService().registerEventTimeTimer(value.getTimestamp());
        }
    }

    @Override
    public void processElement2(TransactionDetails value, Context ctx, Collector<FullTransaction> out) throws Exception {
        Transaction transaction = transactionState.value();

        if (transaction != null) {

            transactionState.clear();
            ctx.timerService().deleteEventTimeTimer(transaction.getTimestamp());
            out.collect(new FullTransaction(transaction, value.getCategory()));

        } else {
            detailsState.update(value);
            ctx.timerService().registerEventTimeTimer(value.getTimestamp());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FullTransaction> out) throws Exception {
        Transaction transaction = transactionState.value();
        TransactionDetails details = detailsState.value();

        if (transaction != null ) {
            ctx.output(StreamingJob.transactionSideOutput, transaction);
            transactionState.clear();

        }

        if ( details != null ) {
            ctx.output(StreamingJob.detailsSideOutput, details);
            detailsState.clear();
        }
    }
}
