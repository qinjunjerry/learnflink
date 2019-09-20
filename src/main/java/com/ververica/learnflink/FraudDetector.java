package com.ververica.learnflink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.ververica.learnflink.entity.FraudAlert;
import com.ververica.learnflink.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, FraudAlert> {

    private static final long serialVersionUID = 1L;

    public static final double SMALL_AMOUNT = 1.00;

    public static final double LARGE_AMOUNT = 500.00;

    public static final long ONE_MINUTE = 60 * 1000;

    private transient ValueState<Boolean> foundSmallTranState;

    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> foundSmallTranDescriptor = new ValueStateDescriptor<>(
                "foundSmallTran",
                Types.BOOLEAN);

        foundSmallTranState = getRuntimeContext().getState(foundSmallTranDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer",
                Types.LONG);

        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<FraudAlert> collector) throws Exception {

        // Get the current state for the current key
        Boolean lastTransactionWasSmall = foundSmallTranState.value();

        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                FraudAlert alert = new FraudAlert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }

            cleanUp(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            foundSmallTranState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);

            timerState.update(timer);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudAlert> out) throws Exception {
        timerState.clear();
        foundSmallTranState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        Long timer = timerState.value();

        if (timer != null) {
            ctx.timerService().deleteProcessingTimeTimer(timer);
            timerState.clear();
        }

        foundSmallTranState.clear();
    }
}
