package com.ververica.learnflink.eventtime;

import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TransactionTimeAssigner implements AssignerWithPeriodicWatermarks<Transaction> {

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    @Override
    public long extractTimestamp(Transaction element, long previousElementTimestamp) {
        long eventTime = element.getTimestamp();
        currentMaxTimestamp = eventTime;
        return eventTime;
    }
}
