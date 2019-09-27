package com.ververica.learnflink.eventtime;

import com.ververica.learnflink.entity.TransactionDetails;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TransactionDetailsTimeAssigner implements AssignerWithPeriodicWatermarks<TransactionDetails> {

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    @Override
    public long extractTimestamp(TransactionDetails element, long previousElementTimestamp) {
        long eventTime = element.getTimestamp();
        currentMaxTimestamp = eventTime;
        return eventTime;
    }

}
