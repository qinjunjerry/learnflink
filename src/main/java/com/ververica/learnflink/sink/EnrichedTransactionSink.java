package com.ververica.learnflink.sink;

import com.ververica.learnflink.entity.EnrichedTransaction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichedTransactionSink implements SinkFunction<EnrichedTransaction> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(EnrichedTransactionSink.class);

    @Override
    public void invoke(EnrichedTransaction value, SinkFunction.Context context) {
        LOG.info(value.toString());
    }
}
