package com.ververica.learnflink.sink;

import com.ververica.learnflink.entity.TransactionDetails;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionDetailsSink implements SinkFunction<TransactionDetails> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionDetailsSink.class);

    @Override
    public void invoke(TransactionDetails value, SinkFunction.Context context) {
        LOG.info( value.toString() );
    }
}
