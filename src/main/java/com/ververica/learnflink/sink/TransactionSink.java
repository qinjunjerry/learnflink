package com.ververica.learnflink.sink;

import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
   With RichSinkFunction, we get the RuntimeContext which gives us the subtask index
 */
public class TransactionSink extends RichSinkFunction<Transaction> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(TransactionSink.class);

    @Override
    public void invoke(Transaction value, SinkFunction.Context context) {
        int index = getRuntimeContext().getIndexOfThisSubtask() + 1;
        LOG.info(index + "> " + value.toString());
    }
}
