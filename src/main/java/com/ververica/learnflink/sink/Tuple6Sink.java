package com.ververica.learnflink.sink;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tuple6Sink implements SinkFunction<Tuple6<Long, Double, Double, Double, Long, Double>> {

    static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(Tuple6Sink.class);

    @Override
    public void invoke(Tuple6<Long, Double, Double, Double, Long, Double> value, SinkFunction.Context context) {
        LOG.info( value.toString());
    }

}
