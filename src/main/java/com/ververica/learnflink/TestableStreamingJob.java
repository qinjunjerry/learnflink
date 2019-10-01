package com.ververica.learnflink;

import com.ververica.learnflink.entity.Transaction;
import com.ververica.learnflink.function.AccountFilterFunction;
import com.ververica.learnflink.sink.TransactionSink;
import com.ververica.learnflink.source.TransactionSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class TestableStreamingJob {
    private SourceFunction<Transaction> source;
    private SinkFunction<Transaction> sink;

    public TestableStreamingJob(SourceFunction<Transaction> source, SinkFunction<Transaction> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void execute() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // To avoid using all CPU cores on the laptop
        env.setParallelism(2);
        // To use event time instead of processing time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.addSource(source)
                // To tell the return type info. Otherwise org.apache.flink.api.common.functions.InvalidTypesException
                // is thrown with the following error message:
                // The return type of function 'Custom Source' could not be determined automatically, due to type erasure
                .returns(TypeInformation.of(Transaction.class))
                .name("source")
                .filter(new AccountFilterFunction())
                .name("filter")
                .addSink(sink)
                .name("filtered-transaction");
        env.execute();

    }

    public static void main(String[] args) throws Exception {
        TestableStreamingJob job = new TestableStreamingJob(new TransactionSource(), new TransactionSink());
        job.execute();
    }

}
