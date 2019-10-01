package com.ververica.learnflink.function;

import com.ververica.learnflink.TestableStreamingJob;
import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestableStreamingJobTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    public void testFraudDetection() throws Exception {
        // source
        List<Transaction> transactions = Arrays.asList(
                new Transaction(1,0,0,0),
                new Transaction(2,0,0,0),
                new Transaction(99,0,0,0)
        );


        TypeInformation<Transaction> typeInfo = TypeExtractor.getForObject(transactions.get(0));
        //TypeInformation<Transaction> typeInfo = TypeExtractor.getForClass(Transaction.class);
        //TypeInformation<Transaction> typeInfo = TypeExtractor.createTypeInfo(Transaction.class);

        TypeSerializer<Transaction> serializer = typeInfo.createSerializer(StreamExecutionEnvironment.getExecutionEnvironment().getConfig());

        SourceFunction<Transaction> source = new FromElementsFunction<>(serializer, transactions);


        // sink
        TransactionColSink sink = new TransactionColSink();

        // run integration test
        TestableStreamingJob job = new TestableStreamingJob(source, sink);
        job.execute();


        assertThat(TransactionColSink.result).containsExactlyInAnyOrder(
                new Transaction(1,0,0,0),
                new Transaction(2,0,0,0)
        );
    }

    public static class TransactionColSink implements SinkFunction<Transaction> {

        // This needs to be static because Flink serialize all operators before distributing them cross a cluster.
        // Without 'static', Flink runtime creates a different instance of TransactionColSink, which makes it difficult
        // to verify the result. Using 'static' is a way to work around it.
        public static final List<Transaction> result =
                Collections.synchronizedList(new ArrayList<>());
        @Override
        public void invoke(Transaction value, SinkFunction.Context context) throws Exception {
            result.add(value);

        }
    }

}