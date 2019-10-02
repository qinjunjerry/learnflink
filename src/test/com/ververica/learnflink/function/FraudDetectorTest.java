package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.FraudAlert;
import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FraudDetectorTest {

    @Test
    public void testFraudDetectorOperator() throws Exception {

        // initialize operator
        FraudDetector f = new FraudDetector();
        KeyedProcessOperator<Long, Transaction, FraudAlert> operator =
                new KeyedProcessOperator<>(f);

        // setup test harness
        KeyedOneInputStreamOperatorTestHarness<Long, Transaction, FraudAlert>  testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        Transaction::getAccountId,
                        TypeInformation.of(Long.class)
                );
        testHarness.open();

        // initial condition
        assertThat(testHarness.numKeyedStateEntries(), is(0));

        Transaction t1 = new Transaction(0, 0, 0, 0.3);
        testHarness.processElement(t1, 0L);

        assertThat(testHarness.numKeyedStateEntries(), is(2));
        assertThat(testHarness.numProcessingTimeTimers(), is(1));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        Transaction t2 = new Transaction(0, 1, 0, 501);
        testHarness.processElement(t2, 1L);

        assertThat(testHarness.numKeyedStateEntries(), is(0));
        assertThat(testHarness.numProcessingTimeTimers(), is(0));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        FraudAlert alert = new FraudAlert();
        alert.setId(0);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(alert, 1));
        TestHarnessUtil.assertOutputEquals( "Unexpected output", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testTimelyFraudDetectorOperator() throws Exception {

        // initialize operator
        FraudDetector f = new FraudDetector();
        KeyedProcessOperator<Long, Transaction, FraudAlert> operator =
                new KeyedProcessOperator<>(f);

        // setup test harness
        KeyedOneInputStreamOperatorTestHarness<Long, Transaction, FraudAlert>  testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        Transaction::getAccountId,
                        TypeInformation.of(Long.class)
                );

//        testHarness.setup();
//        testHarness.initializeEmptyState();
        testHarness.open();


        testHarness.setProcessingTime(0L);
        testHarness.processWatermark(0L);

        // initial condition
        assertThat(testHarness.numKeyedStateEntries(), is(0));

        Transaction t1 = new Transaction(0, 0, 0, 0.3);
        testHarness.processElement(t1, 0L);

        assertThat(testHarness.numKeyedStateEntries(), is(2));
        assertThat(testHarness.numProcessingTimeTimers(), is(1));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        testHarness.setProcessingTime(60 * 1000 - 1);
        testHarness.processWatermark(60 * 1000 - 1);

        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 60 * 1000 - 1);

        assertThat(testHarness.numKeyedStateEntries(), is(2));
        assertThat(testHarness.numProcessingTimeTimers(), is(1));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        // check operator state
        assertThat(f.getFoundState().value(), is(true));
        assertThat(f.getTimerState().value(), is(60000L));

        Transaction t2 = new Transaction(0, 1, 0, 501);
        testHarness.processElement(t2, 1L);

        assertThat(testHarness.numKeyedStateEntries(), is(0));
        assertThat(testHarness.numProcessingTimeTimers(), is(0));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        testHarness.processWatermark(60 * 1000);

        testHarness.close();
        // recreate testHarness, this is necessary because after restore, Flink uses another instance of the operator
        // otherwise, one gets error on initialization:
        // java.lang.IllegalStateException: TestHarness has already been initialized. Have you opened this harness before initializing it?
        testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        Transaction::getAccountId,
                        TypeInformation.of(Long.class)
                );
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();


        assertThat(testHarness.numKeyedStateEntries(), is(2));
        assertThat(testHarness.numProcessingTimeTimers(), is(1));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        // check operator state
        //assertThat(f.getFoundState().value(), is(true));
        //assertThat(f.getTimerState().value(), is(60000L));

        testHarness.processElement(t2, 1L);

        assertThat(testHarness.numKeyedStateEntries(), is(0));
        assertThat(testHarness.numProcessingTimeTimers(), is(0));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        testHarness.processWatermark(60 * 1000);


        //System.out.println("Processing Time: " + testHarness.getProcessingTime() );
        //System.out.println(testHarness.getOutput());

        FraudAlert alter = new FraudAlert();
        alter.setId(0);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        //expectedOutput.add(new Watermark(0L));
        //expectedOutput.add(new Watermark(60000L-1));
        expectedOutput.add(new StreamRecord<>(alter, 1L));
        expectedOutput.add(new Watermark(60000L));
        TestHarnessUtil.assertOutputEquals( "Unexpected output", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testNoFraudDetectorOperator() throws Exception {

        // initialize operator
        FraudDetector f = new FraudDetector();
        KeyedProcessOperator<Long, Transaction, FraudAlert> operator =
                new KeyedProcessOperator<>(f);

        // setup test harness
        KeyedOneInputStreamOperatorTestHarness<Long, Transaction, FraudAlert>  testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        Transaction::getAccountId,
                        TypeInformation.of(Long.class)
                );
        testHarness.open();

        testHarness.setProcessingTime(0L);
        testHarness.processWatermark(0L);

        // initial condition
        assertThat(testHarness.numKeyedStateEntries(), is(0));

        Transaction t1 = new Transaction(0, 0, 0, 0.3);
        testHarness.processElement(t1, 0L);

        assertThat(testHarness.numKeyedStateEntries(), is(2));
        assertThat(testHarness.numProcessingTimeTimers(), is(1));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        testHarness.setProcessingTime(60 * 1000);
        testHarness.processWatermark(60 * 1000);

        assertThat(testHarness.numKeyedStateEntries(), is(0));
        assertThat(testHarness.numProcessingTimeTimers(), is(0));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        Transaction t2 = new Transaction(0, 1, 0, 501);
        testHarness.processElement(t2, 1L);

        assertThat(testHarness.numKeyedStateEntries(), is(0));
        assertThat(testHarness.numProcessingTimeTimers(), is(0));
        assertThat(testHarness.numEventTimeTimers(), is(0));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new Watermark(0L));
        expectedOutput.add(new Watermark(60000L));
        TestHarnessUtil.assertOutputEquals( "Unexpected output", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

}
