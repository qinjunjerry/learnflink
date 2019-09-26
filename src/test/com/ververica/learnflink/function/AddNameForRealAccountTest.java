package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.EnrichedTransaction;
import com.ververica.learnflink.entity.Transaction;
import org.apache.flink.util.Collector;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class AddNameForRealAccountTest {

    @Test
    public void testFlatMapWithRealAccount() {
        AddNameForRealAccount theFlatMap = new AddNameForRealAccount();
        @SuppressWarnings("unchecked")
        Collector<EnrichedTransaction> collector = mock(Collector.class);
        Transaction t = new Transaction(0, 0, 0, 0);
        theFlatMap.flatMap(t, collector);
        EnrichedTransaction et = new EnrichedTransaction(t);
        verify(collector, times(1)).collect(et);

    }

    @Test
    public void testFlatMapWithDummyAccount() {
        AddNameForRealAccount theFlatMap = new AddNameForRealAccount();
        @SuppressWarnings("unchecked")
        Collector<EnrichedTransaction> collector = mock(Collector.class);
        Transaction t = new Transaction(99, 0, 0, 0);
        theFlatMap.flatMap(t, collector);
        EnrichedTransaction et = new EnrichedTransaction(t);
        verify(collector, never()).collect(et);

    }
}