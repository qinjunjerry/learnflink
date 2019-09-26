package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.EnrichedTransaction;
import com.ververica.learnflink.entity.Transaction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AddAccountNameFunctionTest {

    @Test
    public void testAddingAccountName() {
        AddAccountNameFunction f = new AddAccountNameFunction();

        Transaction t = new Transaction(0,0,0,0);

        assertEquals( f.map(t), new EnrichedTransaction(t) );

    }

}