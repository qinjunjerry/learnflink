package com.ververica.learnflink.function;

import com.ververica.learnflink.entity.Transaction;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AccountFilterFunctionTest {

    @Test
    public void testWithRealAccount() {
        AccountFilterFunction filter = new AccountFilterFunction();
        assertTrue(filter.filter(new Transaction(1, 0, 0, 0)));
    }

    @Test
    public void testWithTestAccount() {
        AccountFilterFunction filter = new AccountFilterFunction();
        assertFalse(filter.filter(new Transaction(99, 0, 0, 0)));
    }

}