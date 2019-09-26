package com.ververica.learnflink.source;

import com.ververica.learnflink.entity.TransactionDetails;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransactionDetailsIterator implements Iterator<TransactionDetails>, Serializable {

    private static List<String> data = Arrays.asList(
            "Computers",
            "Books",
            "Household",
            "Baby",
            "Beauty"
    );

    private static final long INITIAL_TRANSACTION_ID = 0;

    private int index;
    private long transactionId;

    TransactionDetailsIterator() {
        this.transactionId = INITIAL_TRANSACTION_ID;
        this.index = 0;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public TransactionDetails next() {
        String category = data.get( this.index % 5 );

        this.index++;
        this.transactionId ++;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return new TransactionDetails(this.transactionId, category);

    }
}
