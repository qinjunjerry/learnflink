package com.ververica.learnflink.source;

import com.ververica.learnflink.entity.TransactionDetails;

import java.io.Serializable;
import java.sql.Timestamp;
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
    private static final Timestamp INITIAL_TIMESTAMP = Timestamp.valueOf("2019-01-01 00:00:00");
    private static final long EVENT_TIME_INTERVAL = 2 * 60 * 1000;

    private int index;
    private long transactionId;
    private long timestamp;

    TransactionDetailsIterator() {
        this.transactionId = INITIAL_TRANSACTION_ID;
        this.index = 0;
        this.timestamp = INITIAL_TIMESTAMP.getTime();
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
        this.timestamp += EVENT_TIME_INTERVAL;

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return new TransactionDetails(this.transactionId, category, timestamp);

    }

}
