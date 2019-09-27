package com.ververica.learnflink.entity;

import java.util.Objects;

public class TransactionDetails {

    private long transactionId;
    private String category;
    private long timestamp;

    public TransactionDetails() { }

    public TransactionDetails(long transactionId, String category, long timestamp) {
        this.transactionId = transactionId;
        this.category = category;
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransactionDetails details = (TransactionDetails) o;

        if (transactionId != details.transactionId) return false;
        if (timestamp != details.timestamp) return false;
        return Objects.equals(category, details.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, category, timestamp);
    }

    @Override
    public String toString() {
        return "TransactionDetails{" +
                "transactionId=" + transactionId +
                ", category='" + category + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
