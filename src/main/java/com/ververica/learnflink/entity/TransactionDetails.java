package com.ververica.learnflink.entity;

import java.util.Objects;

public class TransactionDetails {

    private long transactionId;
    private String category;

    public TransactionDetails() { }

    public TransactionDetails(long transactionId, String category) {
        this.transactionId = transactionId;
        this.category = category;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransactionDetails that = (TransactionDetails) o;

        if (transactionId != that.transactionId) return false;
        return category.equals(that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, category);
    }

    @Override
    public String toString() {
        return "TransactionCategory{" +
                "transactionId=" + transactionId +
                ", category=" + category +
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
}
