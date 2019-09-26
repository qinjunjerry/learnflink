package com.ververica.learnflink.entity;

import java.util.Objects;

public class FullTransaction extends Transaction {

    private String category;

    public FullTransaction() {
    }

    public FullTransaction(Transaction t, String category) {
        super(t.accountId, t.timestamp, t.transactionId, t.amount);
        this.category = category;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        FullTransaction that = (FullTransaction) o;

        return Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (category != null ? category.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FullTransaction{" +
                "category='" + category + '\'' +
                "} " + super.toString();
    }
}
