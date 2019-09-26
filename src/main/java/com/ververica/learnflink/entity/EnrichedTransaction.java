package com.ververica.learnflink.entity;

public class EnrichedTransaction extends Transaction {
    private String name;

    public EnrichedTransaction(Transaction t) {
        super(t.accountId, t.timestamp, t.transactionId, t.amount);
        this.name = "AccountName_" + this.accountId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "EnrichedTransaction{" +
                "name='" + name + '\'' +
                "} " + super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        EnrichedTransaction that = (EnrichedTransaction) o;

        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
