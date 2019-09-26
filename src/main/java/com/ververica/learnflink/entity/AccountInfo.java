package com.ververica.learnflink.entity;

import java.util.Objects;

public class AccountInfo {

    private long accountId;
    private String name;

    public AccountInfo() {}

    public AccountInfo(long accountId, String name) {
        this.accountId = accountId;
        this.name = name;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AccountInfo that = (AccountInfo) o;

        if (accountId != that.accountId) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, name);
    }

    @Override
    public String toString() {
        return "AccountInfo{" +
                "accountId=" + accountId +
                ", name='" + name + '\'' +
                '}';
    }
}
