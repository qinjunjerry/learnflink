/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.learnflink.entity;

import java.util.Objects;

/**
 * A simple transaction: accountId, timestamp, amount.
 */
public class Transaction {

	long accountId;

	long timestamp;

	long transactionId;

	double amount;

	public Transaction() { }

	public Transaction(long accountId, long timestamp, long transactionId, double amount) {
		this.accountId = accountId;
		this.timestamp = timestamp;
		this.transactionId = transactionId;
		this.amount = amount;
	}

	public long getAccountId() {
		return accountId;
	}

	public void setAccountId(long accountId) {
		this.accountId = accountId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(long transactionId) {
		this.transactionId = transactionId;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Transaction that = (Transaction) o;

		if (accountId != that.accountId) return false;
		if (timestamp != that.timestamp) return false;
		if (transactionId != that.transactionId) return false;
		return Double.compare(that.amount, amount) == 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(accountId, timestamp, transactionId, amount);
	}

	@Override
	public String toString() {
		return "Transaction{" +
				"accountId=" + accountId +
				", timestamp=" + timestamp +
				", transactionId=" + transactionId +
				", amount=" + amount +
				'}';
	}
}
