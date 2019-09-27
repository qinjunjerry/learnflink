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

package com.ververica.learnflink.source;

import com.ververica.learnflink.entity.Transaction;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * An iterator of transaction events which can be bounded and unbounded
 *
 */
final class TransactionIterator implements Iterator<Transaction>, Serializable {

	private static final long serialVersionUID = 1L;

	private static final long SIX_MINUTES = 2 * 60 * 1000;

	private static final Timestamp INITIAL_TIMESTAMP = Timestamp.valueOf("2019-01-01 00:00:00");

	private static final long INITIAL_TRANSACTION_ID = 0;

	private final boolean bounded;

	private int index = 0;

	private long timestamp;
	private long transactionId;

	TransactionIterator(boolean bounded) {
		this.bounded = bounded;
		this.timestamp = INITIAL_TIMESTAMP.getTime();
		this.transactionId = INITIAL_TRANSACTION_ID;
	}

	@Override
	public boolean hasNext() {
		if (index < data.size()) {
			return true;
		} else if (!bounded) {
			index = 0;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Transaction next() {
		Transaction transaction = data.get(index++);
		transaction.setTimestamp(timestamp);
		transaction.setTransactionId(transactionId);
		timestamp += SIX_MINUTES;
		transactionId++;
		return transaction;
	}

	private static List<Transaction> data = Arrays.asList(
		new Transaction(1,  0L, -1, 188.23),
		new Transaction(2,  0L, -1, 374.79),
		new Transaction(3,  0L, -1, 112.15),
		new Transaction(4,  0L, -1, 478.75),
		new Transaction(5,  0L, -1, 208.85),
		new Transaction(99, 0L, -1, 208.85),
		new Transaction(1,  0L, -1, 379.64),
		new Transaction(2,  0L, -1, 351.44),
		new Transaction(3,  0L, -1, 320.75),
		new Transaction(4,  0L, -1, 259.42),
		new Transaction(5,  0L, -1, 273.44),
		new Transaction(99, 0L, -1, 273.44),
		new Transaction(1,  0L, -1, 267.25),
		new Transaction(2,  0L, -1, 397.15),
		new Transaction(3,  0L, -1, 0.219),
		new Transaction(4,  0L, -1, 231.94),
		new Transaction(5,  0L, -1, 384.73),
		new Transaction(99, 0L, -1, 384.73),
		new Transaction(1,  0L, -1, 419.62),
		new Transaction(2,  0L, -1, 412.91),
		new Transaction(3,  0L, -1, 0.77),
		new Transaction(4,  0L, -1, 22.10),
		new Transaction(5,  0L, -1, 377.54),
		new Transaction(99, 0L, -1, 377.54),
		new Transaction(1,  0L, -1, 375.44),
		new Transaction(2,  0L, -1, 230.18),
		new Transaction(3,  0L, -1, 0.80),
		new Transaction(4,  0L, -1, 350.89),
		new Transaction(5,  0L, -1, 127.55),
		new Transaction(99, 0L, -1, 127.55),
		new Transaction(1,  0L, -1, 483.91),
		new Transaction(2,  0L, -1, 228.22),
		new Transaction(3,  0L, -1, 871.15),
		new Transaction(4,  0L, -1, 64.19),
		new Transaction(5,  0L, -1, 79.43),
		new Transaction(99, 0L, -1, 79.43),
		new Transaction(1,  0L, -1, 56.12),
		new Transaction(2,  0L, -1, 256.48),
		new Transaction(3,  0L, -1, 148.16),
		new Transaction(4,  0L, -1, 199.95),
		new Transaction(5,  0L, -1, 0.60),
		new Transaction(99, 0L, -1, 0.60),
		new Transaction(1,  0L, -1, 274.73),
		new Transaction(2,  0L, -1, 473.54),
		new Transaction(3,  0L, -1, 119.92),
		new Transaction(4,  0L, -1, 323.59),
		new Transaction(5,  0L, -1, 553.16),
		new Transaction(99, 0L, -1, 653.16),
		new Transaction(1,  0L, -1, 211.90),
		new Transaction(2,  0L, -1, 280.93),
		new Transaction(3,  0L, -1, 347.89),
		new Transaction(4,  0L, -1, 459.86),
		new Transaction(5,  0L, -1, 82.31),
		new Transaction(99, 0L, -1, 82.31),
		new Transaction(1,  0L, -1, 373.26),
		new Transaction(2,  0L, -1, 479.83),
		new Transaction(3,  0L, -1, 454.25),
		new Transaction(4,  0L, -1, 83.64),
		new Transaction(5,  0L, -1, 292.44),
		new Transaction(99, 0L, -1, 292.44)
	);
}
