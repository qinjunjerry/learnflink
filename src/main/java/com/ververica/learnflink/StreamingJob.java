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

package com.ververica.learnflink;

import com.ververica.learnflink.entity.EnrichedTransaction;
import com.ververica.learnflink.entity.FraudAlert;
import com.ververica.learnflink.entity.Transaction;
import com.ververica.learnflink.entity.TransactionDetails;
import com.ververica.learnflink.function.*;
import com.ververica.learnflink.sink.EnrichedTransactionSink;
import com.ververica.learnflink.sink.FraudAlertSink;
import com.ververica.learnflink.sink.TransactionSink;
import com.ververica.learnflink.sink.Tuple6Sink;
import com.ververica.learnflink.source.TransactionDetailsSource;
import com.ververica.learnflink.source.TransactionSource;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		//final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//		// To load a specific flink-conf.yaml, provide the directory holding the conf file.
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(
//				StreamExecutionEnvironment.getDefaultLocalParallelism(),
//				GlobalConfiguration.loadConfiguration(".")
//		);

		// To enable the flink web UI
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
				GlobalConfiguration.loadConfiguration(".")
		);

		// To avoid using all CPU cores on the laptop
		env.setParallelism(2);

		filterStream(env);
		//enrichStream(env);
		//enrichStreamWithFlatMap(env);
		//statefulStreamStats(env);
		//fraudDetection(env);
		//connectStream(env);

	}

	private static void filterStream(StreamExecutionEnvironment env) throws Exception {
		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions");

		DataStream<Transaction> filteredTransactions = transactions
				.filter(new AccountFilterFunction())
				.name("filter");

		filteredTransactions
				.addSink(new TransactionSink())
				.name("filtered-transaction");

		// execute program
		env.execute("Flink Job: filter stream");
	}

    private static void enrichStream(StreamExecutionEnvironment env) throws Exception {
		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions");

		KeyedStream<Transaction, Long> keyedTransactions = transactions
				.keyBy(Transaction::getAccountId);

		DataStream<EnrichedTransaction> enrichedTransaction = keyedTransactions
				.map(new AddAccountNameFunction() )
				.name("add account name");

		enrichedTransaction
				.addSink(new EnrichedTransactionSink())
				.name("enriched transaction sink");


		// execute program
		env.execute("Flink Job: enrich stream");
	}

    private static void enrichStreamWithFlatMap(StreamExecutionEnvironment env) throws Exception {
		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions");

		KeyedStream<Transaction, Long> keyedTransactions = transactions
				.keyBy(Transaction::getAccountId);

		DataStream<EnrichedTransaction> enrichedTransaction = keyedTransactions
				.flatMap(new AddNameForRealAccount())
				.name("add account name flatMap");

		enrichedTransaction
				.addSink(new EnrichedTransactionSink())
				.name("enriched transaction sink");


		// execute program
		env.execute("Flink Job: enrich stream flatMap");
	}

    private static void statefulStreamStats(StreamExecutionEnvironment env) throws Exception {
		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions");

		KeyedStream<Transaction, Long> keyedTransactions = transactions
				.filter(new AccountFilterFunction())
				.keyBy(Transaction::getAccountId);

		DataStream<Tuple6<Long, Double, Double, Double, Long, Double>> statsStream = keyedTransactions
				.map(new StatRichMapFunction())
				.name("moving stats");

		statsStream
				// Optionally, get the max total spend
				//.timeWindowAll(Time.minutes(1))
				//.maxBy(5)
				.addSink(new Tuple6Sink())
				.name("enriched transaction sink");


		// execute program
		env.execute("Flink Job: stateful stream with running stats");
	}

    private static void fraudDetection(StreamExecutionEnvironment env) throws Exception {
		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions");

		DataStream<FraudAlert> fraudAlerts = transactions
				.filter(new AccountFilterFunction())
				.keyBy(Transaction::getAccountId)
				.process(new FraudDetector())
				.name("fraud detector");

		fraudAlerts
				.addSink(new FraudAlertSink())
				.name("fraud alerts");

		// execute program
		env.execute("Flink Job: fraud detection");
	}

    private static void connectStream(StreamExecutionEnvironment env) throws Exception {

		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions");

		KeyedStream<Transaction, Long> keyedTransactions = transactions
				.filter(new AccountFilterFunction())
				.keyBy(Transaction::getTransactionId);


		DataStream<TransactionDetails> transactionDetails = env
				.addSource(new TransactionDetailsSource())
				.name("transaction details");

		KeyedStream<TransactionDetails, Long> keyedTransactionDetails = transactionDetails
				.keyBy(TransactionDetails::getTransactionId);

		keyedTransactions
				.connect(keyedTransactionDetails)
				.flatMap(new TransactionDetailCoFlatMapFunction())
				.print()
				.name("transaction details sink");

		env.execute("Flink Job: connect stream");
	}
}

