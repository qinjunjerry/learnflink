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

import com.ververica.learnflink.function.StringTokenizer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCountBatchJob {

	public static void main(String[] args) throws Exception {

		final String[] TEXT = new String[] {
				"The story about fox and dog:",
				"The quick brown fox jumps over the lazy dog"
		};

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final ParameterTool params = ParameterTool.fromArgs(args);
		// input parameter
		DataSet<String> text;
		if (!params.has("input")) {
			System.out.println("No input file is given via --input, using the provided sample text in jar");
			text = env.fromElements(TEXT);
		} else {
			String input = params.get("input");
			text = env.readTextFile(input);

		}

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-element-tuple) containing: (word,1)
				text.flatMap(new StringTokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		counts.print();

	}

}