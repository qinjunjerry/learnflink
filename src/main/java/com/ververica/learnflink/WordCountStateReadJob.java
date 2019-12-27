package com.ververica.learnflink;

import com.ververica.learnflink.function.WordCountStateReadFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

public class WordCountStateReadJob {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, "file:///tmp/bootstrapped_savepoint", new MemoryStateBackend());

        DataSet<Tuple2<String,Integer>> state = savepoint.readKeyedState("word_count_sum", new WordCountStateReadFunction());

        state.print();
    }
}
