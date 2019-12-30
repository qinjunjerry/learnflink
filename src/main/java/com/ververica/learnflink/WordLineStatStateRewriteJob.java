package com.ververica.learnflink;

import com.ververica.learnflink.function.WordCountStateReadFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

public class WordLineStatStateRewriteJob {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        // input parameter
        if (!params.has("input")) {
            System.err.println("Error: --input not found. Use --input to specify the savepoint path.");
            System.exit(1);
        }

        String savepointPath = "file:///" + params.get("input");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, savepointPath, new MemoryStateBackend());


        DataSet<Tuple3<Integer, Integer, String>> state = savepoint.readListState(
                "line_stat_map",
                "line_stat",
                TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, String>>() {})
        );
        state.print();

        DataSet<Tuple2<String, Integer>> state2 = savepoint.readKeyedState("word_count_sum", new WordCountStateReadFunction());
        state2.print();
    }
}
