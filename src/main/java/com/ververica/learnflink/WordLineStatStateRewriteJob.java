package com.ververica.learnflink;

import com.ververica.learnflink.function.WordCountBootstrapper;
import com.ververica.learnflink.function.WordCountStateReadFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;

public class WordLineStatStateRewriteJob {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        // input parameter
        if (!params.has("input")) {
            System.err.println("Error: --input not found. Use --input to specify the savepoint path.");
            System.exit(1);
        }
        String oldSavepointPath = "file:///" + params.get("input");

        String output = params.get("output");


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint existingSavepoint = Savepoint.load(env, oldSavepointPath, new FsStateBackend("file://"+output));

        DataSet<Tuple3<Integer, Integer, String>> operatorState = existingSavepoint.readListState(
                "line_stat_map",
                "line_stat",
                TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, String>>() {})
        );
        operatorState.print();

        DataSet<Tuple2<String, Integer>> keyedState = existingSavepoint.readKeyedState(
                "word_count_sum",
                new WordCountStateReadFunction()
        );


        // Modify the keyed state
        DataSet<Tuple2<String, Integer>> modKeyedState = keyedState.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                if (value.f0.equals("the") ) {
                    return new Tuple2<>("the", 1000);
                } else {
                    return value;
                }
            }
        });

        modKeyedState.print();


        if (params.has("output")) {
            String newSavepointPath = "file:///" + params.get("output");

            // create BootstrapTransformation
            BootstrapTransformation<Tuple2<String, Integer>> transformation = OperatorTransformation
                    .bootstrapWith(modKeyedState)
                    .keyBy(v -> v.f0)
                    .transform(new WordCountBootstrapper());
            // If WordCountStreamJob uses a custom KeyedProcessFunction, then replace the line above with this line
            // .transform(new WordCountCustomBootstrapper());

            // create a savepoint with BootstrapTransformations (one per operator)
            // write the created savepoint to a given path
            existingSavepoint
                    .removeOperator("word_count_sum")
                    .withOperator("word_count_sum", transformation)
                    .write(newSavepointPath);

            // Do not forget to call env.execute() to submit for execution!
            // Otherwise, only a JobGraph is built, nothing is executed.
            env.execute();

        }

    }
}
