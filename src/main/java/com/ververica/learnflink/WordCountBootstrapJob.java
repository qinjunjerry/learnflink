package com.ververica.learnflink;

import com.ververica.learnflink.function.StringTokenizer;
import com.ververica.learnflink.function.WordCountBootstrapper;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;

public class WordCountBootstrapJob {

    public static void main(String[] args) throws Exception {

        final String OUTPUT = "/tmp/bootstrapped_savepoint";
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

        // output parameter
        if (!params.has("output")) {
            System.out.println("No output path is given via --output, using " + OUTPUT);
        }
        String output = params.get("output", OUTPUT);


        // Construct DataSet
        DataSet<Tuple2<String,Integer>> counts = text.flatMap(new StringTokenizer());

        // create BootstrapTransformation
        BootstrapTransformation<Tuple2<String,Integer>> transformation = OperatorTransformation
                .bootstrapWith(counts)
                .keyBy(v -> v.f0)
                .transform(new WordCountBootstrapper());
                // If WordCountStreamJob uses a custom KeyedProcessFunction, then replace the line above with this line
                // .transform(new WordCountCustomBootstrapper());

        // create a savepoint with BootstrapTransformations (one per operator)
        // write the created savepoint to a given path
        Savepoint.create(new MemoryStateBackend(), 128)
               .withOperator("word_count_sum", transformation)
               .write(output);

       // Do not forget to call env.execute() to submit for execution!
       // Otherwise, only a JobGraph is built, nothing is executed.
       env.execute();

    }



}
