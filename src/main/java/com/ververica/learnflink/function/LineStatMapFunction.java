package com.ververica.learnflink.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class LineStatMapFunction extends RichMapFunction<String,String> implements CheckpointedFunction {

    private Integer lineCount;
    private Integer maxLineLen;
    private String  longestLine;

    private transient ListState<Tuple3<Integer, Integer, String>> checkpointedState;

    public LineStatMapFunction() {
        this.lineCount = 0;
        this.maxLineLen = -1;
        this.longestLine = "";
    }

    @Override
    public String map(String line) throws Exception {
        if (lineCount == 0) {
            lineCount = 1;
        } else {
            lineCount += 1;
        }

        if (maxLineLen == -1 ||
                line.length() > maxLineLen ) {
            maxLineLen = line.length();
            longestLine =  line;
        }

        return line;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        checkpointedState.add( new Tuple3<>(lineCount, maxLineLen, longestLine) );
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple3<Integer, Integer, String>> descriptor =
                new ListStateDescriptor<>(
                        "line_stat",
                        TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, String>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);


        if (context.isRestored()) {
            for (Tuple3<Integer, Integer, String> element : checkpointedState.get()) {
                lineCount = element.f0;
                maxLineLen = element.f1;
                longestLine = element.f2;
            }
        }

    }
}
