package com.ververica.learnflink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.junit.jupiter.params.shadow.com.univocity.parsers.csv.Csv;


public class TableJob {
    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        final Schema schema = new Schema()
                .field("a", DataTypes.INT())
                .field("b", DataTypes.STRING())
                .field("c", DataTypes.FLOAT());


//        DataStream<Tuple2<Long, String>> stream = ...
//        Table table1 = tableEnv.fromDataStream(stream);

//        tableEnv.connect(new FileSystem("/path/to/file"))
//                .withFormat(new Csv().fieldDelimiter('|').deriveSchema())
//                .withSchema(schema)
//                .createTemporaryTable("CsvSinkTable");

    }
}
