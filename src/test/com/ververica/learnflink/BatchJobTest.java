package com.ververica.learnflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class BatchJobTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(new Configuration())
                            .build());

    @Test
    public void testFraudDetection() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(GlobalConfiguration.loadConfiguration("."));
        DataSet<String> data = env.fromCollection(Arrays.asList("a", "b"));
        data.filter((FilterFunction<String>) value -> value.startsWith("a"))
                .printOnTaskManager("prefix");
        env.execute("flink-test-batch-job");
    }
}