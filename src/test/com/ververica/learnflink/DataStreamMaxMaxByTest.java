package com.ververica.learnflink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

public class DataStreamMaxMaxByTest {

    public static class MyWordCount {
        private int count;
        private String word;
        private int frequency;

        public MyWordCount() {
        }

        public MyWordCount(int count, String word, int frequency) {
            this.count = count;
            this.word = word;
            this.frequency = frequency;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "MyWordCount{" +
                    "count=" + count +
                    ", word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }

    private MyWordCount[] data = new MyWordCount[]{
            new MyWordCount(1, "Hello", 1),
            new MyWordCount(2, "Hello", 2),
            new MyWordCount(3, "Hello", 3),
            new MyWordCount(1, "World", 3)
    };

    @Test
    public void testMax() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(data)
                .keyBy("word")
                .max("frequency")
                .addSink(new SinkFunction<MyWordCount>() {
                    @Override
                    public void invoke(MyWordCount value, Context context) {
                        System.err.println("\n" + value + "\n");
                    }
                });
        env.execute("testMax");
    }

    @Test
    public void testMaxBy() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(data)
                .keyBy("word")
                .maxBy("frequency")
                .addSink(new SinkFunction<MyWordCount>() {
                    @Override
                    public void invoke(MyWordCount value, Context context) {
                        System.err.println("\n" + value + "\n");
                    }
                });
        env.execute("testMaxBy");
    }
}