package com.flink.apps.sql;

import com.flink.apps.constant.ConstantsWithStringLable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 用SQL进行单词统计.
 *
 * @author root
 * @date 2021/7/8
 **/
public class DataStreamWithSqlCountWords {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //S
        DataStreamSource<String> wordDataSource =
                env.fromCollection(Arrays.asList("Apache Flink is a framework and distributed processing engine"
                        , "for stateful computations over unbounded and bounded data "
                        , "Flink has been designed to run in all common cluster environments"));

        //T
        SingleOutputStreamOperator<WordCount> wordCountSingleOutputStreamOperator = wordDataSource.flatMap(new RichFlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String line, Collector<WordCount> collector) throws Exception {
                for (String word : line.split(ConstantsWithStringLable.SPACE_STRING)) {
                    collector.collect(new WordCount(word, 1));
                }
            }
        });
        Table wordTable = tableEnv.fromDataStream(wordCountSingleOutputStreamOperator, $("word"), $("count"));
        Table table = tableEnv.sqlQuery("select * from " + wordTable + " where word like 'f%' ");
        DataStream<Tuple2<Boolean, WordCount>> wordCountResult = tableEnv.toRetractStream(table, WordCount.class);
        //S
        wordCountResult.print();
        //E
        env.execute();
    }

    /**
     * 进行出现出现次数统计.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class WordCount implements Serializable {
        /**
         * 单词.
         */
        private String word;
        /**
         * 出现次数.
         */
        private int count;
    }
}
