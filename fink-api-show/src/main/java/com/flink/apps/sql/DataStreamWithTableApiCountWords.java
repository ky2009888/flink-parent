package com.flink.apps.sql;

import com.flink.apps.constant.ConstantsWithStringLable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * count words with table api
 *
 * @author root
 * @date 2021/7/8
 **/
public class DataStreamWithTableApiCountWords {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);
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
        Table table = tabEnv.fromDataStream(wordCountSingleOutputStreamOperator, $("word"), $("wordCount"));
        Table selectResult = table.groupBy($("word"))
                .select($("word"), $("wordCount").sum().as("wordCount"))
                .filter($("word").like("f%"));
        //S
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tabEnv.toRetractStream(selectResult, Row.class);
        tuple2DataStream.print();
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
        private int wordCount;
    }
}
