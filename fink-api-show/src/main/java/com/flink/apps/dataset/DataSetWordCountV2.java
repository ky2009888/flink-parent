package com.flink.apps.dataset;

import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * 基于dataset版本的wordcount
 *
 * @author ky2009666
 * @date 2021/8/7
 **/
public class DataSetWordCountV2 {
    public static void main(String[] args) throws Exception {
        //E
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //S
        DataSource<String> dataSource = env.fromElements(
                ConstantsWithStringLable.WORD_COUNT_CONTENT_STR);
        //T
        FlatMapOperator<String, Tuple2<String, Integer>> wordsTuple2FlatMapOperator = dataSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        AggregateOperator<Tuple2<String, Integer>> resultData = wordsTuple2FlatMapOperator.groupBy(0).sum(1);
        //S
        resultData.print();
        //E
    }
}
