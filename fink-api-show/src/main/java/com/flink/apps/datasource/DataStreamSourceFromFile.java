package com.flink.apps.datasource;

import com.flink.apps.constant.ConstantsWithPath;
import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * read data from file（local file ,hdfs file,zip file)
 *
 * @author root
 * @date 2021/7/6
 **/
public class DataStreamSourceFromFile {

    public static void main(String[] args) throws Exception {
        // E Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        // S Source
        DataStreamSource<String> fileStreamSource = env.readTextFile(ConstantsWithPath.ROOT_RESULT_PATH);
        // T Transformation
        SingleOutputStreamOperator<Tuple1<Integer>> result = fileStreamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.contains(ConstantsWithStringLable.VEDIO_STR);
            }
        }).flatMap(new FlatMapFunction<String, Tuple1<Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple1<Integer>> collector) throws Exception {
                collector.collect(Tuple1.of(1));
            }
        }).keyBy(new KeySelector<Tuple1<Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple1<Integer> integerTuple1) throws Exception {
                return integerTuple1.f0;
            }
        }).sum(0);
        // S Sink
        result.print();
        // E Execution
        env.execute();
    }
}
