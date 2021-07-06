package com.flink.apps.datasource;

import cn.hutool.core.util.StrUtil;
import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author root
 */
public class DataStreamSourceCollections {
    /**
     * 命令行执行入口.
     *
     * @param args 命令行参数.
     * @throws Exception 异常.
     */
    public static void main(String[] args) throws Exception {
        //E Environment 建立执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        streamEnv.setParallelism(1);
        //S Source
        DataStreamSource<String> streamSource = streamEnv.fromCollection(Arrays.asList("hello world ", " org apache flink api common RuntimeExecutionMode"));
        DataStreamSource<Long> numDataSource = streamEnv.fromSequence(1, 1000);
        //T transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(ConstantsWithStringLable.SPACE_STRING)) {
                    if(StrUtil.isNotBlank(word)){
                        collector.collect(Tuple2.of(word, 1));
                    }
                }
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1);
        //S Sink
        result.print();
        numDataSource.print();
        //E Execute
        streamEnv.execute();
    }
}
