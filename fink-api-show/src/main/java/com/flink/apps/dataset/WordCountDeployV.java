package com.flink.apps.dataset;

import com.flink.apps.constant.ConstantsWithPath;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * word count 使用lambda表达式进行处理,将发布结果发给hdfs.
 *
 * @author ky2009666
 * @date 2021/7/4
 **/
public class WordCountDeployV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStreamSource<String> dataStream = streamEnv.readTextFile(ConstantsWithPath.HELLO_PATH);
        DataStream<Tuple2<String, Integer>> resultStream = dataStream
                .flatMap((String line, Collector<String> out) -> Arrays.stream(line.split(" "))
                        .forEach(out::collect))
                //需要指定返回类型
                .returns(Types.STRING)
                .map((perWord) -> Tuple2.of(perWord, 1))
                //需要指定返回类型
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);
        //设置用户名
        System.setProperty("HADOOP_USER_NAME", "root");
        resultStream.writeAsText(ConstantsWithPath.HDFS_PATH + System.currentTimeMillis()).setParallelism(1);
        streamEnv.execute("word-count-lambda");
    }
}
