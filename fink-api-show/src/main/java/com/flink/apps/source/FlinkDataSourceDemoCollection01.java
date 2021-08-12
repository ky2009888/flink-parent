package com.flink.apps.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Flink数据的输入方式演示
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
@Slf4j
public class FlinkDataSourceDemoCollection01 {
    public static void main(String[] args) {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<String> a1DataSource = env.fromElements("a1", "a2", "b2", "b3");
        DataStreamSource<String> c1DataSource = env.fromCollection(Arrays.asList("c1", "c2", "c3"));
        DataStreamSource<Long> numDataSource = env.fromSequence(1, 10);
        //T
        //TODO none
        //S
        a1DataSource.print("打印A1的数据集:");
        c1DataSource.print("打印C1的数据集:");
        numDataSource.print("打印序列生成的数据集");
        //E
        try {
            env.execute();
        } catch (Exception e) {
            log.error("Exception:{}", e.getMessage());
        }
    }
}
