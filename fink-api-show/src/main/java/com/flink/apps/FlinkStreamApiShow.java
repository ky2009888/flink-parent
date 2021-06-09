package com.flink.apps;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author ky2009666
 * @description flink api usg show
 * @date 2021/6/3
 **/
@Slf4j(topic = "flink_api用法")
public class FlinkStreamApiShow {
    public static void main1(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置执行环境的并行度
        execEnv.setParallelism(4);
        //从java集合中读取数据
        DataStreamSource<SensorReading> streamSource =
                execEnv.fromCollection(
                        Arrays.asList(
                                new SensorReading("sensor_01", 14L, 34.5)
                                , new SensorReading("sensor_02", 15L, 36.5)
                        ));
        //打印内容
        streamSource.print();
        //执行环境
        execEnv.execute();
    }

    public static void main(String[] args) {
        //获取流的执行环境
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    }

    @Data
    @ToString
    public static class SensorReading {
        /**
         * 定义默认值.
         */
        @Builder.Default
        private long time = System.currentTimeMillis();
        /**
         * 定义sensorStr字符串.
         */
        @NonNull
        private String sensorStr;
        private long number;
        private double value;

        /**
         * 构造方法.
         *
         * @param sensorStr
         * @param number
         * @param value
         */
        public SensorReading(String sensorStr, long number, double value) {
            this.sensorStr = sensorStr;
            this.number = number;
            this.value = value;
        }
    }
}
