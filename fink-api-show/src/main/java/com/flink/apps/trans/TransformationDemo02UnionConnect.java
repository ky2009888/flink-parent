package com.flink.apps.trans;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 流合并
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
public class TransformationDemo02UnionConnect {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<Long> numData = env.fromSequence(1, 100);
        DataStreamSource<String> aDataStream = env.addSource(new RichParallelSourceFunction<String>() {
            //定义是否运行
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect("A:" + System.currentTimeMillis());
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        DataStreamSource<String> bDataStream = env.addSource(new RichParallelSourceFunction<String>() {
            //定义是否运行
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect("B:" + System.currentTimeMillis());
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        //T
        DataStream<String> dataStream = aDataStream.union(bDataStream);
        //S
        dataStream.print("打印合并之后的字符流数据:");
        ConnectedStreams<Long, String> connectedStreams = numData.connect(dataStream);
        //IN1, IN2, OUT
        SingleOutputStreamOperator<String> map = connectedStreams.map(new RichCoMapFunction<Long, String, String>() {
            @Override
            public String map1(Long value) throws Exception {
                return "C:" + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });
        map.print("打印最终的数据:");
        //E
        env.execute();
    }
}
