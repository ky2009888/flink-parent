package com.flink.apps.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义数据源
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
public class FlinkDataSourceDemoCustom04 {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<Long> numData = env.addSource(new SourceFunction<Long>() {
            //定义计数器.
            private long count = 0L;
            //定义停止器.
            private volatile boolean isRunning = true;

            /**
             * 生成数据方法.
             * @param ctx 上下文.
             */
            public void run(SourceContext<Long> ctx) throws InterruptedException {
                while (isRunning && count < 30) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(count);
                        count += 2;
                        Thread.sleep(200);
                    }
                }
            }

            /**
             * 停止方法.
             */
            public void cancel() {
                isRunning = false;
            }

        });
        numData.print("打印原始数据:");
        //T
        SingleOutputStreamOperator<Long> handleData = numData.filter(new RichFilterFunction<Long>() {
            @Override
            public boolean filter(Long num) throws Exception {
                return num % 4 == 0;
            }
        });
        //S
        handleData.print("打印处理完成的数据:");
        //E
        env.execute();
    }
}
