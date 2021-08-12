package com.flink.apps.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * RichParallelSourceFunction
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
public class RichParallelSourceFunctionCustom05 {
    /**
     * 命令行执行入口方法
     *
     * @param args 命令行参数
     * @throws Exception 异常
     */
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S RichParallelSourceFunction 多功能并行数据源
        DataStreamSource<Long> numData = env.addSource(new RichParallelSourceFunction<Long>() {
            //计数器
            private long count = 0L;
            //是否执行
            private volatile boolean isRunning = true;

            /**
             * 执行运行，生产数据.
             * @param ctx 上下文.
             */
            public void run(SourceContext<Long> ctx) {
                while (isRunning && count < 30) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(count);
                        count += 3;
                    }
                }
            }

            public void cancel() {
                isRunning = false;
            }

        });
        //T
        //TODO NONE
        //S
        numData.print("执行数据集:");
        //E
        env.execute("自定义数据");
    }
}
