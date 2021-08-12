package com.flink.apps.trans;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流，将一个流分成多个流
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
public class TransformationDemo03Split {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<Long> numData = env.fromSequence(1, 100);
        //T
        OutputTag evenTag = new OutputTag("奇数", TypeInformation.of(Long.TYPE));
        OutputTag<Long> oddTag = new OutputTag("偶数", TypeInformation.of(Long.TYPE));
        SingleOutputStreamOperator<Long> process = numData.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long value, ProcessFunction<Long, Long>.Context ctx, Collector<Long> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(oddTag, value);
                } else {
                    ctx.output(evenTag, value);
                }
            }
        });
        DataStream<Long> eventSideOut = process.getSideOutput(evenTag);
        DataStream<Long> oddSideOut = process.getSideOutput(oddTag);
        //S
        eventSideOut.print("打印奇数:");
        oddSideOut.print("打印偶数:");
        //E
        env.execute();
    }
}
