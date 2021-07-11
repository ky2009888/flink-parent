package com.flink.apps.window;

import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 基于数量进行统计的窗口
 *
 * @author root .
 * @date 2021/7/7
 **/
public class DataStreamWithCountWindow {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<String> socketTextStream =
                env.socketTextStream(ConstantsWithStringLable.H7_SERVER, ConstantsWithStringLable.H7_SERVER_PORT);
        //T
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = socketTextStream.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(Tuple2.of(line.split(",")[0], Integer.parseInt(line.split(",")[1].trim())));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = streamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        //最近5条消息中，每出现3次进行统计 滑动窗口
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countWindow = tuple2StringKeyedStream.countWindow(5, 3);
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countWindow1 = tuple2StringKeyedStream.countWindow(5);
        //S
        countWindow.sum(1).print();
        //Tumbling window
        countWindow1.sum(1).print();
        //E
        env.execute("基于次数进行统计的窗口");
    }
}
