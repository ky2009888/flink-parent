package com.flink.apps.window;

import com.flink.apps.constant.ConstantsWithStringLable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * flink中的滚动窗口和滑动窗口
 *
 * @author root
 * @date 2021/7/7
 **/
public class DataStreamWithWindow {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        //S
        DataStreamSource<String> socketTextStream =
                env.socketTextStream(ConstantsWithStringLable.H7_SERVER, ConstantsWithStringLable.H7_SERVER_PORT);
        //T
        SingleOutputStreamOperator<CartInfo> sensorMap = socketTextStream.map(new RichMapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String cart) throws Exception {
                return new CartInfo(
                        cart.split(",")[0],
                        Integer.parseInt(cart.split(",")[1]));
            }
        });
        //得到分组流
        KeyedStream<CartInfo, String> keyedStream = sensorMap.keyBy(cartInfo -> cartInfo.getSensorId());
        //得到滑动窗口流
        WindowedStream<CartInfo, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        //进行计算
        windowedStream.sum("count").print();
        //得到滑动窗口流
        WindowedStream<CartInfo, String, TimeWindow> windowedStream2 =
                keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        //进行计算
        windowedStream2.sum("count").print();
        //S
        //E
        env.execute();
    }

    /**
     * 信号灯实体类.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CartInfo {
        /**
         * 信号灯ID.
         */
        private String sensorId;
        /**
         * 信号灯数量.
         */
        private Integer count;
    }
}
