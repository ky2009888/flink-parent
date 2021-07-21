package com.flink.apps.window;

import com.flink.apps.constant.ConstantsWithStringLable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * 每5秒中统计一次，最近5秒钟内，通过各个路口红绿灯汽车的数量
 *
 * @author root
 * @date 2021/7/13
 **/
public class CountLightsWithTumblingWindow {
    /**
     * 主启动类.
     *
     * @param args 命令行参数.
     * @throws Exception 异常.
     */
    public static void main(String[] args) throws Exception {
        //E 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        //S 建立数据源
        DataStreamSource<String> data = env.socketTextStream(ConstantsWithStringLable.HOSTNAME_64_1, ConstantsWithStringLable.H7_SERVER_PORT);
        //T 进行流处理
        SingleOutputStreamOperator<LightCount> result = data.map(new RichMapFunction<String, Light>() {
            @Override
            public Light map(String value) throws Exception {
                if(value.trim().length() > 0){
                    String[] lights = value.split(ConstantsWithStringLable.COMMA);
                    //红绿灯编号.
                    String lightNum = lights[0].trim();
                    //通过红绿灯车辆的数量.
                    long passNum = Long.parseLong(lights[1].trim());
                    return new Light(lightNum, passNum);
                }
                return  new Light("0", 0L);
            }
        }).keyBy(new KeySelector<Light, String>() {
            /**
             * User-defined function that deterministically extracts the key from an object.
             *
             * <p>For example for a class:
             *
             * <pre>
             * 	public class Word {
             * 		String word;
             * 		int count;
             *        }
             * </pre>
             *
             * <p>The key extractor could return the word as a key to group all Word objects by the String
             * they contain.
             *
             * <p>The code would look like this
             *
             * <pre>
             * 	public String getKey(Word w) {
             * 		return w.word;
             *    }
             * </pre>
             *
             * @param value The object to get the key from.
             * @return The extracted key.
             * @throws Exception Throwing an exception will cause the execution of the respective task to
             *                   fail, and trigger recovery or cancellation of the program.
             */
            @Override
            public String getKey(Light value) throws Exception {
                return value.getLightNum();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new AggregateFunction<Light, Long, Long>() {
            /**
             * Creates a new accumulator, starting a new aggregate.
             *
             * <p>The new accumulator is typically meaningless unless a value is added via {@link
             *
             * <p>The accumulator is the state of a running aggregation. When a program has multiple
             * aggregates in progress (such as per key and window), the state (per key and window) is the
             * size of the accumulator.
             *
             * @return A new accumulator, corresponding to an empty aggregate.
             */
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            /**
             * Adds the given input value to the given accumulator, returning the new accumulator value.
             *
             * <p>For efficiency, the input accumulator may be modified and returned.
             *
             * @param value       The value to add
             * @param accumulator The accumulator to add the value to
             * @return The accumulator with the updated state
             */
            @Override
            public Long add(Light value, Long accumulator) {
                return accumulator + value.passNum;
            }

            /**
             * Gets the result of the aggregation from the accumulator.
             *
             * @param accumulator The accumulator of the aggregation
             * @return The final aggregation result.
             */
            @Override
            public Long getResult(Long accumulator) {
                return accumulator;
            }

            /**
             * Merges two accumulators, returning an accumulator with the merged state.
             *
             * <p>This function may reuse any of the given accumulators as the target for the merge and
             * return that. The assumption is that the given accumulators will not be used any more after
             * having been passed to this function.
             *
             * @param a An accumulator to merge
             * @param b Another accumulator to merge
             * @return The accumulator with the merged state
             */
            @Override
            public Long merge(Long a, Long b) {
                return a + b;
            }
        }, new ProcessWindowFunction<Long, LightCount, String, TimeWindow>() {
            /**
             * Evaluates the window and outputs none or several elements.
             *
             * @param lightNum        The key for which this window is evaluated.
             * @param context  The context in which the window is being evaluated.
             * @param elements The elements in the window being evaluated.
             * @param out      A collector for emitting elements.
             * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
             */
            @Override
            public void process(String lightNum, Context context, Iterable<Long> elements, Collector<LightCount> out) throws Exception {
                out.collect(new LightCount(lightNum, elements.iterator().next()));
            }
        });
        //S 进行结果输出
        result.print("5秒钟内个路口红绿灯通过的车辆数量：");
        //E 开启流环境
        env.execute("红绿灯通过车辆统计Job");
    }

    /**
     * 红绿灯对象.
     */
    @ToString
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Light implements Serializable {
        /**
         * 红绿灯编号.
         */
        private String lightNum;
        /**
         * 定义通过红绿灯车辆的数量.
         */
        private long passNum;
    }

    /**
     * 红绿灯统计对象.
     */
    @ToString
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class LightCount implements Serializable {
        /**
         * 红绿灯编号.
         */
        private String lightNum;
        /**
         * 定义通过红绿灯车辆的数量.
         */
        private long passNum;
    }
}
