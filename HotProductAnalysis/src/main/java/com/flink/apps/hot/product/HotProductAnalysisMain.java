package com.flink.apps.hot.product;

import com.flink.apps.bean.ItemViewCount;
import com.flink.apps.bean.UserBehavior;
import com.flink.apps.constants.ConstansForHotProduct;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 热门商品分析主类
 *
 * @author ky2009666
 * @date 2021/7/11
 **/
public class HotProductAnalysisMain {

    /**
     * 主程序入口方法.
     *
     * @param args 命令行参数.
     */
    public static void main(String[] args) throws Exception {
        //E 创建流执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        //Deprecated
        //In Flink 1.12 the default stream time characteristic has been changed to
        // TimeCharacteristic.EventTime, thus you don't need to call this method for
        // enabling event-time support anymore.
        //设置时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //S
        DataStreamSource<String> hotProudctDataStreamSource = env.readTextFile(ConstansForHotProduct.RESOURCES_USER_BEHAVIOR_CSV);
        //T
        DataStream<UserBehavior> userBehaviorData = hotProudctDataStreamSource.map(new RichMapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String line) throws Exception {
                String data[] = line.split(ConstansForHotProduct.COMMA);
                return new UserBehavior(
                        Long.parseLong(data[0]),
                        Long.parseLong(data[1]),
                        Integer.parseInt(data[2]),
                        data[3],
                        Long.parseLong(data[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                //分配时间戳
                return element.getTimestamp() * 1000L;
            }
        });
        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<ItemViewCount> aggregateDataStream = userBehaviorData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return ConstansForHotProduct.PV.equals(value.getBehavior());
            }
        })
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new HotProductAggregate(), new HotProductWindowCountResult());
        //
        SingleOutputStreamOperator<Object> windowEnd
                = aggregateDataStream.keyBy("windowEnd").process(new ProductTopFiveFunction(5));
        windowEnd.print();
        //S
        //E
        env.execute("热门商品统计");
    }

    /**
     * 进行具体的聚合操作.
     */
    public static class HotProductAggregate implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 通过窗口函数进行类型转换.
     */
    public static class HotProductWindowCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        /**
         * Evaluates the window and outputs none or several elements.
         *
         * @param tuple  The key for which this window is evaluated.
         * @param window The window that is being evaluated.
         * @param input  The elements in the window being evaluated.
         * @param out    A collector for emitting elements.
         * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
         */
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    /**
     * 进行数据排序
     */
    private static class ProductTopFiveFunction extends KeyedProcessFunction<Tuple, ItemViewCount, Object> {
        // 定义属性，top n的大小
        private Integer topSize;

        public ProductTopFiveFunction(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        /**
         * Initialization method for the function. It is called before the actual working methods (like
         * <i>map</i> or <i>join</i>) and thus suitable for one time setup work. For functions that are
         * part of an iteration, this method will be invoked at the beginning of each iteration
         * superstep.
         *
         * <p>The configuration object passed to the function can be used for configuration and
         * initialization. The configuration contains all parameters that were configured on the
         * function in the program composition.
         *
         * <pre>{@code
         * public class MyFilter extends RichFilterFunction<String> {
         *
         *     private String searchString;
         *
         *     public void open(Configuration parameters) {
         *         this.searchString = parameters.getString("foo");
         *     }
         *
         *     public boolean filter(String value) {
         *         return value.equals(searchString);
         *     }
         * }
         * }</pre>
         *
         * <p>By default, this method does nothing.
         *
         * @param parameters The configuration containing the parameters attached to the contract.
         * @throws Exception Implementations may forward exceptions, which are caught by the runtime.
         *                   When the runtime catches an exception, it aborts the task and lets the fail-over logic
         *                   decide whether to retry the task execution.
         * @see org.apache.flink.configuration.Configuration
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        /**
         * Process one element from the input stream.
         *
         * <p>This function can output zero or more elements using the {@link Collector} parameter and
         * also update internal state or set timers using the {@link Context} parameter.
         *
         * @param value The input value.
         * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting a
         *              {@link TimerService} for registering timers and querying the time. The context is only
         *              valid during the invocation of this method, do not store it.
         * @param out   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the
         *                   operation to fail and may trigger recovery.
         */
        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<Object> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(value);
            //设置定时器什么时间触发，Registers a timer to be fired when the event time watermark passes the given time
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        /**
         * Called when a timer set using {@link TimerService} fires.
         *
         * @param timestamp The timestamp of the firing timer.
         * @param ctx       An {@link OnTimerContext} that allows querying the timestamp, the {@link
         *                  TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
         *                  registering timers and querying the time. The context is only valid during the invocation
         *                  of this method, do not store it.
         * @param out       The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the
         *                   operation to fail and may trigger recovery.
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });
            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");
            // 控制输出频率
            Thread.sleep(1000L);
            out.collect(resultBuilder.toString());
        }
    }
}
