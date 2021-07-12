package com.flink.apps.hot.product;

import cn.hutool.core.comparator.CompareUtil;
import com.flink.apps.bean.ItemViewCount;
import com.flink.apps.bean.UserBehavior;
import com.flink.apps.constants.ConstansForHotProduct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 实时热门商品统计V1
 * 实时热门数据就是每隔5分钟统计最近1小时的热门商品.
 *
 * @author root
 * @date 2021/7/12
 **/
@Slf4j(topic = "最终计算结果:")
public class HotProductAnalysisV1Main {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置流的执行模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //设置并行度
        env.setParallelism(1);
        //S Source 读取数据
        DataStreamSource<String> data = env.readTextFile(ConstansForHotProduct.RESOURCES_USER_BEHAVIOR_CSV_LINUX);
        //T
        DataStream<ItemViewCount> aggregateDataStream = data.map(new ProductConvertRichMapFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                })
                .keyBy(new KeySelector<UserBehavior, Long>() {
                    @Override
                    public Long getKey(UserBehavior value) throws Exception {
                        return value.getItemId();
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(5))).aggregate(
                        new ProductAggregateFunction(), new ProductWindowFunction()
                ).keyBy(new KeySelector<ItemViewCount, Long>() {
                    /**
                     * User-defined function that deterministically extracts the key from an object.
                     * @param value The object to get the key from.
                     * @return The extracted key.
                     * @throws Exception Throwing an exception will cause the execution of the respective task to
                     *                   fail, and trigger recovery or cancellation of the program.
                     */
                    @Override
                    public Long getKey(ItemViewCount value) throws Exception {
                        return value.getWindowEnd();
                    }
                });
        //S
        //aggregateDataStream.print("打印统计结果:");
        SingleOutputStreamOperator<Object> processData = aggregateDataStream.process(new ProcessFunction<ItemViewCount, Object>() {
            /**
             * 存储结果.
             */
            private ListState<ItemViewCount> listState;

            /**
             * 初始化参数.
             * @param parameters 配置参数.
             * @throws Exception 异常.
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
            }

            @Override
            public void processElement(ItemViewCount value, Context ctx, Collector<Object> out) throws Exception {
                listState.add(value);
                //定义什么时间触发定时器
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            }

            /**
             * Called when a timer set using TimerService} fires.
             *
             * @param timestamp The timestamp of the firing timer.
             * @param ctx       An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
             *                  querying the  TimeDomain} of the firing timer and getting a  TimerService}
             *                  for registering timers and querying the time. The context is only valid during the
             *                  invocation of this method, do not store it.
             * @param out       The collector for returning result values.
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the
             *                   operation to fail and may trigger recovery.
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                ArrayList<ItemViewCount> viewCounts = Lists.newArrayList(listState.get().iterator());
                List<ItemViewCount> resultList = viewCounts.stream().sorted(new Comparator<ItemViewCount>() {
                    @Override
                    public int compare(ItemViewCount o1, ItemViewCount o2) {
                        return CompareUtil.compare(o1.getCount(), o2.getCount());
                    }
                }).collect(Collectors.toList());
                List<ItemViewCount> topList = Lists.newArrayList();
                int startIndex = resultList.size()-1;
                int endIndex = startIndex -3;
                for (int i = startIndex; i-endIndex>=0; i--) {
                    topList.add(resultList.get(i));
                }
                log.info("{}:",topList);
            }
        });
        //E
        env.execute();
    }

    /**
     * 自定义类型转换Map
     */
    private static class ProductConvertRichMapFunction extends RichMapFunction<String, UserBehavior> {
        @Override
        public UserBehavior map(String line) throws Exception {
            String[] productStr = line.split(ConstansForHotProduct.COMMA);
            //用户ID.
            Long userId = Long.parseLong(productStr[0].trim());
            //商品ID.
            Long itemId = Long.parseLong(productStr[1].trim());
            //商品类别.
            Integer categoryId = Integer.parseInt(productStr[2].trim());
            //用户行为.
            String behavior = productStr[3].trim();
            //时间戳.
            Long timestamp = Long.parseLong(productStr[4].trim());
            return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
        }
    }

    private static class ProductWindowFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
        /**
         * Evaluates the window and outputs none or several elements.
         *
         * @param aLong  The key for which this window is evaluated.
         * @param window The window that is being evaluated.
         * @param input  The elements in the window being evaluated.
         * @param out    A collector for emitting elements.
         * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
         */
        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            //商品ID.
            Long itemId = aLong;
            //窗口结束时间
            Long windowEnd = window.getEnd();
            //统计.
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    /**
     * 定义复杂的聚合函数.
     */
    private static class ProductAggregateFunction implements AggregateFunction<UserBehavior, Long, Long> {

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
}
