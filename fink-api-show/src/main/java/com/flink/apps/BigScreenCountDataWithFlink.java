package com.flink.apps;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * 大屏数据统计
 *
 * @author root
 * @date 2021/7/9
 **/
public class BigScreenCountDataWithFlink {
    /**
     * 命令行入口方法
     *
     * @param args 命令行参数.
     */
    public static void main(String[] args) throws Exception {
        //E Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        //S Source
        DataStreamSource<Tuple2<String, Double>> tyProductDataSource = env.addSource(new TyProductSource());
        //T Transformation
        //聚合结果
        DataStream<CategoryPojo> aggregateDataSource = tyProductDataSource
                //根据商品类别分类
                .keyBy(ty -> ty.f0)
                //代表从一天的0点0分0秒开始统计数据到23点59分59秒，-8代表中国时东八区
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                //代表每隔1秒钟触发一次计算
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new MyAggregateFunction(), new MyProcessWindow());
        //S Sink
        aggregateDataSource.print("初步聚合分类销售额:");
        //E Execute
        env.execute();
    }

    /**
     * config source datasource
     */
    public static class TyProductSource implements SourceFunction<Tuple2<String, Double>> {
        /**
         * 定义产生数据的循环标志.
         */
        private volatile boolean isRunning = true;
        /**
         * 定义商品分类.
         */
        private String[] categorys = {"女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游泳", "户外", "家具", "乐器", "办公"};

        /**
         * Starts the source. Implementations can use the {@link SourceContext} emit elements.
         *
         * <p>Sources that implement {@link
         * CheckpointedFunction} must lock on the checkpoint
         * lock (using a synchronized block) before updating internal state and emitting elements, to
         * make both an atomic operation:
         *
         * <pre>{@code
         *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
         *      private long count = 0L;
         *      private volatile boolean isRunning = true;
         *
         *      private transient ListState<Long> checkpointedCount;
         *
         *      public void run(SourceContext<T> ctx) {
         *          while (isRunning && count < 1000) {
         *              // this synchronized block ensures that state checkpointing,
         *              // internal state updates and emission of elements are an atomic operation
         *              synchronized (ctx.getCheckpointLock()) {
         *                  ctx.collect(count);
         *                  count++;
         *              }
         *          }
         *      }
         *
         *      public void cancel() {
         *          isRunning = false;
         *      }
         *
         *      public void initializeState(FunctionInitializationContext context) {
         *          this.checkpointedCount = context
         *              .getOperatorStateStore()
         *              .getListState(new ListStateDescriptor<>("count", Long.class));
         *
         *          if (context.isRestored()) {
         *              for (Long count : this.checkpointedCount.get()) {
         *                  this.count = count;
         *              }
         *          }
         *      }
         *
         *      public void snapshotState(FunctionSnapshotContext context) {
         *          this.checkpointedCount.clear();
         *          this.checkpointedCount.add(count);
         *      }
         * }
         * }</pre>
         *
         * @param ctx The context to emit elements to and for accessing locks.
         */
        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (isRunning) {
                //获取商品分类的下表索引
                int index = RandomUtil.randomInt(0, categorys.length - 1);
                //获取商品分类
                String category = categorys[index];
                double price = RandomUtil.randomDouble() * 100;
                ctx.collect(Tuple2.of(category, price));
                Thread.sleep(20);
            }
        }

        /**
         * Cancels the source. Most sources will have a while loop inside the {@link
         * #run(SourceContext)} method. The implementation needs to ensure that the source will break
         * out of that loop after this method is called.
         *
         * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
         * {@code false} in this method. That flag is checked in the loop condition.
         *
         * <p>When a source is canceled, the executing thread will also be interrupted (via {@link
         * Thread#interrupt()}). The interruption happens strictly after this method has been called, so
         * any interruption handler can rely on the fact that this method has completed. It is good
         * practice to make any flags altered by this method "volatile", in order to guarantee the
         * visibility of the effects of this method to any interruption handler.
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 定义聚合函数.
     */
    private static class MyAggregateFunction implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
        /**
         * 初始化累加器.
         *
         * @return double.
         */
        @Override
        public Double createAccumulator() {
            return 0D;
        }

        /**
         * 进行数据累加到累加器上面.
         *
         * @param stringDoubleTuple2 商品分类元组.
         * @param aDouble            累加器.
         * @return Double sum price.
         */
        @Override
        public Double add(Tuple2<String, Double> stringDoubleTuple2, Double aDouble) {
            return stringDoubleTuple2.f1 + aDouble;
        }

        /**
         * 获取结果.
         *
         * @param aDouble 当前结果.
         * @return aDouble.
         */
        @Override
        public Double getResult(Double aDouble) {
            return aDouble;
        }

        /**
         * 合并结果集.
         *
         * @param aDouble 当前结果.
         * @param acc1    实际结果.
         * @return aDouble + acc1.
         */
        @Override
        public Double merge(Double aDouble, Double acc1) {
            return aDouble + acc1;
        }
    }

    private static class MyProcessWindow implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {
        /**
         * Evaluates the window and outputs none or several elements.
         *
         * @param key    The key for which this window is evaluated.
         * @param window The window that is being evaluated.
         * @param input  The elements in the window being evaluated.
         * @param out    A collector for emitting elements.
         * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
         */
        @Override
        public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            //当前时间
            String currentTime = DateUtil.format(new Date(), DatePattern.NORM_DATETIME_PATTERN);
            //当前时间当前类别的销售额
            Double salesTotal = input.iterator().next();
            out.collect(new CategoryPojo(key, salesTotal, currentTime));
        }
    }

    /**
     * 用于存储聚合的结果
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class CategoryPojo {
        private String category;//分类名称
        private double totalPrice;//该分类总销售额
        private String dateTime;// 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可
    }
}
