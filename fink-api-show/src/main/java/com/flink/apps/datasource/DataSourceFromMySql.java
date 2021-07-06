package com.flink.apps.datasource;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import com.flink.apps.constant.ConstantsWithStringLable;
import com.flink.apps.datasource.entity.Account;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;

/**
 * 自定义从MySQL加载数据
 *
 * @author root
 * @date 2021/7/6
 **/
public class DataSourceFromMySql {
    /**
     * 命令行执行入口方法.
     *
     * @param args 命令行参数.
     * @throws Exception 异常.
     */
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //S
        DataStreamSource<Entity> accountDataStreamSource = env.addSource(new AcountSourceWithMySql());
        //T
        //S
        accountDataStreamSource.print();
        //E
        env.execute();
    }

    public static class AcountSourceWithMySql extends RichParallelSourceFunction<Entity> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

        }

        @Override
        public void close() throws Exception {
            super.close();
        }

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
        public void run(SourceContext<Entity> ctx) throws Exception {
            while (true){
                //查询
                List<Entity> result = Db.use().query("SELECT * FROM account where id = ?", "1014041506545352708");
                Entity entity = result.get(0);
                ctx.collect(entity);
                Thread.sleep(ConstantsWithStringLable.THREE_THOUSAND);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
