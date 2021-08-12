package com.flink.apps.source;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import com.flink.apps.source.entity.UserInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;

/**
 * FlinkDataSourceDemoCustomMySql06
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
public class FlinkDataSourceDemoCustomMySql06 {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<UserInfo> userData = env.addSource(new RichParallelSourceFunction<UserInfo>() {
            /**
             * 定义是否运行.
             */
            private volatile boolean isRunning = true;

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
            public void run(SourceContext<UserInfo> ctx) throws Exception {
                //程序读取数据
                while (isRunning) {
                    List<Entity> userList = Db.use().findAll("user_info");
                    UserInfo userInfo = new UserInfo();
                    for (Entity user : userList) {
                        userInfo.setUserid(user.getStr("userID"));
                        userInfo.setUsername(user.getStr("userName"));
                        userInfo.setUserage(user.getInt("userAge"));
                        ctx.collect(userInfo);
                    }
                    Thread.sleep(2000);
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
        });
        //T
        SingleOutputStreamOperator<UserInfo> userFilterData = userData.filter(new RichFilterFunction<UserInfo>() {
            @Override
            public boolean filter(UserInfo value) throws Exception {
                return value.getUserage() > 10;
            }
        });
        //S
        SingleOutputStreamOperator<String> userStr = userFilterData.map(new RichMapFunction<UserInfo, String>() {
            @Override
            public String map(UserInfo value) throws Exception {
                return "用户ID：" + value.getUserid() + "-----用户姓名:" + value.getUsername() + "----------用户年龄:" + value.getUserage();
            }
        });
        userStr.print("打印用户信息:");
        //E
        env.execute();
    }
}
