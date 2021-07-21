package com.flink.apps.datasource;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.json.JSONObject;
import com.flink.apps.constant.ConstantsWithStringLable;
import com.flink.apps.datasource.entity.UserInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 通过案例展示broadcast的广播流的用法
 *
 * @author ky2009666
 * @date 2021/7/19
 **/
public class BroadcastStateConfigUpdate {
    /**
     * 命令行启动类
     *
     * @param args 命令行参数.
     * @throws Exception 异常.
     */
    public static void main(String[] args) throws Exception {
        //E 创建流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        JdbcInputFormat jdbcDataFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(ConstantsWithStringLable.JDBC_DRIVER_CLASS_STR)
                .setDBUrl(ConstantsWithStringLable.MYSQL_CONNECTION_STR)
                .setUsername(ConstantsWithStringLable.MYSQL_FLINK_DB_USERNAME)
                .setPassword(ConstantsWithStringLable.MYSQL_FLINK_DB_PASSWORD)
                .setQuery("select userID,userName,userAge from user_info ")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO))
                .finish();
        DataStreamSource<Row> mysqlUserInfo = env.createInput(jdbcDataFormat);
        SingleOutputStreamOperator<Map<String, UserInfo>> userMap = mysqlUserInfo.map(new RichMapFunction<Row, Map<String, UserInfo>>() {
            @Override
            public Map<String, UserInfo> map(Row value) throws Exception {
                UserInfo userInfo = new UserInfo(value.getField(0).toString(), value.getField(1).toString(), Integer.parseInt(value.getField(2).toString()));
                System.out.println("userInfo.toString() = " + userInfo.toString());
                Map userMap = MapUtil.newHashMap(16);
                userMap.put(value.getField(0).toString(), userInfo);
                return userMap;
            }
        });
        DataStreamSource<ClickInfo> clickInfoData = env.addSource(new ClickDataSource());
        //clickInfoData.print("打印点击事件");
        MapStateDescriptor<Void, Map<String, UserInfo>> descriptor =
                new MapStateDescriptor("userData", Types.VOID, Types.MAP(TypeInformation.of(String.class), TypeInformation.of(UserInfo.class)));
        BroadcastStream<Map<String, UserInfo>> mysqlUserBroadcast = userMap.broadcast(descriptor);
        BroadcastConnectedStream<ClickInfo, Map<String, UserInfo>> connect = clickInfoData.connect(mysqlUserBroadcast);

        //T
        SingleOutputStreamOperator<String> process = connect.process(new BroadcastProcessFunction<ClickInfo, Map<String, UserInfo>, String>() {
            @Override
            public void processElement(ClickInfo value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                String userId = value.getUserId();
                if (ctx != null) {
                    ReadOnlyBroadcastState<Void, Map<String, UserInfo>> broadcastState = ctx.getBroadcastState(descriptor);
                    Map<String, UserInfo> userMap = broadcastState.get(null);
                    if (userMap != null) {
                        UserInfo userInfo = userMap.get(userId);
                        JSONObject result = new JSONObject();
                        if (userInfo != null) {
                            result.set("userId", userInfo.getUserid())
                                    .set("userAge", userInfo.getUserage())
                                    .set("userName", userInfo.getUsername())
                                    .set("eventTime", value.getEventTime())
                                    .set("eventType", value.getEventType())
                                    .set("productId", value.getProductId());
                            out.collect(result.toString());
                        }
                    }
                }
            }

            @Override
            public void processBroadcastElement(Map<String, UserInfo> value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<Void, Map<String, UserInfo>> broadcastState = ctx.getBroadcastState(descriptor);
                //再清空历史状态数据
                broadcastState.clear();
                //最后将最新的广播流数据放到state中（更新状态数据）
                broadcastState.put(null, value);
            }
        });
        //S
        process.print("合并结果:");
        //E
        env.execute("执行数据匹配任务");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Slf4j
    @ToString
    public static class ClickInfo implements Serializable {
        //{"userID": "user_3", "eventTime": "2019-08-17 12:19:47", "eventType": "browse", "productID": 1}
        /**
         * 用户ID.
         */
        private String userId;
        /**
         * 事件时间.
         */
        private String eventTime;
        /**
         * 事件类型.
         */
        private String eventType;
        /**
         * 产品id.
         */
        private int productId;
    }

    private static class ClickDataSource extends RichParallelSourceFunction<ClickInfo> {
        /**
         * 定义运行标记.
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
        public void run(SourceContext ctx) throws Exception {
            while (isRunning) {
                //生成用户编号
                int num = RandomUtil.randomInt(1, 5);
                String userId = "user_" + num;
                //事件时间
                String eventTime = DateUtil.format(new Date(), DatePattern.NORM_DATETIME_PATTERN);
                String[] eventTypes = {"browse", "click", "reading", "no_action"};
                ctx.collect(new ClickInfo(userId, eventTime, eventTypes[num - 1], num));
                Thread.sleep(500);
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
}
