package com.flink.apps.sink;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import com.flink.apps.source.entity.UserInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;

/**
 * 将数据存储到MySQL
 *
 * @author ky2009666
 * @date 2021/8/10
 **/
public class FlinkSinkToMySqlDemo02 {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<UserInfo> userInfoData = env.addSource(new RichParallelSourceFunction<UserInfo>() {
            //执行标志
            private volatile boolean isRunning = true;
            //执行次数
            private long count = 0L;

            @Override
            public void run(SourceContext<UserInfo> ctx) throws Exception {
                UserInfo userInfo = new UserInfo();
                while (isRunning && count < 1000) {
                    count++;
                    userInfo.setUserid("UA_" + count + RandomUtil.randomInt(1, 30));
                    userInfo.setUsername("大王" + count + "号");
                    userInfo.setUserage(RandomUtil.randomInt(10, 60));
                    ctx.collect(userInfo);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        //T
        //S
        userInfoData.addSink(new RichSinkFunction<UserInfo>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            /**
             * Writes the given value to the sink. This function is called for every record.
             *
             * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
             * {@code default} method for backward compatibility with the old-style method only.
             *
             * @param value   The input record.
             * @param context Additional context about the input record.
             * @throws Exception This method may throw exceptions. Throwing an exception will cause the
             *                   operation to fail and may trigger recovery.
             */
            @Override
            public void invoke(UserInfo value, Context context) throws Exception {
                List<Entity> result = Db.use().find(Entity.create("user_info").set("userID", value.getUserid()));
                if (!(result != null && result.size()>0)){
                    Db.use().insert(
                            Entity.create("user_info")
                                    .set("userID", value.getUserid())
                                    .set("userName", value.getUsername())
                                    .set("userAge", value.getUserage())
                    );
                }
            }
        });
        //E
        env.execute("执行写入MySQL的数据");
    }
}
