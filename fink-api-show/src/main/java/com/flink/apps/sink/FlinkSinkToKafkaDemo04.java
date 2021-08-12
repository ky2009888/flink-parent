package com.flink.apps.sink;

import cn.hutool.core.util.RandomUtil;
import com.flink.apps.source.entity.UserInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 将数据存储到Kafka
 *
 * @author ky2009666
 * @date 2021/8/10
 **/
public class FlinkSinkToKafkaDemo04 {
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
                while (isRunning && count < 100) {
                    count++;
                    userInfo.setUserid("UA_" + count + RandomUtil.randomInt(1, 10000));
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
        userInfoData.addSink(
                JdbcSink.sink(
                        "insert into user_info (userID, userName, userAge) values (?, ?, ?)",
                        (statement, userInfo) -> {
                            statement.setString(1, userInfo.getUserid());
                            statement.setString(2, userInfo.getUsername());
                            statement.setInt(3, userInfo.getUserage());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(10)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:13309/coin-exchange?useSSL=false")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("Qwer@#1234")
                                .build()
                )
        );
        //E
        env.execute("执行写入MySQL的数据");
    }
}
