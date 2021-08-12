package com.flink.apps.trans;

import cn.hutool.db.Db;
import com.flink.apps.source.entity.UserInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 对流进行处理
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
public class TransformationDemo01 {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<UserInfo> userInfoData = env.addSource(new RichParallelSourceFunction<UserInfo>() {
            //定义运行标志
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<UserInfo> ctx) throws Exception {
                while (isRunning) {
                    List<UserInfo> userInfoList = Db.use().query("select * from user_info", UserInfo.class);
                    for (UserInfo userInfo : userInfoList) {
                        ctx.collect(userInfo);
                    }
                    Thread.sleep(3000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        //T
        SingleOutputStreamOperator<String> userNameData = userInfoData.filter(new RichFilterFunction<UserInfo>() {
            @Override
            public boolean filter(UserInfo value) throws Exception {
                return value.getUserage() > 10;
            }
        }).map(new RichMapFunction<UserInfo, UserInfo>() {
            @Override
            public UserInfo map(UserInfo value) throws Exception {
                return new UserInfo(value.getUsername(), value.getUserage());
            }
        }).flatMap(new RichFlatMapFunction<UserInfo, String>() {
            @Override
            public void flatMap(UserInfo value, Collector<String> out) throws Exception {
                out.collect(value.getUsername());
            }
        });
        //S
        userNameData.print("打印用户名:");
        //E
        env.execute();
    }
}
