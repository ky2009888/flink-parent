package com.flink.apps.sink;

import cn.hutool.core.util.RandomUtil;
import com.flink.apps.constant.ConstantsWithStringLable;
import com.flink.apps.source.entity.UserInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 将数据存储到MySQL
 *
 * @author ky2009666
 * @date 2021/8/10
 **/
public class FlinkSinkToMySqlDemo03 {
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
                    userInfo.setUsername("我是大王" + count + "号");
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
        SingleOutputStreamOperator<String> result = userInfoData.flatMap(new RichFlatMapFunction<UserInfo, String>() {
            @Override
            public void flatMap(UserInfo value, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }
        });
        //S
        //根据参数创建KafkaProducer/KafkaSink
        Properties props = new Properties();
        props.setProperty(ConstantsWithStringLable.BOOTSTRAP_SERVERS, ConstantsWithStringLable.H7_9092);
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer("user_info_kafka", new SimpleStringSchema(), props);
        result.addSink(kafkaSink);
        //E
        env.execute("执行写入MySQL的数据");
    }
}
