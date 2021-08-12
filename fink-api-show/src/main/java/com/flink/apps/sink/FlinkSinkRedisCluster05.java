package com.flink.apps.sink;

import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * 从redis中读取数据和写入数据
 *
 * @author ky2009666
 * @date 2021/8/10
 **/
public class FlinkSinkRedisCluster05 {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<Long> numData = env.fromSequence(1, 100);
        //T
        //S
        DataStreamSink<Long> longDataToRedis = numData.addSink(new RichSinkFunction<Long>() {
            //定义操作句柄
            private JedisCluster jedisCluster = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                Set<HostAndPort> set = new HashSet<>();
                set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR, 7001));
                set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR, 7002));
                set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR, 7003));
                set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR, 8001));
                set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR, 8002));
                set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR, 8003));
                jedisCluster = new JedisCluster(set);
            }

            @Override
            public void close() throws Exception {
                jedisCluster.close();
            }


            @Override
            public void invoke(Long value, Context context) throws Exception {
                jedisCluster.set("KEY-" + value, String.valueOf(value));
            }
        });

        //E
        env.execute();
    }
}
