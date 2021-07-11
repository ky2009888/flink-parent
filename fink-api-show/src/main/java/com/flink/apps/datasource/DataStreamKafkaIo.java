package com.flink.apps.datasource;

import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 通过kafka进行消息的操作
 *
 * @author root
 * @date 2021/7/7
 **/
public class DataStreamKafkaIo {

    /**
     * 命令行执行入口
     *
     * @param args 命令行参数.
     */
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        Properties properties = new Properties();
        properties.setProperty(ConstantsWithStringLable.BOOTSTRAP_SERVERS, ConstantsWithStringLable.H7_9092);
        properties.setProperty(ConstantsWithStringLable.GROUP_ID, ConstantsWithStringLable.TEST_CONSUMER_GROUP);
        //latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        properties.setProperty(ConstantsWithStringLable.AUTO_OFFSET_RESET, ConstantsWithStringLable.LATEST);
        //会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        properties.setProperty(ConstantsWithStringLable.FLINK_PARTITION_DISCOVERY_INTERVAL_MILLIS, ConstantsWithStringLable.FIVE_THOUSAND);
        //自动提交(提交到默认主题,后续学习了Checkpoint后随着Checkpoint存储在Checkpoint和默认主题中)
        properties.setProperty(ConstantsWithStringLable.ENABLE_AUTO_COMMIT, ConstantsWithStringLable.TRUE);
        //自动提交的时间间隔
        properties.setProperty(ConstantsWithStringLable.AUTO_COMMIT_INTERVAL_MS, ConstantsWithStringLable.TWO_THOUSAND);
        DataStream<String> kafkaDataStream = env
                .addSource(new FlinkKafkaConsumer<>(ConstantsWithStringLable.INPUT_KAFKA, new SimpleStringSchema(), properties));
        //T
        SingleOutputStreamOperator<String> etlDS = kafkaDataStream.filter((FilterFunction<String>) value -> value.contains("success"));
        //S
        etlDS.print();
        FlinkKafkaProducer<String> kafkaSink =
                new FlinkKafkaProducer<>(ConstantsWithStringLable.OUTPUT_KAFKA, new SimpleStringSchema(), properties);
        etlDS.addSink(kafkaSink);
        //E
        env.execute();
    }
}
