package com.flink.apps.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author ky2009666
 * @description 从kafka中读取数据，通过flink进行处理，最终将数据存储到kafka
 * @date 2021/6/5
 **/
public class FromKafkaHandleWithFlinkToKafka {
    /**
     * kafka服务器连接地址.
     */
    public static final String KAFKA_SERVER_ADDR_STR = "192.168.64.7:9092";

    /**
     * 默认的执行入口.
     *
     * @param args 命令行参数.
     */
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER_ADDR_STR);
        properties.setProperty("group.id", "test-consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        // 从文件读取数据
        DataStream<String> inputStream = environment.addSource(new FlinkKafkaConsumer<String>("fromTopic", new SimpleStringSchema(), properties));
        // 转换成SensorReading类型
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            //改变连接字符串的分隔符
            String content = " ";
            for (String field : fields) {
                content += ":" + field;
            }
            return content;
        });
        //将处理完成的数据存储到kafka中
        dataStream.addSink(new FlinkKafkaProducer<String>(KAFKA_SERVER_ADDR_STR, "toTopic", new SimpleStringSchema()));
        environment.execute();
    }
}
