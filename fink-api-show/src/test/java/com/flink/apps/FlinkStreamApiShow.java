package com.flink.apps;

import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author ky2009666
 * @description FlinkStreamApiShow
 * @date 2021/6/3
 **/
@SpringBootTest
@SpringBootConfiguration
public class FlinkStreamApiShow {
    /**
     * 验证从集合中读取数据.
     */
    @Test
    void testReadDataFromCollection() throws Exception {
        //获取执行环境
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行执行度
        execEnv.setParallelism(4);
        DataStreamSource<String> streamSource = execEnv.fromCollection(Arrays.asList(new String[]{"1", "2", "3", "4"}));
        //打印数据
        streamSource.print();
        //执行环境
        execEnv.execute();
    }
    /**
     * 验证从文件中读取数据.
     */
    @SneakyThrows
    @Test
    void testReadDataFromFile(){
        //获取执行环境
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        execEnv.setParallelism(1);
        //从文件中读取内容
        DataStreamSource<String> streamSource = execEnv.readTextFile("F:\\workspace-flink-2021\\flink-parent\\fink-api-show\\pom.xml");
        //打印内容
        streamSource.print();
        //执行环境
        execEnv.execute();
    }
    /**
     * 验证从文件中读取数据.
     */
    @SneakyThrows
    @Test
    void testReadDataFromKafka(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "h7:9092");
        properties.setProperty("group.id", "test-consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        //获取执行环境
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        execEnv.setParallelism(4);
        //DataStream<String> dataStream = execEnv.addSource( new FlinkKafkaConsumer011(new OrderInfo(), new SimpleStringSchema(), properties));
        //执行环境
        execEnv.execute();
    }
    /**
     * kafka的消息生产者
     */
    @Test
    void kafkaProducer() {
        // 1. 创建用于连接Kafka的Properties配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.64.7:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 2. 创建一个生产者对象KafkaProducer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        // 3. 调用send发送1-100消息到指定Topic test
        for (int i = 0; i < 100; ++i) {
            try {
                // 获取返回值Future，该对象封装了返回值
                Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("test", null, i + ""));
                // 调用一个Future.get()方法等待响应
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        // 5. 关闭生产者
        producer.close();
    }


    /**
     * kafka的消息消费者
     */
    @Test
    void kafkaConsumer() {

    }
}
