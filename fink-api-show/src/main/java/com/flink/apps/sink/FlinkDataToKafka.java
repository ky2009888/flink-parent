package com.flink.apps.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

/**
 * @author ky2009666
 * @description 将flink数据写入kafka
 * @date 2021/6/5
 **/
public class FlinkDataToKafka {
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //source 操作，读取数据
        DataStream<String> dataStream = environment.readTextFile(
                "F:\\workspace-flink-2021\\flink-parent\\fink-api-show\\src\\main\\resources\\hello.txt");
        DataStream<String> outputStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
        //将文本中的数据输出到kafka
        outputStream.addSink(new FlinkKafkaProducer("192.168.64.7:9092", "test", new SimpleStringSchema()));
        environment.execute();
    }
}
