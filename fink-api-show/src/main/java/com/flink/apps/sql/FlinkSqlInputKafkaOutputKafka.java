package com.flink.apps.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * 将数据处理完成后的结果存储到kafka.
 * @author root
 */
public class FlinkSqlInputKafkaOutputKafka {
    /**
     * kafka服务器连接地址.
     */
    public static final String KAFKA_SERVER_ADDR_STR = "192.168.64.7:9092";

    public static void main(String[] args) throws Exception{
        //1、建立flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        //2、建立flink sql的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);
        //3、建立kafka的source
        TableResult inputTableResult = tableEnvironment.executeSql("CREATE TABLE input_kafka (\n" +
                "  `user_id` BIGINT,\n" +
                "  `page_id` BIGINT,\n" +
                "  `status` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'input_kafka',\n" +
                "  'properties.bootstrap.servers' = 'h7:9092',\n" +
                "  'properties.group.id' = 'test-consumer-group',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        TableResult outputTableResult = tableEnvironment.executeSql("CREATE TABLE output_kafka (\n" +
                "  `user_id` BIGINT,\n" +
                "  `page_id` BIGINT,\n" +
                "  `status` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'output_kafka',\n" +
                "  'properties.bootstrap.servers' = 'h7:9092',\n" +
                "  'format' = 'json',\n" +
                "  'sink.partitioner' = 'round-robin'\n" +
                ")");
        String sql = "select " +
                "user_id," +
                "page_id," +
                "status " +
                "from input_kafka " +
                "where status = 'success'";
        Table sqlQuery = tableEnvironment.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnvironment.toRetractStream(sqlQuery, Row.class);
        dataStream.print();
        //写入kafka的主题
        tableEnvironment.executeSql("insert into output_kafka select * from " + sqlQuery);
        env.execute();
    }
}
