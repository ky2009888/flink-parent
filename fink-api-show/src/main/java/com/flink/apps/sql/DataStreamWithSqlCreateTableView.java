package com.flink.apps.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * create table or view with SQL
 *
 * @author root
 * @date 2021/7/8
 **/
public class DataStreamWithSqlCreateTableView {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //S
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //T
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        DataStreamSource<OrderInfo> dataStreamSource1 = env.fromCollection(Arrays.asList(
                new OrderInfo(1, "apple", "USA"),
                new OrderInfo(2, "HUAWEI", "China"),
                new OrderInfo(3, "OPP", "China")
        ));
        DataStreamSource<OrderInfo> dataStreamSource2 = env.fromCollection(Arrays.asList(
                new OrderInfo(4, "MAC", "USA"),
                new OrderInfo(5, "THINK PAD", "China"),
                new OrderInfo(6, "LENOVO", "China")
        ));
        //创建表
        Table tablePhone = tableEnvironment.fromDataStream(dataStreamSource1, $("id"), $("name"), $("addr"));
        tableEnvironment.createTemporaryView("computer", dataStreamSource2, $("id"), $("name"), $("addr"));
        Table resultTable =
                tableEnvironment.sqlQuery("select * from " + tablePhone + " where addr = 'USA' union select * from computer where addr = 'USA' ");
        //
        DataStream<Tuple2<Boolean, OrderInfo>> dataStream = tableEnvironment.toRetractStream(resultTable, OrderInfo.class);
        //S
        dataStream.print();
        //E
        env.execute();
    }

    /**
     * order information.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class OrderInfo {
        /**
         * id
         */
        private int id;
        /**
         * name.
         */
        private String name;
        /**
         * address of name.
         */
        private String addr;
    }
}
