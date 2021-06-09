package com.flink.apps.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ky2009666
 * @description flinktable的案例1
 * @date 2021/6/6
 **/
public class FlinkTableApiDemo01 {
    public static void main(String[] args) throws Exception {
        //E->S->T->S->E
        //0、创建EVN环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //1、source 输入
        DataStreamSource<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)

        ));
        DataStreamSource<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)
        ));
        //2、translate 转换
        Table orderATable = tableEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        tableEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));
        //执行查询
        String unionTableAandTableB = "SELECT * FROM " + orderATable + " WHERE amount > 2 " +
                "UNION ALL " +
                "SELECT * FROM OrderB WHERE amount < 2";
        Table tableResult = tableEnv.sqlQuery(unionTableAandTableB);
        DataStream<Order> orderDataStreamReuslt = tableEnv.toAppendStream(tableResult, Order.class);
        //3、sink 输出
        orderDataStreamReuslt.print();
        //4、执行 execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        /**
         * 用户ID.
         */
        public Long user;
        /**
         * 购买的产品.
         */
        public String product;
        /**
         * 产品数量.
         */
        public int amount;
    }
}
