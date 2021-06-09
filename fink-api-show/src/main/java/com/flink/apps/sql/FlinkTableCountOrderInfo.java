package com.flink.apps.sql;

import com.flink.apps.OrderInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 *using flink to count order max and min
 * @author root
 */
public class FlinkTableCountOrderInfo {
    /**
     *      * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        //prepare enviroment for flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        //source
        DataStreamSource<Order> orderDs = env.addSource(new RichSourceFunction<Order>() {
            //default the thread is running.
            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        //trans
        SingleOutputStreamOperator<Order> operator = orderDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, timestamp) -> event.getCreateTime())
        );
        //register table
        tableEnvironment.createTemporaryView("orderT",operator,
                $("orderId"),$("userId"),$("money"),$("createTime"));
        //ecuting query info
        String sql = " select " +
                " userId," +
                " count(*) as totalCount," +
                " max(money) as maxMoney," +
                " min(money) as minMoney " +
                " from orderT " +
                " group by userId," +
                " tumble(createTime, interval '5' second)";
        Table table = tableEnvironment.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnvironment.toRetractStream(table, Row.class);
        resultDS.print();
        env.execute();
    }

    /**
     * ustom order object.
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }
}
