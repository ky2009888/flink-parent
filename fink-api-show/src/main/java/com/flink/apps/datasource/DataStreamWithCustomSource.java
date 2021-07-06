package com.flink.apps.datasource;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.flink.apps.constant.ConstantsWithStringLable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;
import java.util.Date;

/**
 * custom data source
 *
 * @author root
 * @date 2021/7/6
 **/
public class DataStreamWithCustomSource {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();
        envStream.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        envStream.setParallelism(1);
        //S
        //envStream.from
        DataStreamSource<OrderInfo> orderInfoSource = envStream.addSource(new MyRandomOrderSource());
        //T
        //S
        orderInfoSource.print();
        //E
        envStream.execute();
    }

    /**
     * 自定义source源.
     */
    public static class MyRandomOrderSource extends RichParallelSourceFunction<OrderInfo> {
        /**
         * 定义数据执行的flag标志.
         */
        boolean flag = true;

        /**
         * 产生数据.
         *
         * @param sourceContext 上下文.
         * @throws Exception 异常.
         */
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while (flag) {
                OrderInfo orderInfo = generateOrderInfo();
                sourceContext.collect(orderInfo);
                Thread.sleep(ConstantsWithStringLable.THREE_THOUSAND);
            }
        }

        /**
         * generate order info
         *
         * @return OrderInfo.
         */
        private OrderInfo generateOrderInfo() {
            String userId = UUID.fastUUID().toString();
            long addNum = RandomUtil.randomLong();
            String address = "南京软件大道" + addNum + "号";
            String productName = "格力空调型号:" + UUID.fastUUID();
            return new OrderInfo(userId, address, productName, new Date());
        }

        /**
         * 执行取消命令的时候执行.
         */
        @Override
        public void cancel() {

        }
    }

    /**
     * 订单信息实体类.
     *
     * @author root
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class OrderInfo implements Serializable {
        /**
         * 订单ID.
         */
        private String id;
        /**
         * 订单地址.
         */
        private String addr;
        /**
         * 产品名称.
         */
        private String productName;
        /**
         * 创建时间.
         */
        private Date createTime;
    }
}
