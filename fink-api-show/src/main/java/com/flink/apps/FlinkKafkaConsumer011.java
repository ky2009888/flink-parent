package com.flink.apps;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * @author ky2009666
 * @description
 * @date 2021/6/3
 **/
public class FlinkKafkaConsumer011 implements SourceFunction<OrderInfo> {
    /**
     * 定义running标志
     */
    private volatile boolean running = true;

    public FlinkKafkaConsumer011(OrderInfo orderInfo, SimpleStringSchema simpleStringSchema, Properties properties) {
    }

    @Override
    public void run(SourceContext<OrderInfo> sourceContext) throws Exception {
        Random random = new Random();
        HashMap<String, Double> sensorTempMap = new HashMap<String, Double>(16);
        for (int i = 0; i < 10; i++) {
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }
        while (running) {
            for (String sensorId : sensorTempMap.keySet()) {
                Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                sensorTempMap.put(sensorId, newTemp);
                OrderInfo orderInfo = new OrderInfo(sensorId, System.currentTimeMillis(), newTemp);
                sourceContext.collect(orderInfo);
            }
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
