package com.flink.apps;

import lombok.*;

import java.io.Serializable;

/**
 * @author ky2009666
 * @description SensorReading
 * @date 2021/6/3
 **/
@Data
@ToString
@AllArgsConstructor
public class SensorReading implements Serializable {
    /**
     * 定义sensorStr字符串.
     */
    private String sensorStr;
    private long number;
    private double value;
}
