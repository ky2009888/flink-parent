package com.flink.apps;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author ky2009666
 * @description OrderInfo
 * @date 2021/6/3
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo implements Serializable {
    /**
     * 定义sensorStr字符串.
     */
    private String sensorStr;
    private long number;
    private double value;
}
