package com.flink.apps.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * 商品统计实体类
 *
 * @author ky2009666
 * @date 2021/7/11
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ItemViewCount implements Serializable {
    /**
     * 商品ID.
     */
    private Long itemId;
    /**
     * 窗口结束时间.
     */
    private Long windowEnd;
    /**
     * 统计.
     */
    private Long count;
}
