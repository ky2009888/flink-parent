package com.flink.apps.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * 用户行为实体类
 *
 * @author ky2009666
 * @date 2021/7/11
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UserBehavior implements Serializable {
    /**
     * 用户ID.
     */
    private Long userId;
    /**
     * 商品ID.
     */
    private Long itemId;
    /**
     * 商品类别.
     */
    private Integer categoryId;
    /**
     * 用户行为.
     */
    private String behavior;
    /**
     * 时间戳.
     */
    private Long timestamp;
}
