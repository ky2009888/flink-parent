package com.flink.apps.datasource.entity;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * user_info
 * @author 
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UserInfo implements Serializable {
    /**
     * 用户ID.
     */
    private String userid;
    /**
     * 用户名.
     */
    private String username;
    /**
     * 用户年龄.
     */
    private Integer userage;
    /**
     * 序列化字符串.
     */
    private static final long serialVersionUID = 1L;
}