package com.flink.apps.source.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * (UserInfo)实体类
 *
 * @author makejava
 * @since 2021-08-09 21:59:29
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class UserInfo implements Serializable {
    private static final long serialVersionUID = -87290162585382724L;
    /**
     * 用户id.
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
     * 定义构造方法.
     *
     * @param username 用户名.
     * @param userage  年龄.
     */
    public UserInfo(String username, Integer userage) {
        this.username = username;
        this.userage = userage;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "userid='" + userid + '\'' +
                ", username='" + username + '\'' +
                ", userage=" + userage +
                '}';
    }
}
