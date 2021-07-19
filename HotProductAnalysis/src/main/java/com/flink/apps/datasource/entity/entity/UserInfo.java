package com.flink.apps.datasource.entity.entity;

import java.io.Serializable;

/**
 * (UserInfo)实体类
 *
 * @author makejava
 * @since 2021-07-19 17:27:25
 */
public class UserInfo implements Serializable {
    private static final long serialVersionUID = -17175760699173149L;

    private String userid;

    private String username;

    private Integer userage;


    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getUserage() {
        return userage;
    }

    public void setUserage(Integer userage) {
        this.userage = userage;
    }

}
