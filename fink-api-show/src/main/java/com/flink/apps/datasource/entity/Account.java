package com.flink.apps.datasource.entity;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Date;
import java.io.Serializable;

/**
 * 用户财产记录(Account)实体类
 *
 * @author root
 * @since 2021-07-06 17:07:33
 */
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Account implements Serializable {
    private static final long serialVersionUID = -71682479436072158L;
    /**
     * 自增id
     */
    private Long id;
    /**
     * 用户id
     */
    private Long userId;
    /**
     * 币种id
     */
    private Long coinId;
    /**
     * 账号状态：1，正常；2，冻结；
     */
    private Object status;
    /**
     * 币种可用金额
     */
    private Double balanceAmount;
    /**
     * 币种冻结金额
     */
    private Double freezeAmount;
    /**
     * 累计充值金额
     */
    private Double rechargeAmount;
    /**
     * 累计提现金额
     */
    private Double withdrawalsAmount;
    /**
     * 净值
     */
    private Double netValue;
    /**
     * 占用保证金
     */
    private Double lockMargin;
    /**
     * 持仓盈亏/浮动盈亏
     */
    private Double floatProfit;
    /**
     * 总盈亏
     */
    private Double totalProfit;
    /**
     * 充值地址
     */
    private String recAddr;
    /**
     * 版本号
     */
    private Long version;
    /**
     * 更新时间
     */
    private Date lastUpdateTime;
    /**
     * 创建时间
     */
    private Date created;

}
