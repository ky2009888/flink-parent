package com.flink.apps.datasource;

import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * 通过案例展示broadcast的广播流的用法
 *
 * @author ky2009666
 * @date 2021/7/19
 **/
public class BroadcastStateConfigUpdate {
    /**
     * 命令行启动类
     *
     * @param args 命令行参数.
     * @throws Exception 异常.
     */
    public static void main(String[] args) throws Exception {
        //E 创建流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(30000, CheckpointingMode.AT_LEAST_ONCE);
        //S
        JdbcInputFormat jdbcDataFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(ConstantsWithStringLable.JDBC_DRIVER_CLASS_STR)
                .setDBUrl(ConstantsWithStringLable.MYSQL_CONNECTION_STR)
                .setUsername(ConstantsWithStringLable.MYSQL_FLINK_DB_USERNAME)
                .setPassword(ConstantsWithStringLable.MYSQL_FLINK_DB_PASSWORD)
                .setQuery("select userID,userName,userAge from user_info ")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO))
                .finish();
        DataStreamSource<Row> mysqlUserInfo = env.createInput(jdbcDataFormat);
        mysqlUserInfo.print("打印读取MySQL数据:");
        //T
        //S
        //E
        env.execute("读取数据");
    }
}
