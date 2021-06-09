package com.flink.apps.sink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ky2009666
 * @description 将处理完成的数据写入MySQL数据库
 * @date 2021/6/5
 **/
public class FlinkJdbcDemo {
    /**
     * 入口执行方法.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //构建执行环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // 从配置文件中读取配置信息
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("F:\\workspace-flink-2021\\flink-parent\\fink-api-show\\src\\main\\resources\\application.properties");
        String url = parameterTool.get("url");
        String driver = parameterTool.get("driver");
        String user = parameterTool.get("user");
        String password = parameterTool.get("password");
        // sql语句，用问号做占位符
        String sql = "insert into tb_traffic_statistic_min(starttime, city_name, distinct_user_count, total_traffic) values(?, ?, ?, ?)";
        // 伪造数据
        Tuple4<String, String, Integer, Double> bjTp = Tuple4.of("2020-12-01 00:00:00", "北京", 10, 2.3d);
        env
                .fromElements(bjTp)
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.DOUBLE))
                // 添加JDBCSink
                .addSink(
                        JdbcSink.sink(
                                // sql语句
                                sql,
                                // 设置占位符对应的字段值
                                (ps, tp) -> {
                                    ps.setString(1, tp.f0);
                                    ps.setString(2, tp.f1);
                                    ps.setInt(3, tp.f2);
                                    ps.setDouble(4, tp.f3);
                                },
                                // 传递jdbc的连接属性
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withDriverName(driver)
                                        .withUrl(url)
                                        .withUsername(user)
                                        .withPassword(password)
                                        .build()
                        )
                );
        // 执行
        env.execute();
    }
}
