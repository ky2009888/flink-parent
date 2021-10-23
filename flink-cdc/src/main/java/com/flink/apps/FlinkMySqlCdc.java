package com.flink.apps;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;

/**
 * 全量和增量获取MySQL的数据
 *
 * @author ky2009666
 * @date 2021/10/23
 **/
public class FlinkMySqlCdc {
    public static void main(String[] args) throws Exception {
        //E 建立环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //开启检查点 配置开始
        // 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000);
        // 高级选项：
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkpoints 自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        // 指定检查点存储位置
        env.setStateBackend(new FsStateBackend("hdfs://h7:9820/flinkCDC"));
        // 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "root");
        //开启检查点 配置结束
        //3.创建Flink-MySQL-CDC的Source
        DebeziumSourceFunction<String> build
                = MySqlSource.<String>builder().hostname("192.168.64.1")
                .port(13309).username("h7").password("Qwer@#1234").serverTimeZone("UTC")
                .databaseList("flinkdb").tableList("flinkdb.user_info")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema()).build();
        //S
        DataStreamSource<String> mysqlDataSource = env.addSource(build);
        //T
        //S
        mysqlDataSource.print();
        //E
        env.execute("mysqlDataSourceTask");
    }
}
